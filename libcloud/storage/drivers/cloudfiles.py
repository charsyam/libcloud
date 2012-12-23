# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from hashlib import sha1
import hmac
import os
from time import time

from libcloud.utils.py3 import httplib
from libcloud.utils.py3 import urlencode

try:
    import simplejson as json
except ImportError:
    import json

from libcloud.utils.py3 import PY3
from libcloud.utils.py3 import b
from libcloud.utils.py3 import urlquote

if PY3:
    from io import FileIO as file

from libcloud.utils.files import read_in_chunks
from libcloud.common.types import MalformedResponseError, LibcloudError
from libcloud.common.base import Response, RawResponse

from libcloud.storage.providers import Provider
from libcloud.storage.base import Object, Container, StorageDriver
from libcloud.storage.types import ContainerAlreadyExistsError
from libcloud.storage.types import ContainerDoesNotExistError
from libcloud.storage.types import ContainerIsNotEmptyError
from libcloud.storage.types import ObjectDoesNotExistError
from libcloud.storage.types import ObjectHashMismatchError
from libcloud.storage.types import InvalidContainerNameError
from libcloud.common.openstack import OpenStackBaseConnection
from libcloud.common.openstack import OpenStackDriverMixin
from libcloud.storage.drivers.openstack import (
    OpenStackFileResponse, OpenStackFileRawResponse, OpenStackStorageDriver)

from libcloud.common.rackspace import (
    AUTH_URL_US, AUTH_URL_UK)

CDN_HOST = 'cdn.clouddrive.com'
API_VERSION = 'v1.0'

class CloudFilesResponse(OpenStackFileResponse):
    pass


class CloudFilesRawResponse(CloudFilesResponse, RawResponse):
    pass


class CloudFilesConnection(OpenStackBaseConnection):
    """
    Base connection class for the Cloudfiles driver.
    """

    responseCls = CloudFilesResponse
    rawResponseCls = CloudFilesRawResponse

    def __init__(self, user_id, key, secure=True, auth_url=AUTH_URL_US,
                 **kwargs):
        super(CloudFilesConnection, self).__init__(user_id, key, secure=secure,
                                                   **kwargs)
        self.auth_url = auth_url
        self.api_version = API_VERSION
        self.accept_format = 'application/json'
        self.cdn_request = False

    def get_endpoint(self):
        # First, we parse out both files and cdn endpoints
        # for each auth version
        if '2.0' in self._auth_version:
            eps = self.service_catalog.get_endpoints(
                service_type='object-store',
                name='cloudFiles')
            cdn_eps = self.service_catalog.get_endpoints(
                service_type='object-store',
                name='cloudFilesCDN')
        elif ('1.1' in self._auth_version) or ('1.0' in self._auth_version):
            eps = self.service_catalog.get_endpoints(name='cloudFiles')
            cdn_eps = self.service_catalog.get_endpoints(name='cloudFilesCDN')

        # if this is a CDN request, return the cdn url instead
        if self.cdn_request:
            eps = cdn_eps

        if self._ex_force_service_region:
            eps = [ep for ep in eps if ep['region'].lower() == self._ex_force_service_region.lower()]

        if len(eps) == 0:
            # TODO: Better error message
            raise LibcloudError('Could not find specified endpoint')

        ep = eps[0]

        if 'publicURL' in ep:
            return ep['publicURL']
        else:
            raise LibcloudError('Could not find specified endpoint')

    def request(self, action, params=None, data='', headers=None, method='GET',
                raw=False, cdn_request=False):
        if not headers:
            headers = {}
        if not params:
            params = {}

        self.cdn_request = cdn_request
        params['format'] = 'json'

        if method in ['POST', 'PUT'] and 'Content-Type' not in headers:
            headers.update({'Content-Type': 'application/json; charset=UTF-8'})

        return super(CloudFilesConnection, self).request(
            action=action,
            params=params, data=data,
            method=method, headers=headers,
            raw=raw)


class CloudFilesSwiftConnection(CloudFilesConnection):
    """
    Connection class for the Cloudfiles Swift endpoint.
    """

    def __init__(self, *args, **kwargs):
        self.region_name = kwargs.pop('ex_region_name', None)
        super(CloudFilesSwiftConnection, self).__init__(*args, **kwargs)

    def get_endpoint(self, *args, **kwargs):
        if '2.0' in self._auth_version:
            endpoint = self.service_catalog.get_endpoint(
                service_type='object-store',
                name='swift',
                region=self.region_name)
        elif ('1.1' in self._auth_version) or ('1.0' in self._auth_version):
            endpoint = self.service_catalog.get_endpoint(
                name='swift', region=self.region_name)

        if 'publicURL' in endpoint:
            return endpoint['publicURL']
        else:
            raise LibcloudError('Could not find specified endpoint')


class CloudFilesStorageDriver(OpenStackStorageDriver):
    """
    CloudFiles driver.
    """
    name = 'CloudFiles'
    website = 'http://www.rackspace.com/'

    connectionCls = CloudFilesConnection

    def _ex_connection_class_kwargs(self):
        kwargs = {'ex_force_service_region': self.datacenter}

        if self.datacenter in ['dfw', 'ord']:
            kwargs['auth_url'] = AUTH_URL_US
        elif self.datacenter == 'lon':
            kwargs['auth_url'] = AUTH_URL_UK

        kwargs.update(self.openstack_connection_kwargs())
        return kwargs


class CloudFilesUSStorageDriver(CloudFilesStorageDriver):
    """
    Cloudfiles storage driver for the US endpoint.
    """

    type = Provider.CLOUDFILES_US
    name = 'CloudFiles (US)'
    _datacenter = 'ord'


class CloudFilesSwiftStorageDriver(CloudFilesStorageDriver):
    """
    Cloudfiles storage driver for the OpenStack Swift.
    """
    type = Provider.CLOUDFILES_SWIFT
    name = 'CloudFiles (SWIFT)'
    connectionCls = CloudFilesSwiftConnection

    def __init__(self, *args, **kwargs):
        self._ex_region_name = kwargs.get('ex_region_name', 'RegionOne')
        super(CloudFilesSwiftStorageDriver, self).__init__(*args, **kwargs)

    def openstack_connection_kwargs(self):
        rv = super(CloudFilesSwiftStorageDriver,
                   self).openstack_connection_kwargs()
        rv['ex_region_name'] = self._ex_region_name
        return rv


class CloudFilesUKStorageDriver(CloudFilesStorageDriver):
    """
    Cloudfiles storage driver for the UK endpoint.
    """

    type = Provider.CLOUDFILES_UK
    name = 'CloudFiles (UK)'
    _datacenter = 'lon'
