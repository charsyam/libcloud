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
from libcloud.common.openstack import OpenStackAuthConnection
from libcloud.common.openstack import OpenStackServiceCatalog
from libcloud.common.openstack import OpenStackAuthResponse
from libcloud.storage.drivers.openstack import (
    OpenStackFileResponse,
    OpenStackFileRawResponse, OpenStackStorageDriver)

API_VERSION = 'v1.0'
AUTH_API_VERSION_KTUCLOUD = '1.0'
AUTH_URL_KTUCLOUD = 'https://api.ucloudbiz.olleh.com/storage/v1/auth'


class KTUCloudStorageAuthResponse(OpenStackAuthResponse):
    def parse_body(self):
        if not self.body:
            return None

        content_type = 'application/json'
        try:
            data = json.loads(self.body)
        except:
            raise MalformedResponseError('Failed to parse JSON',
                                         body=self.body,
                                         driver=KTUCloudStorageDriver)
        return data


class KTUCloudStorageResponse(OpenStackFileResponse):
    pass


class KTUCloudStorageRawResponse(KTUCloudStorageResponse, RawResponse):
    pass


class KTUCloudStorageAuthConnection(OpenStackAuthConnection):
    responseCls = KTUCloudStorageAuthResponse
    action = "/storage/v1/auth"


class KTUCloudStorageConnection(OpenStackBaseConnection):
    """
    Base connection class for the Cloudfiles driver.
    """

    responseCls = KTUCloudStorageResponse
    rawResponseCls = KTUCloudStorageRawResponse
    connCls = KTUCloudStorageAuthConnection

    def __init__(self, user_id, key, secure=True, auth_url=AUTH_URL_KTUCLOUD,
                 **kwargs):
        super(KTUCloudStorageConnection, self).__init__(user_id, key,
                                                      secure=secure, **kwargs)
        self.auth_url = auth_url
        self.api_version = API_VERSION
        self._auth_version = AUTH_API_VERSION_KTUCLOUD
        self.accept_format = 'application/json'
        self.cdn_request = False

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

        return super(KTUCloudStorageConnection, self).request(
            action=action,
            params=params, data=data,
            method=method, headers=headers,
            raw=raw)

    def get_endpoint(self, *args, **kwargs):
        eps = self.service_catalog.get_endpoints(name='cloudFiles')
        endpoint = eps[0]

        if 'publicURL' in endpoint:
            return endpoint['publicURL']
        else:
            raise LibcloudError('Could not find specified endpoint')


class KTUCloudStorageDriver(OpenStackStorageDriver):
    """
    KTCloudStorage driver.
    """
    name = 'KTUCloudStorage'
    website = 'http://cs.ucloud.com/'

    connectionCls = KTUCloudStorageConnection

    def _ex_connection_class_kwargs(self):
        kwargs = {'ex_force_service_region': self.datacenter}

        kwargs['auth_url'] = AUTH_URL_KTUCLOUD

        kwargs.update(self.openstack_connection_kwargs())
        return kwargs

