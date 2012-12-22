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
from libcloud.storage.drivers.cloudfiles import CloudFilesStorageDriver

API_VERSION = 'v1.0'
AUTH_API_VERSION_KTUCLOUD = '1.0'
AUTH_URL_KTUCLOUD = 'https://api.ucloudbiz.olleh.com/storage/v1/auth'


class KTUCloudAuthResponse(Response):
    valid_response_codes = [httplib.NOT_FOUND, httplib.CONFLICT]

    def success(self):
        i = int(self.status)
        return i >= 200 and i <= 299 or i in self.valid_response_codes

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


class KTUCloudFilesResponse(KTUCloudAuthResponse):
    def parse_body(self):
        if not self.body:
            return None

        if 'content-type' in self.headers:
            key = 'content-type'
        elif 'Content-Type' in self.headers:
            key = 'Content-Type'
        else:
            raise LibcloudError('Missing content-type header')

        content_type = self.headers[key]
        if content_type.find(';') != -1:
            content_type = content_type.split(';')[0]

        if content_type == 'application/json':
            try:
                data = json.loads(self.body)
            except:
                raise MalformedResponseError('Failed to parse JSON',
                                             body=self.body,
                                             driver=CloudFilesStorageDriver)
        elif content_type == 'text/plain':
            data = self.body
        else:
            data = self.body

        return data


class KTUCloudFilesRawResponse(KTUCloudFilesResponse, RawResponse):
    pass


class KTUCloudAuthConnection(OpenStackAuthConnection):
    responseCls = KTUCloudAuthResponse

    def authenticate_1_0(self):
        resp = self.request("/storage/v1/auth",
                            headers={
                                'X-Auth-User': self.user_id,
                                'X-Auth-Key': self.key,
                            },
                            method='GET')

        if resp.status == httplib.UNAUTHORIZED:
            # HTTP UNAUTHORIZED (401): auth failed
            raise InvalidCredsError()
        else:
            headers = resp.headers
            # emulate the auth 1.1 URL list
            self.urls = {}
            self.urls['cloudServers'] = \
                [{'publicURL': headers.get('x-server-management-url', None)}]
            self.urls['cloudFilesCDN'] = \
                [{'publicURL': headers.get('x-cdn-management-url', None)}]
            self.urls['cloudFiles'] = \
                [{'publicURL': headers.get('x-storage-url', None)}]
            self.auth_token = headers.get('x-auth-token', None)
            self.auth_user_info = None

            if not self.auth_token:
                raise MalformedResponseError('Missing X-Auth-Token in \
                                              response headers')


class KTUCloudFilesConnection(OpenStackBaseConnection):
    """
    Base connection class for the Cloudfiles driver.
    """

    responseCls = KTUCloudFilesResponse
    rawResponseCls = KTUCloudFilesRawResponse

    def __init__(self, user_id, key, secure=True, auth_url=AUTH_URL_KTUCLOUD,
                 **kwargs):
        super(KTUCloudFilesConnection, self).__init__(user_id, key,
                                                      secure=secure, **kwargs)
        self.auth_url = auth_url
        self.api_version = API_VERSION
        self._auth_version = AUTH_API_VERSION_KTUCLOUD
        self.accept_format = 'application/json'
        self.cdn_request = False

    def get_endpoint(self):
        eps = self.service_catalog.get_endpoints(name='cloudFiles')
        cdn_eps = self.service_catalog.get_endpoints(name='cloudFilesCDN')

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

        return super(KTUCloudFilesConnection, self).request(
            action=action,
            params=params, data=data,
            method=method, headers=headers,
            raw=raw)

    def _populate_hosts_and_request_paths(self):
        """
        KTUCloudStorage uses a separate host for
        API calls which is only provided
        after an initial authentication request.
        """

        if not self.auth_token:
            aurl = self.auth_url

            if self._ex_force_auth_url is not None:
                aurl = self._ex_force_auth_url

            if aurl is None:
                raise LibcloudError('KTUCloudStorageinstance must ' +
                                    'have auth_url set')

            osa = KTUCloudAuthConnection(self, aurl, self._auth_version,
                                         self.user_id, self.key,
                                         tenant_name=self._ex_tenant_name,
                                         timeout=self.timeout)

            # may throw InvalidCreds, etc
            osa.authenticate()

            self.auth_token = osa.auth_token
            self.auth_token_expires = None
            self.auth_user_info = osa.auth_user_info

            # pull out and parse the service catalog
            self.service_catalog = OpenStackServiceCatalog(osa.urls,
                                   ex_force_auth_version=self._auth_version)

        # Set up connection info
        url = self._ex_force_base_url or self.get_endpoint()
        (self.host, self.port, self.secure, self.request_path) = \
                self._tuple_from_url(url)


class KTUCloudStorageDriver(CloudFilesStorageDriver):
    """
    KTCloudFiles driver.
    """
    name = 'KTUCloudStorage'
    website = 'http://cs.ucloud.com/'

    connectionCls = KTUCloudFilesConnection

    def _ex_connection_class_kwargs(self):
        kwargs = {'ex_force_service_region': self.datacenter}

        kwargs['auth_url'] = AUTH_URL_KTUCLOUD

        kwargs.update(self.openstack_connection_kwargs())
        return kwargs


class FileChunkReader(object):
    def __init__(self, file_path, chunk_size):
        self.file_path = file_path
        self.total = os.path.getsize(file_path)
        self.chunk_size = chunk_size
        self.bytes_read = 0
        self.stop_iteration = False

    def __iter__(self):
        return self

    def next(self):
        if self.stop_iteration:
            raise StopIteration

        start_block = self.bytes_read
        end_block = start_block + self.chunk_size
        if end_block >= self.total:
            end_block = self.total
            self.stop_iteration = True
        self.bytes_read += end_block - start_block
        return ChunkStreamReader(file_path=self.file_path,
                                 start_block=start_block,
                                 end_block=end_block,
                                 chunk_size=8192)

    def __next__(self):
        return self.next()


class ChunkStreamReader(object):
    def __init__(self, file_path, start_block, end_block, chunk_size):
        self.fd = open(file_path, 'rb')
        self.fd.seek(start_block)
        self.start_block = start_block
        self.end_block = end_block
        self.chunk_size = chunk_size
        self.bytes_read = 0
        self.stop_iteration = False

    def __iter__(self):
        return self

    def next(self):
        if self.stop_iteration:
            self.fd.close()
            raise StopIteration

        block_size = self.chunk_size
        if self.bytes_read + block_size > \
                self.end_block - self.start_block:
            block_size = self.end_block - self.start_block - self.bytes_read
            self.stop_iteration = True

        block = self.fd.read(block_size)
        self.bytes_read += block_size
        return block

    def __next__(self):
        return self.next()
