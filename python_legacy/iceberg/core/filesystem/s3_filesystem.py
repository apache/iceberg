# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import io
import logging
import re
import time
from urllib.parse import urlparse

import boto3
from botocore.credentials import RefreshableCredentials
from botocore.exceptions import ClientError
from botocore.session import get_session
from retrying import retry

from .file_status import FileStatus
from .file_system import FileSystem

_logger = logging.getLogger(__name__)


S3_CLIENT = dict()
BOTO_STS_CLIENT = None
CONF = None
ROLE_ARN = "default"
AUTOREFRESH_SESSION = None


@retry(wait_incrementing_start=100, wait_exponential_multiplier=4,
       wait_exponential_max=5000, stop_max_delay=600000, stop_max_attempt_number=7)
def get_s3(obj="resource"):
    global AUTOREFRESH_SESSION
    global ROLE_ARN
    if ROLE_ARN not in S3_CLIENT:
        S3_CLIENT[ROLE_ARN] = dict()

    if ROLE_ARN == "default":
        if AUTOREFRESH_SESSION is None:
            AUTOREFRESH_SESSION = boto3.Session()
        S3_CLIENT["default"]["resource"] = AUTOREFRESH_SESSION.resource('s3')
        S3_CLIENT["default"]["client"] = AUTOREFRESH_SESSION.client('s3')
    else:
        if AUTOREFRESH_SESSION is None:
            sess = get_session()
            sess._credentials = RefreshableCredentials.create_from_metadata(metadata=refresh_sts_session_keys(),
                                                                            refresh_using=refresh_sts_session_keys,
                                                                            method="sts-assume-role")
            AUTOREFRESH_SESSION = boto3.Session(botocore_session=sess)

        S3_CLIENT[ROLE_ARN]["resource"] = AUTOREFRESH_SESSION.resource("s3")
        S3_CLIENT[ROLE_ARN]["client"] = AUTOREFRESH_SESSION.client("s3")

    return S3_CLIENT.get(ROLE_ARN, dict()).get(obj)


def refresh_sts_session_keys():
    params = {"RoleArn": ROLE_ARN,
              "RoleSessionName": "iceberg_python_client_{}".format(int(time.time() * 1000.00))}

    global BOTO_STS_CLIENT
    if not BOTO_STS_CLIENT:
        BOTO_STS_CLIENT = boto3.client('sts')
    sts_creds = BOTO_STS_CLIENT.assume_role(**params).get("Credentials")
    credentials = {"access_key": sts_creds.get("AccessKeyId"),
                   "secret_key": sts_creds.get("SecretAccessKey"),
                   "token": sts_creds.get("SessionToken"),
                   "expiry_time": sts_creds.get("Expiration").isoformat()}
    return credentials


def url_to_bucket_key_name_tuple(url):
    parsed_url = urlparse(url)
    return parsed_url.netloc, parsed_url.path[1:], parsed_url.path.split("/")[-1]


class S3FileSystem(FileSystem):
    fs_inst = None

    @staticmethod
    def get_instance():
        if S3FileSystem.fs_inst is None:
            S3FileSystem()
        return S3FileSystem.fs_inst

    def __init__(self):
        if S3FileSystem.fs_inst is None:
            S3FileSystem.fs_inst = self

    def set_conf(self, conf):
        global CONF

        CONF = conf
        self.set_role(CONF.get("hive.aws_iam_role", "default"))

    def set_role(self, role):
        global ROLE_ARN

        if role is not None:
            ROLE_ARN = role

    def exists(self, path):
        try:
            self.info(path)
        except ClientError as ce:
            if ce.response['Error']['Code'] == "404":
                return False
            else:
                raise

        return True

    def open(self, path, mode='rb'):
        return S3File(path, mode=mode)

    def delete(self, path):
        bucket, key, _ = url_to_bucket_key_name_tuple(S3FileSystem.normalize_s3_path(path))
        get_s3().Object(bucket_name=bucket,
                        key=key).delete()

    def stat(self, path):
        st = self.info(S3FileSystem.normalize_s3_path(path))

        return FileStatus(path=path, length=st.get("ContentLength"), is_dir=False,
                          blocksize=None, modification_time=st.get("LastModified"), access_time=None,
                          permission=None, owner=None, group=None)

    @staticmethod
    def info(url):
        bucket, key, _ = url_to_bucket_key_name_tuple(url)
        return get_s3("client").head_object(Bucket=bucket,
                                            Key=key)

    @staticmethod
    def normalize_s3_path(path):
        return re.sub(r'^s3n://|s3a://', 's3://', path)


class S3File(object):
    MAX_CHUNK_SIZE = 4 * 1048576
    MIN_CHUNK_SIZE = 2 * 65536

    def __init__(self, path, mode="rb"):
        self.path = path
        bucket, key, name = url_to_bucket_key_name_tuple(S3FileSystem.normalize_s3_path(path))
        self.curr_pos = 0
        self.obj = (get_s3()
                    .Object(bucket_name=bucket,
                            key=key))
        self.name = name
        if mode.startswith("r"):
            self.size = self.obj.content_length

        self.isatty = False
        self.closed = False

        self.mode = mode

        self.buffer_remote_reads = True

        self.curr_buffer = None
        self.curr_buffer_start = -1
        self.curr_buffer_end = -1
        self.buffer_reads = 0
        self.buffer_hits = 0

        self.chunk_size = self.MAX_CHUNK_SIZE

    def close(self):
        self.closed = True

    def flush(self):
        pass

    def __next__(self):
        return next(self.readline())

    def read(self, n=0):
        if self.curr_pos >= self.size:
            return None
        if self.buffer_remote_reads:
            stream = self._read_from_buffer(n)
        else:
            if n <= 0:
                n = self.size
                stream = self.obj.get(Range='bytes={}-{}'.format(self.curr_pos,
                                                                 self.size - self.curr_pos))['Body'].read()
            else:
                stream = self.obj.get(Range='bytes={}-{}'.format(self.curr_pos, self.curr_pos + n - 1))['Body'].read()

        self.curr_pos = min(self.curr_pos + n, self.size)
        return stream

    def _read_from_buffer(self, n):
        self.buffer_reads += 1
        # if the buffer is none or if the entire read is not contained
        # in our current buffer fill the buffer
        if self.curr_buffer is None or not (self.curr_buffer_start <= self.curr_pos
                                            and self.curr_pos + n < self.curr_buffer_end):
            self.curr_buffer_start = self.curr_pos
            self.curr_buffer_end = self.curr_pos + max(self.chunk_size, n)
            byte_range = 'bytes={}-{}'.format(self.curr_buffer_start,
                                              self.curr_buffer_end)
            self.curr_buffer = io.BytesIO(self.obj.get(Range=byte_range)['Body'].read())
        else:
            self.buffer_hits += 1

        # seek to the right position if we aren't at the start of the buffer
        if self.curr_buffer_start != self.curr_pos:
            self.curr_buffer.seek(self.curr_pos - self.curr_buffer_start)

        return self.curr_buffer.read(n)

    def readline(self, n=0):
        if self.curr_buffer is None:
            self.curr_buffer = io.BytesIO(self.obj.get()['Body'].read())
        for line in self.curr_buffer:
            yield line

    def seek(self, offset, whence=0):
        if whence == 0:
            self.curr_pos = offset
        elif whence == 1:
            self.curr_pos += offset
        elif whence == 2:
            self.curr_pos = self.size + offset

    def tell(self):
        return self.curr_pos

    def write(self, string):
        resp = self.obj.put(Body=string)
        if not resp.get("ResponseMetadata", dict()).get("HTTPStatusCode") == 200:
            raise RuntimeError("Unable to write to {}".format(self.path))

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def __iter__(self):
        return self
