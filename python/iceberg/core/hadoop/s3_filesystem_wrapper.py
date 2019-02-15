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

import s3fs

from .file_status import FileStatus
from .file_system import FileSystem


class S3FileSystemWrapper(FileSystem):

    def __init__(self, anon=False, key=None, secret=None, token=None):
        self._fs = s3fs.S3FileSystem(anon=anon, key=key, secret=secret, token=token)

    def open(self, path, mode='rb'):
        return self._fs.open(S3FileSystemWrapper.fix_s3_path(path), mode=mode)

    def stat(self, path):
        is_dir = False
        st = dict()
        try:
            st = self._fs.info(S3FileSystemWrapper.fix_s3_path(path), refresh=True)
        except RuntimeError:
            # must be a directory or subsequent du will fail
            du = self._fs.du(S3FileSystemWrapper.fix_s3_path(path), refresh=True)
            if len(du) > 0:
                is_dir = True
            length = 0
            for key, val in du.items():
                length += val

        return FileStatus(path=path, length=st.get("Size", length), is_dir=is_dir,
                          blocksize=None, modification_time=st.get("LastModified"), access_time=None,
                          permission=None, owner=None, group=None)

    @staticmethod
    def fix_s3_path(path):
        if path.startswith("s3n"):
            path = "s3" + str(path[3:])
        return path
