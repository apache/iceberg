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

import os
import stat

from .file_status import FileStatus
from .file_system import FileSystem


class LocalFileSystem(FileSystem):
    fs_inst = None

    @staticmethod
    def get_instance():
        if LocalFileSystem.fs_inst is None:
            LocalFileSystem()
        return LocalFileSystem.fs_inst

    def __init__(self):
        if LocalFileSystem.fs_inst is None:
            LocalFileSystem.fs_inst = self

    def open(self, path, mode='rb'):

        return open(LocalFileSystem.fix_path(path), mode=mode)

    def stat(self, path):
        st = os.stat(LocalFileSystem.fix_path(path))
        return FileStatus(path=path, length=st.st_size, is_dir=stat.S_ISDIR(st.st_mode),
                          blocksize=st.st_blksize, modification_time=st.st_mtime, access_time=st.st_atime,
                          permission=st.st_mode, owner=st.st_uid, group=st.st_gid)

    @staticmethod
    def fix_path(path):
        if path.startswith("file://"):
            path = str(path[7:])
        return path

    # def ls(self, path):
    #     return os.listdir(path)
    #
    # def delete(self, path, recursive=False):
    #     return os.remove(path)
    #

    #
    # def rename(self, path, new_path):
    #     return os.rename(path, new_path)
    #
    # def mkdir(self, path, create_parents=True):
    #     if create_parents:
    #         return os.makedirs(path)
    #
    #     return os.mkdir(path)
    #
    # def exists(self, path):
    #     return os.path.isfile(path)
    #
    # def isdir(self, path):
    #     return os.path.isdir(path)
    #
    # def isfile(self, path):
    #     return os.path.isfile(path)
    #
    # def _isfilestore(self):
    #     return True
    #
    # def open(self, path, mode='rb'):
    #     return open(path, mode=mode)
