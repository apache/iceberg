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

import errno
import os
from pathlib import Path
import stat
from urllib.parse import urlparse

from .file_status import FileStatus
from .file_system import FileSystem


class LocalFileSystem(FileSystem):
    fs_inst = None

    @staticmethod
    def get_instance() -> "LocalFileSystem":
        if not LocalFileSystem.fs_inst:
            return LocalFileSystem()
        return LocalFileSystem.fs_inst

    def __init__(self: "LocalFileSystem") -> None:
        if LocalFileSystem.fs_inst is None:
            LocalFileSystem.fs_inst = self

    def open(self: "LocalFileSystem", path: str, mode: str = 'rb') -> object:
        open_path = Path(LocalFileSystem.fix_path(path))

        if "w" in mode and not open_path.parents[0].exists():
            try:
                open_path.parents[0].mkdir(parents=True)
            except OSError as exc:
                if exc.errno != errno.EEXIST:
                    raise

        return open(open_path, mode=mode)

    def delete(self: "LocalFileSystem", path: str) -> None:
        os.remove(path)

    def stat(self: "LocalFileSystem", path: str) -> FileStatus:
        st = os.stat(LocalFileSystem.fix_path(path))
        return FileStatus(path=path, length=st.st_size, is_dir=stat.S_ISDIR(st.st_mode),
                          blocksize=st.st_blksize, modification_time=st.st_mtime, access_time=st.st_atime,
                          permission=st.st_mode, owner=st.st_uid, group=st.st_gid)

    @staticmethod
    def fix_path(path: str) -> str:
        return urlparse(path).path

    def create(self: "LocalFileSystem", path: str, overwrite: bool = False) -> object:
        if os.path.exists(path) and not overwrite:
            raise RuntimeError("Path %s already exists" % path)

        return open(path, "w")

    def rename(self: "LocalFileSystem", src: str, dest: str) -> bool:
        try:
            os.rename(src, dest)
        except OSError:
            return False

        return True

    def exists(self: "LocalFileSystem", path: str) -> bool:
        return os.path.exists(path)
