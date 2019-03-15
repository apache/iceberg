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


class FileStatus():

    def __init__(self, path=None, length=None, is_dir=None, block_replication=None,
                 blocksize=None, modification_time=None, access_time=None, permission=None,
                 owner=None, group=None, symlink=None):
        self.path = path
        self.length = length
        self.is_dir = is_dir
        self.block_replication = block_replication
        self.block_size = blocksize
        self.modification_time = modification_time
        self.access_time = access_time
        self.permission = permission
        self.owner = owner
        self.group = group
        self.symlink = symlink
