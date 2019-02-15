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

import struct
import uuid


class TableOperations(object):

    def current(self):
        raise NotImplementedError()

    def refresh(self):
        raise NotImplementedError()

    def commit(self, base, metadata):
        raise NotImplementedError()

    def io(self):
        raise NotImplementedError()

    def metadata_file_location(self, file):
        raise NotImplementedError()

    def new_snapshot_id(self):
        new_uuid = uuid.uuid4()
        msb, lsb = struct.unpack(">qq", new_uuid.bytes)
        return abs(msb ^ lsb)
