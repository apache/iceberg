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


class SeekableInputStream(object):

    def available(self):
        raise NotImplementedError()

    def close(self):
        raise NotImplementedError()

    def mark(self, read_limit):
        raise NotImplementedError()

    def mark_supported(self):
        raise NotImplementedError()

    def read(self, b=None, off=None, len=None):
        raise NotImplementedError()

    def reset(self):
        raise NotImplementedError()

    def skip(self, n):
        raise NotImplementedError()

    def get_pos(self):
        raise NotImplementedError()

    def seek(self, new_pos):
        raise NotImplementedError()
