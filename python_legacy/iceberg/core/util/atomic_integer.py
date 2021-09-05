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

from threading import RLock


class AtomicInteger(object):

    def __init__(self, value):
        if not isinstance(value, int):
            raise RuntimeError("Value %s is not an int" % value)
        self.value = value
        self._lock = RLock()

    def increment_and_get(self):
        with self._lock:
            self.value += 1
            return self.value

    def get(self):
        with self._lock:
            return self.value
