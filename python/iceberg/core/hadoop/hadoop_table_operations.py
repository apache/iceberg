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

import iceberg.core as core


class HadoopTableOperations(core.TableOperations):

    def __init__(self, location, conf):
        self.conf = conf
        self.location = location
        self.should_refresh = True
        self.verison = None
        self.current_metadata = None

    def current(self):
        pass

    def refresh(self):
        raise RuntimeError("Not yet implemented")

    def commit(self, base, metadata):
        raise RuntimeError("Not yet implemented")

    def new_input_file(self, path):
        raise RuntimeError("Not yet implemented")

    def new_metadata_file(self, filename):
        raise RuntimeError("Not yet implemented")

    def delete_file(self, path):
        raise RuntimeError("Not yet implemented")

    def new_snapshot_id(self):
        raise RuntimeError("Not yet implemented")
