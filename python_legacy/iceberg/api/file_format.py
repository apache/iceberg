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

from enum import Enum, unique


@unique
class FileFormat(Enum):
    ORC = {"extension": "orc", "splittable": True}
    PARQUET = {"extension": "parquet", "splittable": True}
    AVRO = {"extension": "avro", "splittable": True}

    def add_extension(self, filename):
        if filename.endswith(self.value["extension"]):
            return filename
        else:
            return filename + "." + self.value["extension"]

    def is_splittable(self):
        return self.value["splittable"]

    @staticmethod
    def from_file_name(filename):
        last_index_of = filename.rfind('.')
        if last_index_of < 0:
            return None
        ext = filename[last_index_of + 1:]
        for fmt in FileFormat:
            if ext == fmt.value["extension"]:
                return fmt
