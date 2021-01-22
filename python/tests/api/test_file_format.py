# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from iceberg.api import FileFormat


def test_parquet():
    file_fmt = FileFormat.PARQUET
    file_name = "test_file.parquet"
    add_extension_file = "test_file"
    assert file_fmt.is_splittable()
    assert FileFormat.from_file_name(file_name) == FileFormat.PARQUET
    assert file_name == FileFormat.PARQUET.add_extension(add_extension_file)


def test_avro():
    file_fmt = FileFormat.AVRO
    file_name = "test_file.avro"
    add_extension_file = "test_file"
    assert file_fmt.is_splittable()
    assert FileFormat.from_file_name(file_name) == FileFormat.AVRO
    assert file_name == FileFormat.AVRO.add_extension(add_extension_file)


def test_orc():
    file_fmt = FileFormat.ORC
    file_name = "test_file.orc"
    add_extension_file = "test_file"
    assert file_fmt.is_splittable()
    assert FileFormat.from_file_name(file_name) == FileFormat.ORC
    assert file_name == FileFormat.ORC.add_extension(add_extension_file)
