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
from pyiceberg.utils.truncate import truncate_upper_bound_binary_string, truncate_upper_bound_text_string


def test_upper_bound_string_truncation() -> None:
    assert truncate_upper_bound_text_string("aaaa", 2) == "ab"
    assert truncate_upper_bound_text_string("".join([chr(0x10FFFF), chr(0x10FFFF), chr(0x0)]), 2) is None


def test_upper_bound_binary_truncation() -> None:
    assert truncate_upper_bound_binary_string(b"\x01\x02\x03", 2) == b"\x01\x03"
    assert truncate_upper_bound_binary_string(b"\xff\xff\x00", 2) is None
