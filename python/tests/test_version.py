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
import re

from pyiceberg import __version__

VERSION_REGEX = re.compile(r"^\d+.\d+.\d+(.(dev|rc)\d+)?$")


def test_version_format():
    # should be in the format of 0.14.0 or 0.14.0.dev0
    assert VERSION_REGEX.search(__version__)

    # RCs should work as well
    assert VERSION_REGEX.search("0.1.0.rc0")
