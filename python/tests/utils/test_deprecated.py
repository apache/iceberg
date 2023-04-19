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
from unittest.mock import Mock, patch

from pyiceberg.utils.deprecated import deprecated


@patch("warnings.warn")
def test_deprecated(warn: Mock) -> None:
    @deprecated(
        deprecated_in="0.1.0",
        removed_in="0.2.0",
        help_message="Please use load_something_else() instead",
    )
    def deprecated_method() -> None:
        pass

    deprecated_method()

    assert warn.called
    assert warn.call_args[0] == (
        "Call to deprecated_method, deprecated in 0.1.0, will be removed in 0.2.0. Please use load_something_else() instead.",
    )
