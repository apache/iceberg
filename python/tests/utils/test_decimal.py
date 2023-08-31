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
import pytest

from pyiceberg.utils.decimal import decimal_required_bytes


def test_decimal_required_bytes() -> None:
    assert decimal_required_bytes(precision=1) == 1
    assert decimal_required_bytes(precision=2) == 1
    assert decimal_required_bytes(precision=3) == 2
    assert decimal_required_bytes(precision=4) == 2
    assert decimal_required_bytes(precision=5) == 3
    assert decimal_required_bytes(precision=7) == 4
    assert decimal_required_bytes(precision=8) == 4
    assert decimal_required_bytes(precision=10) == 5
    assert decimal_required_bytes(precision=32) == 14
    assert decimal_required_bytes(precision=38) == 16

    with pytest.raises(ValueError) as exc_info:
        decimal_required_bytes(precision=40)
    assert "(0, 40]" in str(exc_info.value)

    with pytest.raises(ValueError) as exc_info:
        decimal_required_bytes(precision=-1)
    assert "(0, 40]" in str(exc_info.value)
