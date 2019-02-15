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

from iceberg.api.expressions.expressions import Expressions
from pytest import raises

pred = Expressions.less_than("x", 7)


def test_simplify_or():
    assert Expressions.always_true() == Expressions.or_(Expressions.always_true(), pred)
    assert Expressions.always_true() == Expressions.or_(pred, Expressions.always_true())
    assert pred == Expressions.or_(Expressions.always_false(), pred)
    assert pred == Expressions.or_(pred, Expressions.always_false())


def test_simplify_and():
    assert pred == Expressions.and_(Expressions.always_true(), pred)
    assert pred == Expressions.and_(pred, Expressions.always_true())

    assert Expressions.always_false() == Expressions.and_(Expressions.always_false(), pred)
    assert Expressions.always_false() == Expressions.and_(pred, Expressions.always_false())


def test_simplify_not():
    assert Expressions.always_false() == Expressions.not_(Expressions.always_true())
    assert Expressions.always_true() == Expressions.not_(Expressions.always_false())
    assert pred == Expressions.not_(Expressions.not_(pred))


def test_null_name():
    with raises(RuntimeError):
        Expressions.equal(None, 5)
