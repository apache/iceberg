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

from iceberg.catalog import base


def test_namespace_init():
    namespace = base.Namespace("foo", "bar", "baz")

    assert namespace.levels == ("foo", "bar", "baz")
    assert namespace.empty() is False
    assert namespace.level(0) == "foo"
    assert namespace.level(1) == "bar"
    assert namespace.level(2) == "baz"
    assert hash(namespace) == 1445470134
    assert len(namespace) == 3
    assert str(namespace) == "foo.bar.baz"
    assert namespace == namespace


def test_namespace_raise_on_invalid_pos():
    namespace = base.Namespace("foo", "bar", "baz")

    with pytest.raises(ValueError) as exc_info:
        namespace.level(3)

    assert "Cannot find level at position: 3" in str(exc_info.value)


def test_empty_namespace():
    namespace = base.Namespace()

    assert namespace.levels == ()
    assert namespace.empty() is True
    assert hash(namespace) == 0
    assert len(namespace) == 0
    assert str(namespace) == ""


@pytest.mark.parametrize(
    "namespace1, namespace2, expected_equality",
    [
        (base.Namespace("foo", "bar", "baz"), base.Namespace("foo", "bar", "baz"), True),
        (base.Namespace("qux", "quux", "quuz"), base.Namespace("qux", "quux", "quuz"), True),
        (base.Namespace("foo", "bar", "baz"), base.Namespace("qux", "quux", "quuz"), False),
        (base.Namespace("foo", "bar", "baz"), "foo", False),
        (base.Namespace("foo", "bar", "baz"), None, False),
    ],
)
def test_namespace_equality(namespace1, namespace2, expected_equality):
    assert (namespace1 == namespace2) == expected_equality


@pytest.mark.parametrize(
    "namespace, expected_hash",
    [
        (base.Namespace("foo"), -156908512),
        (base.Namespace("foo", "bar"), -1530604355),
        (base.Namespace("foo", "bar", "baz"), 1445470134),
        (base.Namespace("qux"), 1746253855),
        (base.Namespace("qux", "quux"), -1459570539),
        (base.Namespace("qux", "quux", "quuz"), -700696049),
    ],
)
def test_namespace_hash(namespace, expected_hash):
    assert hash(namespace) == expected_hash


@pytest.mark.parametrize(
    "namespace, expected_repr",
    [
        (base.Namespace("foo"), 'Namespace("foo")'),
        (base.Namespace("foo", "bar"), 'Namespace("foo", "bar")'),
        (base.Namespace("foo", "bar", "baz"), 'Namespace("foo", "bar", "baz")'),
    ],
)
def test_namespace_repr(namespace, expected_repr):
    assert repr(namespace) == expected_repr


@pytest.mark.parametrize(
    "namespace_levels, expected_exception_messages",
    [
        ((None, "bar", "baz"), "Cannot create a namespace with a None level"),
        (("foo", None, "baz"), "Cannot create a namespace with a None level"),
        (("foo", "bar", None), "Cannot create a namespace with a None level"),
        ((None, None, None), "Cannot create a namespace with a None level"),
        (("", "bar", "baz"), "Cannot create a namespace with a None level"),
        (("foo", "", "baz"), "Cannot create a namespace with a None level"),
        (("foo", "bar", ""), "Cannot create a namespace with a None level"),
        (("", "", ""), "Cannot create a namespace with a None level"),
    ],
)
def test_namespace_raise_on_null_level(namespace_levels, expected_exception_messages):
    with pytest.raises(ValueError) as exc_info:
        base.Namespace(*namespace_levels)

    assert expected_exception_messages in str(exc_info.value)


@pytest.mark.parametrize(
    "namespace_levels, expected_exception_messages",
    [
        (("foo\x00", "bar", "baz"), "Cannot create a namespace with the null-byte character: foo\x00"),
        (("foo", "bar\u0000", "baz"), "Cannot create a namespace with the null-byte character: bar\x00"),
        (("foo", "\u0000bar", "baz"), "Cannot create a namespace with the null-byte character: \x00bar"),
        (("foo", "\u0000bar", "baz\x00"), "Cannot create a namespace with the null-byte character: \x00bar"),
        (("\x00foo", "\u0000bar", "baz"), "Cannot create a namespace with the null-byte character: \x00foo"),
    ],
)
def test_namespace_raise_on_null_byte_in_level(namespace_levels, expected_exception_messages):
    with pytest.raises(ValueError) as exc_info:
        base.Namespace(*namespace_levels)

    assert expected_exception_messages in str(exc_info.value)
