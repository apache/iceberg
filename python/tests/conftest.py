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
"""This contains global pytest configurations.

Fixtures contained in this file will be automatically used if provided as an argument
to any pytest function.

In the case where the fixture must be used in a pytest.mark.parametrize decorator, the string representation can be used
and the built-in pytest fixture request should be used as an additional argument in the function. The fixture can then be
retrieved using `request.getfixturevalue(fixture_name)`.
"""

import os
from typing import Any, Union
from urllib.parse import ParseResult, urlparse

import pytest

from iceberg import schema
from iceberg.io.base import FileIO, InputFile, InputStream, OutputFile, OutputStream
from iceberg.types import (
    BooleanType,
    FloatType,
    IntegerType,
    ListType,
    MapType,
    NestedField,
    StringType,
    StructType,
)


class FooStruct:
    """An example of an object that abides by StructProtocol"""

    def __init__(self):
        self.content = {}

    def get(self, pos: int) -> Any:
        return self.content[pos]

    def set(self, pos: int, value) -> None:
        self.content[pos] = value


class LocalInputFile(InputFile):
    """An InputFile implementation for local files (for test use only)"""

    def __init__(self, location: str):

        parsed_location = urlparse(location)  # Create a ParseResult from the uri
        if parsed_location.scheme and parsed_location.scheme != "file":  # Validate that a uri is provided with a scheme of `file`
            raise ValueError("LocalInputFile location must have a scheme of `file`")
        elif parsed_location.netloc:
            raise ValueError(f"Network location is not allowed for LocalInputFile: {parsed_location.netloc}")

        super().__init__(location=location)
        self._parsed_location = parsed_location

    @property
    def parsed_location(self) -> ParseResult:
        """The parsed location

        Returns:
            ParseResult: The parsed results which has attributes `scheme`, `netloc`, `path`,
            `params`, `query`, and `fragments`.
        """
        return self._parsed_location

    def __len__(self):
        return os.path.getsize(self.parsed_location.path)

    def exists(self):
        return os.path.exists(self.parsed_location.path)

    def open(self) -> InputStream:
        input_file = open(self.parsed_location.path, "rb")
        if not isinstance(input_file, InputStream):
            raise TypeError("Object returned from LocalInputFile.open() does not match the OutputStream protocol.")
        return input_file


class LocalOutputFile(OutputFile):
    """An OutputFile implementation for local files (for test use only)"""

    def __init__(self, location: str):

        parsed_location = urlparse(location)  # Create a ParseResult from the uri
        if parsed_location.scheme and parsed_location.scheme != "file":  # Validate that a uri is provided with a scheme of `file`
            raise ValueError("LocalOutputFile location must have a scheme of `file`")
        elif parsed_location.netloc:
            raise ValueError(f"Network location is not allowed for LocalOutputFile: {parsed_location.netloc}")

        super().__init__(location=location)
        self._parsed_location = parsed_location

    @property
    def parsed_location(self) -> ParseResult:
        """The parsed location

        Returns:
            ParseResult: The parsed results which has attributes `scheme`, `netloc`, `path`,
            `params`, `query`, and `fragments`.
        """
        return self._parsed_location

    def __len__(self):
        return os.path.getsize(self.parsed_location.path)

    def exists(self):
        return os.path.exists(self.parsed_location.path)

    def to_input_file(self):
        return LocalInputFile(location=self.location)

    def create(self, overwrite: bool = False) -> OutputStream:
        output_file = open(self.parsed_location.path, "wb" if overwrite else "xb")
        if not isinstance(output_file, OutputStream):
            raise TypeError("Object returned from LocalOutputFile.create(...) does not match the OutputStream protocol.")
        return output_file


class LocalFileIO(FileIO):
    """A FileIO implementation for local files (for test use only)"""

    def new_input(self, location: str):
        return LocalInputFile(location=location)

    def new_output(self, location: str):
        return LocalOutputFile(location=location)

    def delete(self, location: Union[str, LocalInputFile, LocalOutputFile]):
        parsed_location = location.parsed_location if isinstance(location, (InputFile, OutputFile)) else urlparse(location)
        try:
            os.remove(parsed_location.path)
        except FileNotFoundError as e:
            raise FileNotFoundError(f"Cannot delete file, does not exist: {parsed_location.path} - Caused by: " + str(e))


@pytest.fixture(scope="session", autouse=True)
def foo_struct():
    return FooStruct()


@pytest.fixture(scope="session", autouse=True)
def table_schema_simple():
    return schema.Schema(
        NestedField(field_id=1, name="foo", field_type=StringType(), is_optional=False),
        NestedField(field_id=2, name="bar", field_type=IntegerType(), is_optional=True),
        NestedField(field_id=3, name="baz", field_type=BooleanType(), is_optional=False),
        schema_id=1,
        identifier_field_ids=[1],
    )


@pytest.fixture(scope="session", autouse=True)
def table_schema_nested():
    return schema.Schema(
        NestedField(field_id=1, name="foo", field_type=StringType(), is_optional=False),
        NestedField(field_id=2, name="bar", field_type=IntegerType(), is_optional=True),
        NestedField(field_id=3, name="baz", field_type=BooleanType(), is_optional=False),
        NestedField(
            field_id=4,
            name="qux",
            field_type=ListType(element_id=5, element_type=StringType(), element_is_optional=True),
            is_optional=True,
        ),
        NestedField(
            field_id=6,
            name="quux",
            field_type=MapType(
                key_id=7,
                key_type=StringType(),
                value_id=8,
                value_type=MapType(
                    key_id=9, key_type=StringType(), value_id=10, value_type=IntegerType(), value_is_optional=True
                ),
                value_is_optional=True,
            ),
            is_optional=True,
        ),
        NestedField(
            field_id=11,
            name="location",
            field_type=ListType(
                element_id=12,
                element_type=StructType(
                    NestedField(field_id=13, name="latitude", field_type=FloatType(), is_optional=False),
                    NestedField(field_id=14, name="longitude", field_type=FloatType(), is_optional=False),
                ),
                element_is_optional=True,
            ),
            is_optional=True,
        ),
        NestedField(
            field_id=15,
            name="person",
            field_type=StructType(
                NestedField(field_id=16, name="name", field_type=StringType(), is_optional=False),
                NestedField(field_id=17, name="age", field_type=IntegerType(), is_optional=True),
            ),
            is_optional=False,
        ),
        schema_id=1,
        identifier_field_ids=[1],
    )


@pytest.fixture(scope="session", autouse=True)
def foo_struct():
    return FooStruct()


@pytest.fixture(scope="session", autouse=True)
def LocalInputFileFixture():
    return LocalInputFile


@pytest.fixture(scope="session", autouse=True)
def LocalOutputFileFixture():
    return LocalOutputFile


@pytest.fixture(scope="session", autouse=True)
def LocalFileIOFixture():
    return LocalFileIO
