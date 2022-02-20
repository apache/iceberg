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

import os
import tempfile
from unittest.mock import MagicMock

import pytest
from pyarrow.fs import FileType

from iceberg.io.base import InputStream, OutputStream
from iceberg.io.pyarrow import PyArrowFile


def test_pyarrow_input_file():
    """Test reading a file using PyArrowFile"""

    with tempfile.TemporaryDirectory() as tmpdirname:
        file_location = os.path.join(tmpdirname, "foo.txt")
        with open(file_location, "wb") as f:
            f.write(b"foo")

        # Confirm that the file initially exists
        assert os.path.exists(file_location)

        # Instantiate the input file
        absolute_file_location = os.path.abspath(file_location)
        input_file = PyArrowFile(location=f"{absolute_file_location}")

        # Test opening and reading the file
        f = input_file.open()
        assert isinstance(f, InputStream)  # Test that the file object abides by the InputStream protocol
        data = f.read()
        assert data == b"foo"
        assert len(input_file) == 3


def test_pyarrow_output_file():
    """Test writing a file using PyArrowFile"""

    with tempfile.TemporaryDirectory() as tmpdirname:
        file_location = os.path.join(tmpdirname, "foo.txt")

        # Instantiate the output file
        absolute_file_location = os.path.abspath(file_location)
        output_file = PyArrowFile(location=f"{absolute_file_location}")

        # Create the output file and write to it
        f = output_file.create()
        assert isinstance(f, OutputStream)  # Test that the file object abides by the OutputStream protocol
        f.write(b"foo")

        # Confirm that bytes were written
        with open(file_location, "rb") as f:
            assert f.read() == b"foo"

        assert len(output_file) == 3


def test_pyarrow_invalid_scheme():
    """Test that a ValueError is raised if a location is provided with an invalid scheme"""

    with pytest.raises(ValueError) as exc_info:
        PyArrowFile("foo://bar/baz.txt")

    assert ("Unrecognized filesystem type in URI") in str(exc_info.value)

    with pytest.raises(ValueError) as exc_info:
        PyArrowFile("foo://bar/baz.txt")

    assert ("Unrecognized filesystem type in URI") in str(exc_info.value)


def test_pyarrow_violating_input_stream_protocol():
    """Test that a TypeError is raised if an input file is provided that violates the InputStream protocol"""

    # Missing seek, tell, closed, and close
    input_file_mock = MagicMock(spec=["read"])

    # Create a mocked filesystem that returns input_file_mock
    filesystem_mock = MagicMock()
    filesystem_mock.open_input_file.return_value = input_file_mock

    input_file = PyArrowFile("foo.txt")
    input_file._filesystem = filesystem_mock

    f = input_file.open()
    assert not isinstance(f, InputStream)


def test_pyarrow_violating_output_stream_protocol():
    """Test that a TypeError is raised if an output stream is provided that violates the OutputStream protocol"""

    # Missing closed, and close
    output_file_mock = MagicMock(spec=["write", "exists"])
    output_file_mock.exists.return_value = False

    file_info_mock = MagicMock()
    file_info_mock.type = FileType.NotFound

    # Create a mocked filesystem that returns output_file_mock
    filesystem_mock = MagicMock()
    filesystem_mock.open_output_stream.return_value = output_file_mock
    filesystem_mock.get_file_info.return_value = file_info_mock

    output_file = PyArrowFile("foo.txt")
    output_file._filesystem = filesystem_mock

    f = output_file.create()

    assert not isinstance(f, OutputStream)
