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

import pytest

from iceberg.io.pyarrow import PyArrowInputFile, PyArrowOutputFile


def test_pyarrow_input_file():
    with tempfile.TemporaryDirectory() as tmpdirname:
        file_location = os.path.join(tmpdirname, "foo.txt")
        with open(file_location, "wb") as f:
            f.write(b"foo")

        # Confirm that the file initially exists
        assert os.path.exists(file_location)

        # Instantiate the input file
        absolute_file_location = os.path.abspath(file_location)
        input_file = PyArrowInputFile(location=f"file:{absolute_file_location}")

        # Test opening and reading the file
        f = input_file.open()
        data = f.read()
        assert data == b"foo"
        assert len(input_file) == 3


def test_pyarrow_output_file():
    with tempfile.TemporaryDirectory() as tmpdirname:
        file_location = os.path.join(tmpdirname, "foo.txt")

        # Instantiate the output file
        absolute_file_location = os.path.abspath(file_location)
        output_file = PyArrowOutputFile(location=f"file:{absolute_file_location}")

        # Create the output file and write to it
        f = output_file.create()
        f.write(b"foo")

        # Confirm that bytes were written
        with open(file_location, "rb") as f:
            assert f.read() == b"foo"

        assert len(output_file) == 3


def test_pyarrow_invalid_scheme():
    with pytest.raises(ValueError) as exc_info:
        PyArrowInputFile("foo://bar/baz.txt")

    assert ("PyArrowInputFile location must have a scheme of `file`, `mock`, `s3fs`, `hdfs`, or `viewfs`") in str(exc_info.value)

    with pytest.raises(ValueError) as exc_info:
        PyArrowOutputFile("foo://bar/baz.txt")

    assert ("PyArrowOutputFile location must have a scheme of `file`, `mock`, `s3fs`, `hdfs`, or `viewfs`") in str(exc_info.value)
