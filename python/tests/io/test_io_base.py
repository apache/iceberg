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

from iceberg.io.pyarrow import PyArrowFile, PyArrowFileIO


@pytest.mark.parametrize("CustomInputFileFixture", ["LocalInputFileFixture", PyArrowFile])
def test_custom_local_input_file(CustomInputFileFixture, request):
    """Test initializing an InputFile implementation to read a local file"""
    # If a fixture name is used, retrieve the fixture, otherwise assume that a InputFile class was passed directly
    CustomInputFile = (
        request.getfixturevalue(CustomInputFileFixture) if isinstance(CustomInputFileFixture, str) else CustomInputFileFixture
    )

    with tempfile.TemporaryDirectory() as tmpdirname:
        file_location = os.path.join(tmpdirname, "foo.txt")
        with open(file_location, "wb") as f:
            f.write(b"foo")

        # Confirm that the file initially exists
        assert os.path.exists(file_location)

        # Instantiate the input file
        absolute_file_location = os.path.abspath(file_location)
        input_file = CustomInputFile(location=f"{absolute_file_location}")

        # Test opening and reading the file
        f = input_file.open()
        data = f.read()
        assert data == b"foo"
        assert len(input_file) == 3


@pytest.mark.parametrize("CustomOutputFileFixture", ["LocalOutputFileFixture", PyArrowFile])
def test_custom_local_output_file(CustomOutputFileFixture, request):
    """Test initializing an OutputFile implementation to write to a local file"""
    # If a fixture name is used, retrieve the fixture, otherwise assume that a OutputFile class was passed directly
    CustomOutputFile = (
        request.getfixturevalue(CustomOutputFileFixture) if isinstance(CustomOutputFileFixture, str) else CustomOutputFileFixture
    )

    with tempfile.TemporaryDirectory() as tmpdirname:
        file_location = os.path.join(tmpdirname, "foo.txt")

        # Instantiate the output file
        absolute_file_location = os.path.abspath(file_location)
        output_file = CustomOutputFile(location=f"{absolute_file_location}")

        # Create the output file and write to it
        f = output_file.create()
        f.write(b"foo")

        # Confirm that bytes were written
        with open(file_location, "rb") as f:
            assert f.read() == b"foo"

        assert len(output_file) == 3


@pytest.mark.parametrize("CustomOutputFileFixture", ["LocalOutputFileFixture", PyArrowFile])
def test_custom_local_output_file_with_overwrite(CustomOutputFileFixture, request):
    """Test initializing an OutputFile implementation to overwrite a local file"""
    # If a fixture name is used, retrieve the fixture, otherwise assume that a OutputFile class was passed directly
    CustomOutputFile = (
        request.getfixturevalue(CustomOutputFileFixture) if isinstance(CustomOutputFileFixture, str) else CustomOutputFileFixture
    )

    with tempfile.TemporaryDirectory() as tmpdirname:
        output_file_location = os.path.join(tmpdirname, "foo.txt")

        # Create a file in the temporary directory
        with open(output_file_location, "wb") as f:
            f.write(b"foo")

        # Instantiate an output file
        output_file = CustomOutputFile(location=f"{output_file_location}")

        # Confirm that a FileExistsError is raised when overwrite=False
        with pytest.raises(FileExistsError):
            f = output_file.create(overwrite=False)
            f.write(b"foo")

        # Confirm that the file is overwritten with overwrite=True
        f = output_file.create(overwrite=True)
        f.write(b"bar")
        with open(output_file_location, "rb") as f:
            assert f.read() == b"bar"


@pytest.mark.parametrize("CustomFileFixture", ["LocalInputFileFixture", "LocalOutputFileFixture", PyArrowFile, PyArrowFile])
def test_custom_file_exists(CustomFileFixture, request):
    """Test that the exists property returns the proper value for existing and non-existing files"""
    # If a fixture name is used, retrieve the fixture, otherwise assume that a file class was passed directly
    CustomFile = request.getfixturevalue(CustomFileFixture) if isinstance(CustomFileFixture, str) else CustomFileFixture

    with tempfile.TemporaryDirectory() as tmpdirname:
        file_location = os.path.join(tmpdirname, "foo.txt")
        with open(file_location, "wb") as f:
            f.write(b"foo")

        nonexistent_file_location = os.path.join(tmpdirname, "bar.txt")

        # Confirm that the file initially exists
        assert os.path.exists(file_location)

        # Get an absolute path for an existing file and a nonexistent file
        absolute_file_location = os.path.abspath(file_location)
        non_existent_absolute_file_location = os.path.abspath(nonexistent_file_location)

        # Create File instances
        file = CustomFile(location=f"{absolute_file_location}")
        non_existent_file = CustomFile(location=f"{non_existent_absolute_file_location}")

        # Test opening and reading the file
        assert file.exists()
        assert not non_existent_file.exists()


@pytest.mark.parametrize("CustomOutputFileFixture", ["LocalOutputFileFixture", PyArrowFile])
def test_output_file_to_input_file(CustomOutputFileFixture, request):
    """Test initializing an InputFile using the `to_input_file()` method on an OutputFile instance"""
    # If a fixture name is used, retrieve the fixture, otherwise assume that a OutputFile class was passed directly
    CustomOutputFile = (
        request.getfixturevalue(CustomOutputFileFixture) if isinstance(CustomOutputFileFixture, str) else CustomOutputFileFixture
    )

    with tempfile.TemporaryDirectory() as tmpdirname:
        output_file_location = os.path.join(tmpdirname, "foo.txt")

        # Create an output file instance
        output_file = CustomOutputFile(location=f"{output_file_location}")

        # Create the output file and write to it
        f = output_file.create()
        f.write(b"foo")

        # Convert to an input file and confirm the contents
        input_file = output_file.to_input_file()
        with input_file.open() as f:
            assert f.read() == b"foo"


@pytest.mark.parametrize(
    "CustomFileIOFixture,string_uri",
    [
        ("LocalFileIOFixture", "foo/bar.parquet"),
        ("LocalFileIOFixture", "file:///foo/bar.parquet"),
        ("LocalFileIOFixture", "file:/foo/bar/baz.parquet"),
        (PyArrowFileIO, "foo/bar/baz.parquet"),
        (PyArrowFileIO, "file:/foo/bar/baz.parquet"),
        (PyArrowFileIO, "file:/foo/bar/baz.parquet"),
    ],
)
def test_custom_file_io_locations(CustomFileIOFixture, string_uri, request):
    """Test that the location property is maintained as the value of the location argument"""
    # If a fixture name is used, retrieve the fixture, otherwise assume that a FileIO class was passed directly
    CustomFileIO = request.getfixturevalue(CustomFileIOFixture) if isinstance(CustomFileIOFixture, str) else CustomFileIOFixture

    # Instantiate the file-io and create a new input and output file
    file_io = CustomFileIO()
    input_file = file_io.new_input(location=string_uri)
    assert input_file.location == string_uri

    output_file = file_io.new_output(location=string_uri)
    assert output_file.location == string_uri


@pytest.mark.parametrize(
    "string_uri_w_netloc",
    ["file://localhost:80/foo/bar.parquet", "file://foo/bar.parquet"],
)
def test_raise_on_network_location_in_input_file(string_uri_w_netloc, request):
    """Test raising a ValueError when providing a network location to a LocalInputFile"""
    LocalInputFile = request.getfixturevalue("LocalInputFileFixture")
    with pytest.raises(ValueError) as exc_info:
        LocalInputFile(location=string_uri_w_netloc)

    assert ("Network location is not allowed for LocalInputFile") in str(exc_info.value)


@pytest.mark.parametrize(
    "string_uri_w_netloc",
    ["file://localhost:80/foo/bar.parquet", "file://foo/bar.parquet"],
)
def test_raise_on_network_location_in_output_file(string_uri_w_netloc, request):
    """Test raising a ValueError when providing a network location to a LocalOutputFile"""
    LocalInputFile = request.getfixturevalue("LocalInputFileFixture")
    with pytest.raises(ValueError) as exc_info:
        LocalInputFile(location=string_uri_w_netloc)

    assert ("Network location is not allowed for LocalInputFile") in str(exc_info.value)


@pytest.mark.parametrize("CustomFileIOFixture", ["LocalFileIOFixture", PyArrowFileIO])
def test_deleting_local_file_using_file_io(CustomFileIOFixture, request):
    """Test deleting a local file using FileIO.delete(...)"""
    # If a fixture name is used, retrieve the fixture, otherwise assume that a FileIO class was passed directly
    CustomFileIO = request.getfixturevalue(CustomFileIOFixture) if isinstance(CustomFileIOFixture, str) else CustomFileIOFixture

    with tempfile.TemporaryDirectory() as tmpdirname:
        # Write to the temporary file
        output_file_location = os.path.join(tmpdirname, "foo.txt")
        with open(output_file_location, "wb") as f:
            f.write(b"foo")

        # Instantiate the file-io
        file_io = CustomFileIO()

        # Confirm that the file initially exists
        assert os.path.exists(output_file_location)

        # Delete the file using the file-io implementations delete method
        file_io.delete(output_file_location)

        # Confirm that the file no longer exists
        assert not os.path.exists(output_file_location)


@pytest.mark.parametrize("CustomFileIOFixture", ["LocalFileIOFixture", PyArrowFileIO])
def test_raise_file_not_found_error_for_fileio_delete(CustomFileIOFixture, request):
    """Test raising a FileNotFound error when trying to delete a non-existent file"""
    # If a fixture name is used, retrieve the fixture, otherwise assume that a FileIO class was passed directly
    CustomFileIO = request.getfixturevalue(CustomFileIOFixture) if isinstance(CustomFileIOFixture, str) else CustomFileIOFixture

    with tempfile.TemporaryDirectory() as tmpdirname:
        # Write to the temporary file
        output_file_location = os.path.join(tmpdirname, "foo.txt")

        # Instantiate the file-io
        file_io = CustomFileIO()

        # Delete the non-existent file using the file-io implementations delete method
        with pytest.raises(FileNotFoundError) as exc_info:
            file_io.delete(output_file_location)

        assert (f"Cannot delete file") in str(exc_info.value)

        # Confirm that the file no longer exists
        assert not os.path.exists(output_file_location)


@pytest.mark.parametrize(
    "CustomFileIOFixture, CustomInputFileFixture", [("LocalFileIOFixture", "LocalInputFileFixture"), (PyArrowFileIO, PyArrowFile)]
)
def test_deleting_local_file_using_file_io_input_file(CustomFileIOFixture, CustomInputFileFixture, request):
    """Test deleting a local file by passing an InputFile instance to FileIO.delete(...)"""
    # If a fixture name is used, retrieve the fixture, otherwise assume that a FileIO class was passed directly
    CustomFileIO = request.getfixturevalue(CustomFileIOFixture) if isinstance(CustomFileIOFixture, str) else CustomFileIOFixture

    # If a fixture name is used, retrieve the fixture, otherwise assume that a InputFile class was passed directly
    CustomInputFile = (
        request.getfixturevalue(CustomInputFileFixture) if isinstance(CustomInputFileFixture, str) else CustomInputFileFixture
    )

    with tempfile.TemporaryDirectory() as tmpdirname:
        # Write to the temporary file
        file_location = os.path.join(tmpdirname, "foo.txt")
        with open(file_location, "wb") as f:
            f.write(b"foo")

        # Instantiate the file-io
        file_io = CustomFileIO()

        # Confirm that the file initially exists
        assert os.path.exists(file_location)

        # Instantiate the custom InputFile
        input_file = CustomInputFile(location=f"{file_location}")

        # Delete the file using the file-io implementations delete method
        file_io.delete(input_file)

        # Confirm that the file no longer exists
        assert not os.path.exists(file_location)


@pytest.mark.parametrize(
    "CustomFileIOFixture, CustomOutputFileFixture",
    [("LocalFileIOFixture", "LocalOutputFileFixture"), (PyArrowFileIO, PyArrowFile)],
)
def test_deleting_local_file_using_file_io_output_file(CustomFileIOFixture, CustomOutputFileFixture, request):
    """Test deleting a local file by passing an OutputFile instance to FileIO.delete(...)"""
    # If a fixture name is used, retrieve the fixture, otherwise assume that a FileIO class was passed directly
    CustomFileIO = request.getfixturevalue(CustomFileIOFixture) if isinstance(CustomFileIOFixture, str) else CustomFileIOFixture

    # If a fixture name is used, retrieve the fixture, otherwise assume that a OutputFile class was passed directly
    CustomOutputFile = (
        request.getfixturevalue(CustomOutputFileFixture) if isinstance(CustomOutputFileFixture, str) else CustomOutputFileFixture
    )

    with tempfile.TemporaryDirectory() as tmpdirname:
        # Write to the temporary file
        file_location = os.path.join(tmpdirname, "foo.txt")
        with open(file_location, "wb") as f:
            f.write(b"foo")

        # Instantiate the file-io
        file_io = CustomFileIO()

        # Confirm that the file initially exists
        assert os.path.exists(file_location)

        # Instantiate the custom OutputFile
        output_file = CustomOutputFile(location=f"{file_location}")

        # Delete the file using the file-io implementations delete method
        file_io.delete(output_file)

        # Confirm that the file no longer exists
        assert not os.path.exists(file_location)
