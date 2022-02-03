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
from typing import Union
from urllib.parse import ParseResult, urlparse

import pytest

from iceberg.io.base import FileIO, InputFile, OutputFile


class LocalInputFile(InputFile):
    """An InputFile implementation for local files (for test use only)"""

    def __init__(self, location: str):

        parsed_location = urlparse(location)  # Create a ParseResult from the uri
        if parsed_location.scheme != "file":  # Validate that a uri is provided with a scheme of `file`
            raise ValueError("LocalInputFile location must have a scheme of `file`")
        elif parsed_location.netloc:
            raise ValueError(f"Network location is not allowed for LocalInputFile: {parsed_location.netloc}")

        super().__init__(location=location)
        self._parsed_location = parsed_location

    @property
    def parsed_location(self) -> ParseResult:
        return self._parsed_location

    def __len__(self):
        return os.path.getsize(self.parsed_location.path)

    def exists(self):
        return os.path.exists(self.parsed_location.path)

    def open(self):
        return open(self.parsed_location.path, "rb")


class LocalOutputFile(OutputFile):
    """An OutputFile implementation for local files (for test use only)"""

    def __init__(self, location: str):

        parsed_location = urlparse(location)  # Create a ParseResult from the uri
        if parsed_location.scheme != "file":  # Validate that a uri is provided with a scheme of `file`
            raise ValueError("LocalOutputFile location must have a scheme of `file`")
        elif parsed_location.netloc:
            raise ValueError(f"Network location is not allowed for LocalOutputFile: {parsed_location.netloc}")

        super().__init__(location=location)
        self._parsed_location = parsed_location

    @property
    def parsed_location(self) -> ParseResult:
        return self._parsed_location

    def __len__(self):
        return os.path.getsize(self.parsed_location.path)

    def exists(self):
        return os.path.exists(self.parsed_location.path)

    def to_input_file(self):
        return LocalInputFile(location=self.location)

    def create(self, overwrite: bool = False) -> None:
        return open(self.parsed_location.path, "wb" if overwrite else "xb")


class LocalFileIO(FileIO):
    """A FileIO implementation for local files (for test use only)"""

    def new_input(self, location: str):
        return LocalInputFile(location=location)

    def new_output(self, location: str):
        return LocalOutputFile(location=location)

    def delete(self, location: Union[str, LocalInputFile, LocalOutputFile]):
        parsed_location = location.parsed_location if isinstance(location, (InputFile, OutputFile)) else urlparse(location)
        os.remove(parsed_location.path)


@pytest.mark.parametrize("CustomInputFile", [LocalInputFile])
def test_custom_local_input_file(CustomInputFile):
    with tempfile.TemporaryDirectory() as tmpdirname:
        file_location = os.path.join(tmpdirname, "foo.txt")
        with open(file_location, "wb") as f:
            f.write(b"foo")

        # Confirm that the file initially exists
        assert os.path.exists(file_location)

        # Instantiate the input file
        absolute_file_location = os.path.abspath(file_location)
        input_file = CustomInputFile(location=f"file:{absolute_file_location}")

        # Test opening and reading the file
        f = input_file.open()
        data = f.read()
        assert data == b"foo"
        assert len(input_file) == 3


@pytest.mark.parametrize("CustomOutputFile", [LocalOutputFile])
def test_custom_local_output_file(CustomOutputFile):
    with tempfile.TemporaryDirectory() as tmpdirname:
        file_location = os.path.join(tmpdirname, "foo.txt")

        # Instantiate the output file
        absolute_file_location = os.path.abspath(file_location)
        output_file = CustomOutputFile(location=f"file:{absolute_file_location}")

        # Create the output file and write to it
        f = output_file.create()
        f.write(b"foo")

        # Confirm that bytes were written
        with open(file_location, "rb") as f:
            assert f.read() == b"foo"

        assert len(output_file) == 3


@pytest.mark.parametrize("CustomOutputFile", [LocalOutputFile])
def test_custom_local_output_file_with_overwrite(CustomOutputFile):
    with tempfile.TemporaryDirectory() as tmpdirname:
        output_file_location = os.path.join(tmpdirname, "foo.txt")

        # Create a file in the temporary directory
        with open(output_file_location, "wb") as f:
            f.write(b"foo")

        # Instantiate an output file
        output_file = CustomOutputFile(location=f"file://{output_file_location}")

        # Confirm that a FileExistsError is raised when overwrite=False
        with pytest.raises(FileExistsError):
            f = output_file.create(overwrite=False)
            f.write(b"foo")

        # Confirm that the file is overwritten with overwrite=True
        f = output_file.create(overwrite=True)
        f.write(b"bar")
        with open(output_file_location, "rb") as f:
            assert f.read() == b"bar"


@pytest.mark.parametrize("CustomOutputFile", [LocalOutputFile])
def test_output_file_to_input_file(CustomOutputFile):
    with tempfile.TemporaryDirectory() as tmpdirname:
        output_file_location = os.path.join(tmpdirname, "foo.txt")

        # Create an output file instance
        output_file = CustomOutputFile(location=f"file://{output_file_location}")

        # Create the output file and write to it
        f = output_file.create()
        f.write(b"foo")

        # Convert to an input file and confirm the contents
        input_file = output_file.to_input_file()
        with input_file.open() as f:
            assert f.read() == b"foo"


@pytest.mark.parametrize(
    "CustomFileIO,string_uri,scheme,netloc,path",
    [
        (LocalFileIO, "file:///foo/bar.parquet", "file", "", "/foo/bar.parquet"),
        (LocalFileIO, "file:/foo/bar/baz.parquet", "file", "", "/foo/bar/baz.parquet"),
    ],
)
def test_custom_file_io_locations(CustomFileIO, string_uri, scheme, netloc, path):
    # Instantiate the file-io and create a new input and output file
    file_io = CustomFileIO()
    input_file = file_io.new_input(location=string_uri)
    assert input_file.location == string_uri
    assert input_file.parsed_location.scheme == scheme
    assert input_file.parsed_location.netloc == netloc
    assert input_file.parsed_location.path == path

    output_file = file_io.new_output(location=string_uri)
    assert output_file.location == string_uri
    assert output_file.parsed_location.scheme == scheme
    assert output_file.parsed_location.netloc == netloc
    assert output_file.parsed_location.path == path


@pytest.mark.parametrize(
    "string_uri_w_netloc",
    ["file://localhost:80/foo/bar.parquet", "file://foo/bar.parquet"],
)
def test_raise_on_network_location_in_InputFile(string_uri_w_netloc):

    with pytest.raises(ValueError) as exc_info:
        LocalInputFile(location=string_uri_w_netloc)

    assert ("Network location is not allowed for LocalInputFile") in str(exc_info.value)


@pytest.mark.parametrize(
    "string_uri_w_netloc",
    ["file://localhost:80/foo/bar.parquet", "file://foo/bar.parquet"],
)
def test_raise_on_network_location_in_OutputFile(string_uri_w_netloc):

    with pytest.raises(ValueError) as exc_info:
        LocalInputFile(location=string_uri_w_netloc)

    assert ("Network location is not allowed for LocalInputFile") in str(exc_info.value)


@pytest.mark.parametrize("CustomFileIO", [LocalFileIO])
def test_deleting_local_file_using_file_io(CustomFileIO):

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


@pytest.mark.parametrize("CustomFileIO, CustomInputFile", [(LocalFileIO, LocalInputFile)])
def test_deleting_local_file_using_file_io_InputFile(CustomFileIO, CustomInputFile):

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
        input_file = CustomInputFile(location=f"file://{file_location}")

        # Delete the file using the file-io implementations delete method
        file_io.delete(input_file)

        # Confirm that the file no longer exists
        assert not os.path.exists(file_location)


@pytest.mark.parametrize("CustomFileIO, CustomOutputFile", [(LocalFileIO, LocalOutputFile)])
def test_deleting_local_file_using_file_io_OutputFile(CustomFileIO, CustomOutputFile):

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
        output_file = CustomOutputFile(location=f"file://{file_location}")

        # Delete the file using the file-io implementations delete method
        file_io.delete(output_file)

        # Confirm that the file no longer exists
        assert not os.path.exists(file_location)
