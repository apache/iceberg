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
"""FileIO implementation for reading and writing table files that uses pyarrow.fs

This file contains a FileIO implementation that relies on the filesystem interface provided
by PyArrow. It relies on PyArrow's `from_uri` method that infers the correct filesytem
type to use. Theoretically, this allows the supported storage types to grow naturally
with the pyarrow library.
"""

from typing import Union
from urllib.parse import ParseResult, urlparse

from pyarrow.fs import FileSystem, FileType

from iceberg.io.base import FileIO, InputFile, InputStream, OutputFile, OutputStream

SUPPORTED_SCHEMES = [
    "file",
    "mock",
    "s3fs",
    "hdfs",
    "viewfs",
]


class PyArrowFile(InputFile, OutputFile):
    """A combined InputFile and OutputFile implementation that uses a pyarrow filesystem to generate pyarrow.lib.NativeFile instances

    Args:
        location(str): A URI or a path to a local file

    Attributes:
        location(str): The URI or path to a local file for a PyArrowFile instance
        parsed_location(urllib.parse.ParseResult): The parsed location with attributes `scheme`, `netloc`, `path`, `params`,
          `query`, and `fragment`
        exists(bool): Whether the file exists or not

    Examples:
        >>> from iceberg.io.pyarrow import PyArrowFile
        >>> input_file = PyArrowFile("s3://foo/bar.txt")
        >>> file_content = input_file.open().read()  # Read the contents of the PyArrowFile instance
        >>> output_file = PyArrowFile("s3://baz/qux.txt")
        >>> output_file.create().write(b'foobytes')  # Write bytes to the PyarrowFile instance
    """

    def __init__(self, location: str):
        parsed_location = urlparse(location)  # Create a ParseResult from the uri

        if parsed_location.scheme and parsed_location.scheme not in SUPPORTED_SCHEMES:
            raise ValueError(f"PyArrowFile location must have one of the following schemes: {SUPPORTED_SCHEMES}")

        super().__init__(location=location)
        self._parsed_location = parsed_location

    def __len__(self) -> int:
        """Returns the total length of the file, in bytes"""
        filesytem, path = FileSystem.from_uri(self.location)  # Infer the proper filesystem
        file = filesytem.open_input_file(path)
        return file.size()

    @property
    def parsed_location(self) -> ParseResult:
        """The parsed location

        Returns:
            ParseResult: The parsed results which has attributes `scheme`, `netloc`, `path`,
            `params`, `query`, and `fragments`.
        """
        return self._parsed_location

    @property
    def exists(self) -> bool:
        """Checks whether the file exists"""
        filesytem, path = FileSystem.from_uri(self.location)  # Infer the proper filesystem
        file_info = filesytem.get_file_info(path)
        return False if file_info.type == FileType.NotFound else True

    def open(self) -> InputStream:
        """Opens the location using a PyArrow FileSystem inferred from the scheme

        Returns:
            pyarrow.lib.NativeFile: A NativeFile instance for the file located at self.location
        """
        filesytem, path = FileSystem.from_uri(self.location)  # Infer the proper filesystem
        input_file = filesytem.open_input_file(path)
        if not isinstance(input_file, InputStream):
            raise TypeError("""Object returned from PyArrowFile.open does not match the InputStream protocol.""")
        return input_file

    def create(self, overwrite: bool = False) -> OutputStream:
        """Creates a writable pyarrow.lib.NativeFile for this PyArrowFile's location

        Args:
            overwrite(bool): Whether to overwrite the file if it already exists

        Returns:
            pyarrow.lib.NativeFile: A NativeFile instance for the file located at self.location

        Raises:
            FileExistsError: If the file already exists at `self.location` and `overwrite` is False
        """
        if not overwrite and self.exists:
            raise FileExistsError(
                f"A file already exists at this location. If you would like to overwrite it, set `overwrite=True`: {self.location}"
            )
        filesytem, path = FileSystem.from_uri(self.location)  # Infer the proper filesystem
        output_file = filesytem.open_output_stream(path)
        if not isinstance(output_file, OutputStream):
            raise TypeError("Object returned from PyArrowFile.create does not match the OutputStream protocol.")
        return output_file

    def to_input_file(self) -> "PyArrowFile":
        """Returns a new PyArrowFile for the location of an existing PyArrowFile instance

        This method is included to abide by the OutputFile abstract base class. Since this implementation uses a single
        PyArrowFile class (as opposed to separate InputFile and OutputFile implementations), this method effectively returns
        a copy of the same instance.
        """
        return PyArrowFile(self.location)


class PyArrowFileIO(FileIO):
    def new_input(self, location: str) -> PyArrowFile:
        """Get a PyArrowFile instance to read bytes from the file at the given location

        Args:
            location(str): A URI or a path to a local file
        """
        return PyArrowFile(location)

    def new_output(self, location: str) -> PyArrowFile:
        """Get a PyArrowFile instance to write bytes to the file at the given location

        Args:
            location(str): A URI or a path to a local file
        """
        return PyArrowFile(location)

    def delete(self, location: Union[str, InputFile, OutputFile]) -> None:
        """Delete the file at the given path

        Args:
            location(str, InputFile, OutputFile): The URI to the file--if an InputFile instance or an
            OutputFile instance is provided, the location attribute for that instance is used as the URI to delete
        """
        str_path = location.location if isinstance(location, (InputFile, OutputFile)) else location
        filesytem, path = FileSystem.from_uri(str_path)  # Infer the proper filesystem
        try:
            filesytem.delete_file(path)
        except OSError:
            raise FileNotFoundError(f"File could not be deleted because it does not exist: {str_path}")
