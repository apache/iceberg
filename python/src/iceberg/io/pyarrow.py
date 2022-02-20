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
by PyArrow. It relies on PyArrow's `from_uri` method that infers the correct filesystem
type to use. Theoretically, this allows the supported storage types to grow naturally
with the pyarrow library.
"""

import os
from typing import Union
from urllib.parse import urlparse

from pyarrow.fs import FileSystem, FileType

from iceberg.io.base import FileIO, InputFile, InputStream, OutputFile, OutputStream


class PyArrowFile(InputFile, OutputFile):
    """A combined InputFile and OutputFile implementation that uses a pyarrow filesystem to generate pyarrow.lib.NativeFile instances

    Args:
        location(str): A URI or a path to a local file

    Attributes:
        location(str): The URI or path to a local file for a PyArrowFile instance
        exists(bool): Whether the file exists or not
        filesystem(pyarrow.fs.FileSystem): An implementation of the FileSystem base class inferred from the location

    Examples:
        >>> from iceberg.io.pyarrow import PyArrowFile
        >>> input_file = PyArrowFile("s3://foo/bar.txt")
        >>> file_content = input_file.open().read()  # Read the contents of the PyArrowFile instance
        >>> output_file = PyArrowFile("s3://baz/qux.txt")
        >>> output_file.create().write(b'foobytes')  # Write bytes to the PyArrowFile instance
    """

    def __init__(self, location: str):
        parsed_location = urlparse(location)  # Create a ParseResult from the uri
        if not parsed_location.scheme:  # If no scheme, assume the path is to a local file
            self._filesystem, self._path = FileSystem.from_uri(os.path.abspath(location))
        else:
            self._filesystem, self._path = FileSystem.from_uri(location)  # Infer the proper filesystem
        super().__init__(location=location)

    def __len__(self) -> int:
        """Returns the total length of the file, in bytes"""
        file = self._filesystem.open_input_file(self._path)
        return file.size()

    def exists(self) -> bool:
        """Checks whether the file exists"""
        file_info = self._filesystem.get_file_info(self._path)
        return False if file_info.type == FileType.NotFound else True

    def open(self) -> InputStream:
        """Opens the location using a PyArrow FileSystem inferred from the location

        Returns:
            pyarrow.lib.NativeFile: A NativeFile instance for the file located at `self.location`
        """
        input_file = self._filesystem.open_input_file(self._path)
        if not isinstance(input_file, InputStream):
            raise TypeError(
                f"Object of type {type(input_file)} returned from PyArrowFile.open does not match the InputStream protocol."
            )
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
        if not overwrite and self.exists():
            raise FileExistsError(
                f"A file already exists at this location. If you would like to overwrite it, set `overwrite=True`: {self.location}"
            )
        output_file = self._filesystem.open_output_stream(self._path)
        if not isinstance(output_file, OutputStream):
            raise TypeError(
                f"Object of type {type(output_file)} returned from PyArrowFile.create(...) does not match the OutputStream protocol."
            )
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

        Returns:
            PyArrowFile: A PyArrowFile instance for the given location
        """
        return PyArrowFile(location)

    def new_output(self, location: str) -> PyArrowFile:
        """Get a PyArrowFile instance to write bytes to the file at the given location

        Args:
            location(str): A URI or a path to a local file

        Returns:
            PyArrowFile: A PyArrowFile instance for the given location
        """
        return PyArrowFile(location)

    def delete(self, location: Union[str, InputFile, OutputFile]) -> None:
        """Delete the file at the given location

        Args:
            location(str, InputFile, OutputFile): The URI to the file--if an InputFile instance or an
            OutputFile instance is provided, the location attribute for that instance is used as the location
            to delete
        """
        str_path = location.location if isinstance(location, (InputFile, OutputFile)) else location
        filesystem, path = FileSystem.from_uri(str_path)  # Infer the proper filesystem
        try:
            filesystem.delete_file(path)
        except OSError:
            raise FileNotFoundError(f"File could not be deleted because it does not exist: {str_path}")
