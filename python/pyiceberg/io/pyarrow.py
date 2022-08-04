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

from pyarrow.fs import FileInfo, FileSystem, FileType

from pyiceberg.io.base import (
    FileIO,
    InputFile,
    InputStream,
    OutputFile,
    OutputStream,
)


class PyArrowFile(InputFile, OutputFile):
    """A combined InputFile and OutputFile implementation that uses a pyarrow filesystem to generate pyarrow.lib.NativeFile instances

    Args:
        location(str): A URI or a path to a local file

    Attributes:
        location(str): The URI or path to a local file for a PyArrowFile instance

    Examples:
        >>> from pyiceberg.io.pyarrow import PyArrowFile
        >>> # input_file = PyArrowFile("s3://foo/bar.txt")
        >>> # Read the contents of the PyArrowFile instance
        >>> # Make sure that you have permissions to read/write
        >>> # file_content = input_file.open().read()

        >>> # output_file = PyArrowFile("s3://baz/qux.txt")
        >>> # Write bytes to a file
        >>> # Make sure that you have permissions to read/write
        >>> # output_file.create().write(b'foobytes')
    """

    def __init__(self, location: str):
        parsed_location = urlparse(location)  # Create a ParseResult from the URI
        if not parsed_location.scheme:  # If no scheme, assume the path is to a local file
            self._filesystem, self._path = FileSystem.from_uri(os.path.abspath(location))
        else:
            self._filesystem, self._path = FileSystem.from_uri(location)  # Infer the proper filesystem
        super().__init__(location=location)

    def _file_info(self) -> FileInfo:
        """Retrieves a pyarrow.fs.FileInfo object for the location

        Raises:
            PermissionError: If the file at self.location cannot be accessed due to a permission error such as
                an AWS error code 15
        """
        try:
            file_info = self._filesystem.get_file_info(self._path)
        except OSError as e:
            if e.errno == 13 or "AWS Error [code 15]" in str(e):
                raise PermissionError(f"Cannot get file info, access denied: {self.location}") from e
            raise  # pragma: no cover - If some other kind of OSError, raise the raw error

        if file_info.type == FileType.NotFound:
            raise FileNotFoundError(f"Cannot get file info, file not found: {self.location}")
        return file_info

    def __len__(self) -> int:
        """Returns the total length of the file, in bytes"""
        file_info = self._file_info()
        return file_info.size

    def exists(self) -> bool:
        """Checks whether the location exists"""
        try:
            self._file_info()  # raises FileNotFoundError if it does not exist
            return True
        except FileNotFoundError:
            return False

    def open(self) -> InputStream:
        """Opens the location using a PyArrow FileSystem inferred from the location

        Returns:
            pyarrow.lib.NativeFile: A NativeFile instance for the file located at `self.location`

        Raises:
            FileNotFoundError: If the file at self.location does not exist
            PermissionError: If the file at self.location cannot be accessed due to a permission error such as
                an AWS error code 15
        """
        try:
            input_file = self._filesystem.open_input_file(self._path)
        except FileNotFoundError:
            raise
        except PermissionError:
            raise
        except OSError as e:
            if e.errno == 2 or "Path does not exist" in str(e):
                raise FileNotFoundError(f"Cannot open file, does not exist: {self.location}") from e
            elif e.errno == 13 or "AWS Error [code 15]" in str(e):
                raise PermissionError(f"Cannot open file, access denied: {self.location}") from e
            raise  # pragma: no cover - If some other kind of OSError, raise the raw error
        return input_file

    def create(self, overwrite: bool = False) -> OutputStream:
        """Creates a writable pyarrow.lib.NativeFile for this PyArrowFile's location

        Args:
            overwrite(bool): Whether to overwrite the file if it already exists

        Returns:
            pyarrow.lib.NativeFile: A NativeFile instance for the file located at self.location

        Raises:
            FileExistsError: If the file already exists at `self.location` and `overwrite` is False

        Note:
            This retrieves a pyarrow NativeFile by opening an output stream. If overwrite is set to False,
            a check is first performed to verify that the file does not exist. This is not thread-safe and
            a possibility does exist that the file can be created by a concurrent process after the existence
            check yet before the output stream is created. In such a case, the default pyarrow behavior will
            truncate the contents of the existing file when opening the output stream.
        """
        try:
            if not overwrite and self.exists() is True:
                raise FileExistsError(f"Cannot create file, already exists: {self.location}")
            output_file = self._filesystem.open_output_stream(self._path)
        except PermissionError:
            raise
        except OSError as e:
            if e.errno == 13 or "AWS Error [code 15]" in str(e):
                raise PermissionError(f"Cannot create file, access denied: {self.location}") from e
            raise  # pragma: no cover - If some other kind of OSError, raise the raw error
        return output_file

    def to_input_file(self) -> "PyArrowFile":
        """Returns a new PyArrowFile for the location of an existing PyArrowFile instance

        This method is included to abide by the OutputFile abstract base class. Since this implementation uses a single
        PyArrowFile class (as opposed to separate InputFile and OutputFile implementations), this method effectively returns
        a copy of the same instance.
        """
        return self


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

    def mkdir(self, location: Union[str, InputFile, OutputFile], recursive: bool = True):
        """Creates a directory

        Args:
            location: The path to the directory
            recursive: Create the parent directories recursively

        Raises:
            PermissionError: If the file at location cannot be accessed due to a permission error
        """
        str_path = location.location if isinstance(location, (InputFile, OutputFile)) else location
        filesystem, path = FileSystem.from_uri(str_path)  # Infer the proper filesystem
        filesystem.create_dir(path, recursive=recursive)

    def delete(self, location: Union[str, InputFile, OutputFile]) -> None:
        """Delete the file at the given location

        Args:
            location(str, InputFile, OutputFile): The URI to the file--if an InputFile instance or an
            OutputFile instance is provided, the location attribute for that instance is used as the location
            to delete

        Raises:
            FileNotFoundError: When the file at the provided location does not exist
            PermissionError: If the file at the provided location cannot be accessed due to a permission error such as
                an AWS error code 15
        """
        str_path = location.location if isinstance(location, (InputFile, OutputFile)) else location
        filesystem, path = FileSystem.from_uri(str_path)  # Infer the proper filesystem
        try:
            filesystem.delete_file(path)
        except FileNotFoundError:
            raise
        except PermissionError:
            raise
        except OSError as e:
            if e.errno == 2 or "Path does not exist" in str(e):
                raise FileNotFoundError(f"Cannot delete file, does not exist: {location}") from e
            elif e.errno == 13 or "AWS Error [code 15]" in str(e):
                raise PermissionError(f"Cannot delete file, access denied: {location}") from e
            raise  # pragma: no cover - If some other kind of OSError, raise the raw error
