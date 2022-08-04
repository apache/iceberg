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
"""Base FileIO classes for implementing reading and writing table files

The FileIO abstraction includes a subset of full filesystem implementations. Specifically,
Iceberg needs to read or write a file at a given location (as a seekable stream), as well
as check if a file exists. An implementation of the FileIO abstract base class is responsible
for returning an InputFile instance, an OutputFile instance, and deleting a file given
its location.
"""
from abc import ABC, abstractmethod
from io import SEEK_SET
from typing import Protocol, Union, runtime_checkable


@runtime_checkable
class InputStream(Protocol):
    """A protocol for the file-like object returned by InputFile.open(...)

    This outlines the minimally required methods for a seekable input stream returned from an InputFile
    implementation's `open(...)` method. These methods are a subset of IOBase/RawIOBase.
    """

    @abstractmethod
    def read(self, size: int = 0) -> bytes:
        ...

    @abstractmethod
    def seek(self, offset: int, whence: int = SEEK_SET) -> int:
        ...

    @abstractmethod
    def tell(self) -> int:
        ...

    @abstractmethod
    def close(self) -> None:
        ...


@runtime_checkable
class OutputStream(Protocol):  # pragma: no cover
    """A protocol for the file-like object returned by OutputFile.create(...)

    This outlines the minimally required methods for a writable output stream returned from an OutputFile
    implementation's `create(...)` method. These methods are a subset of IOBase/RawIOBase.
    """

    @abstractmethod
    def write(self, b: bytes) -> int:
        ...

    @abstractmethod
    def close(self) -> None:
        ...


class InputFile(ABC):
    """A base class for InputFile implementations

    Args:
        location(str): A URI or a path to a local file

    Attributes:
        location(str): The URI or path to a local file for an InputFile instance
        exists(bool): Whether the file exists or not
    """

    def __init__(self, location: str):
        self._location = location

    @abstractmethod
    def __len__(self) -> int:
        """Returns the total length of the file, in bytes"""

    @property
    def location(self) -> str:
        """The fully-qualified location of the input file"""
        return self._location

    @abstractmethod
    def exists(self) -> bool:
        """Checks whether the location exists


        Raises:
            PermissionError: If the file at self.location cannot be accessed due to a permission error
        """

    @abstractmethod
    def open(self) -> InputStream:
        """This method should return an object that matches the InputStream protocol

        Returns:
            InputStream: An object that matches the InputStream protocol

        Raises:
            PermissionError: If the file at self.location cannot be accessed due to a permission error
            FileNotFoundError: If the file at self.location does not exist
        """


class OutputFile(ABC):
    """A base class for OutputFile implementations

    Args:
        location(str): A URI or a path to a local file

    Attributes:
        location(str): The URI or path to a local file for an OutputFile instance
        exists(bool): Whether the file exists or not
    """

    def __init__(self, location: str):
        self._location = location

    @abstractmethod
    def __len__(self) -> int:
        """Returns the total length of the file, in bytes"""

    @property
    def location(self) -> str:
        """The fully-qualified location of the output file"""
        return self._location

    @abstractmethod
    def exists(self) -> bool:
        """Checks whether the location exists


        Raises:
            PermissionError: If the file at self.location cannot be accessed due to a permission error
        """

    @abstractmethod
    def to_input_file(self) -> InputFile:
        """Returns an InputFile for the location of this output file"""

    @abstractmethod
    def create(self, overwrite: bool = False) -> OutputStream:
        """This method should return an object that matches the OutputStream protocol.

        Args:
            overwrite(bool): If the file already exists at `self.location`
            and `overwrite` is False a FileExistsError should be raised

        Returns:
            OutputStream: An object that matches the OutputStream protocol

        Raises:
            PermissionError: If the file at self.location cannot be accessed due to a permission error
            FileExistsError: If the file at self.location already exists and `overwrite=False`
        """


class FileIO(ABC):
    """A base class for FileIO implementations"""

    @abstractmethod
    def new_input(self, location: str) -> InputFile:
        """Get an InputFile instance to read bytes from the file at the given location

        Args:
            location(str): A URI or a path to a local file
        """

    @abstractmethod
    def new_output(self, location: str) -> OutputFile:
        """Get an OutputFile instance to write bytes to the file at the given location

        Args:
            location(str): A URI or a path to a local file
        """

    @abstractmethod
    def mkdir(self, location: Union[str, InputFile, OutputFile], recursive: bool = True) -> None:
        """Creates the directory (including parent directories)

        Args:
            location: The path to the directory
            recursive: Create the parent directories recursively

        Raises:
            PermissionError: If the file at location cannot be accessed due to a permission error
        """

    @abstractmethod
    def delete(self, location: Union[str, InputFile, OutputFile]) -> None:
        """Delete the file at the given path

        Args:
            location(str, InputFile, OutputFile): A URI or a path to a local file--if an InputFile instance or
            an OutputFile instance is provided, the location attribute for that instance is used as the URI to delete

        Raises:
            PermissionError: If the file at location cannot be accessed due to a permission error
            FileNotFoundError: When the file at the provided location does not exist
        """
