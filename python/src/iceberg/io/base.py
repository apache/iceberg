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
from typing import Union


class InputFile(ABC):
    """A base class for InputFile implementations"""

    def __init__(self, location: str):
        self._location = location

    @abstractmethod
    def __len__(self) -> int:
        """Returns the total length of the file, in bytes"""

    @property
    def location(self) -> str:
        """The fully-qualified location of the input file"""
        return self._location

    @property
    @abstractmethod
    def exists(self) -> bool:
        """Checks whether the file exists"""

    @abstractmethod
    def open(self):
        """This method should return an instance of an seekable input stream."""


class OutputFile(ABC):
    """A base class for OutputFile implementations"""

    def __init__(self, location: str):
        self._location = location

    @abstractmethod
    def __len__(self) -> int:
        """Returns the total length of the file, in bytes"""

    @property
    def location(self) -> str:
        """The fully-qualified location of the output file"""
        return self._location

    @property
    @abstractmethod
    def exists(self) -> bool:
        """Checks whether the file exists"""

    @abstractmethod
    def to_input_file(self) -> InputFile:
        """Returns an InputFile for the location of this output file"""

    @abstractmethod
    def create(self, overwrite: bool = False):
        """This method should return a file-like object.

        Args:
            overwrite(bool): If the file already exists at `self.location`
            and `overwrite` is False a FileExistsError should be raised.
        """


class FileIO(ABC):
    @abstractmethod
    def new_input(self, location: str) -> InputFile:
        """Get an InputFile instance to read bytes from the file at the given location"""

    @abstractmethod
    def new_output(self, location: str) -> OutputFile:
        """Get an OutputFile instance to write bytes to the file at the given location"""

    @abstractmethod
    def delete(self, location: Union[str, InputFile, OutputFile]) -> None:
        """Delete the file at the given path"""
