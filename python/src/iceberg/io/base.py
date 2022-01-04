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

from abc import ABC, abstractmethod


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
    def __enter__(self):
        """Enter context for InputFile

        This method should assign a seekable stream to `self.input_stream` and
        return `self`. If the file does not exist, a FileNotFoundError should
        be raised."""

    @abstractmethod
    def __exit__(self, exc_type, exc_value, exc_traceback):
        """Exit context for InputFile

        This method should perform any necessary teardown."""


class OutputFile(ABC):
    """A base class for OutputFile implementations"""

    def __init__(self, location: str, overwrite: bool = False):
        self._location = location
        self._overwrite = overwrite

    @abstractmethod
    def __len__(self) -> int:
        """Returns the total length of the file, in bytes"""

    @property
    def location(self) -> str:
        """The fully-qualified location of the output file"""
        return self._location

    @property
    def overwrite(self) -> bool:
        """Whether or not to overwrite the file if it exists"""
        return self._overwrite

    @property
    @abstractmethod
    def exists(self) -> bool:
        """Checks whether the file exists"""

    @abstractmethod
    def to_input_file(self) -> InputFile:
        """Returns an InputFile for the location of this output file"""

    @abstractmethod
    def __enter__(self):
        """Enter context for OutputFile

        This method should return a file-like object. If the file already exists
        at `self.location` and `self.overwrite` is False a FileExistsError should
        be raised.

        Example:
            >>> with OutputFile(overwrite=True) as f:
                    content = f.read()
        """

    @abstractmethod
    def __exit__(self, exc_type, exc_value, exc_traceback):
        """Exit context for OutputFile

        This method should perform any necessary teardown.

        Example:
            >>> with OutputFile(connection=connection):
                    content = f.read()
                    connection.close()  # `__exit__` method would contain `del self._connection`
        """


class FileIO(ABC):
    @abstractmethod
    def new_input(self, location: str) -> InputFile:
        """Get an InputFile instance to read bytes from the file at the given location"""

    @abstractmethod
    def new_output(self, location: str) -> OutputFile:
        """Get an OutputFile instance to write bytes to the file at the given location"""

    @abstractmethod
    def delete(self, location: str):
        """Delete the file at the given path"""
