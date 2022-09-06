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
"""FileIO implementation for reading and writing table files that uses fsspec compatible filesystems"""

from functools import lru_cache
from typing import Callable, Union
from urllib.parse import urlparse

from fsspec import AbstractFileSystem

from pyiceberg.io import FileIO, InputFile, OutputFile
from pyiceberg.typedef import Properties


def _s3(properties: Properties) -> AbstractFileSystem:
    from s3fs import S3FileSystem

    print(f"{properties}")
    return S3FileSystem(**properties.get("s3", {}))


SCHEME_TO_FS = {
    "s3": _s3,
    "s3a": _s3,
    "s3n": _s3,
}


class FsspecInputFile(InputFile):
    """An input file implementation for the FsspecFileIO

    Args:
        location(str): A URI to a file location
        fs(AbstractFileSystem): An fsspec filesystem instance
    """

    def __init__(self, location: str, fs: AbstractFileSystem):
        self._fs = fs
        super().__init__(location=location)

    def __len__(self) -> int:
        """Returns the total length of the file, in bytes"""
        object_info = self._fs.info(self.location)
        if size := object_info.get("Size"):
            return size
        elif size := object_info.get("size"):
            return size
        raise RuntimeError(f"Cannot retrieve object info: {self.location}")

    def exists(self) -> bool:
        """Checks whether the location exists"""
        return self._fs.lexists(self.location)

    def open(self):
        """Create an input stream for reading the contents of the file

        Returns:
            OpenFile: An fsspec compliant file-like object
        """
        return self._fs.open(self.location, "rb")


class FsspecOutputFile(OutputFile):
    """An output file implementation for the FsspecFileIO

    Args:
        location(str): A URI to a file location
        fs(AbstractFileSystem): An fsspec filesystem instance
    """

    def __init__(self, location: str, fs: AbstractFileSystem):
        self._fs = fs
        super().__init__(location=location)

    def __len__(self) -> int:
        """Returns the total length of the file, in bytes"""
        object_info = self._fs.info(self.location)
        if size := object_info.get("Size"):
            return size
        elif size := object_info.get("size"):
            return size
        raise RuntimeError(f"Cannot retrieve object info: {self.location}")

    def exists(self) -> bool:
        """Checks whether the location exists"""
        return self._fs.lexists(self.location)

    def create(self, overwrite: bool = False):
        """Create an output stream for reading the contents of the file

        Args:
            overwrite(bool): Whether to overwrite the file if it already exists

        Returns:
            OpenFile: An fsspec compliant file-like object

        Raises:
            FileExistsError: If the file already exists at the location and overwrite is set to False

        Note:
            If overwrite is set to False, a check is first performed to verify that the file does not exist.
            This is not thread-safe and a possibility does exist that the file can be created by a concurrent
            process after the existence check yet before the output stream is created. In such a case, the default
            behavior will truncate the contents of the existing file when opening the output stream.
        """
        if not overwrite and self.exists():
            raise FileExistsError(f"Cannot create file, file already exists: {self.location}")
        return self._fs.open(self.location, "wb")

    def to_input_file(self) -> FsspecInputFile:
        """Returns a new FsspecInputFile for the location at `self.location`"""
        return FsspecInputFile(location=self.location, fs=self._fs)


class FsspecFileIO(FileIO):
    """A FileIO implementation that uses fsspec"""

    def __init__(self, properties: Properties):
        self.get_fs: Callable = lru_cache(self._get_fs)
        super().__init__(properties=properties)

    def new_input(self, location: str) -> FsspecInputFile:
        """Get an FsspecInputFile instance to read bytes from the file at the given location

        Args:
            location(str): A URI or a path to a local file

        Returns:
            FsspecInputFile: An FsspecInputFile instance for the given location
        """
        uri = urlparse(location)
        fs = self.get_fs(uri.scheme)
        return FsspecInputFile(location=location, fs=fs)

    def new_output(self, location: str) -> FsspecOutputFile:
        """Get an FsspecOutputFile instance to write bytes to the file at the given location

        Args:
            location(str): A URI or a path to a local file

        Returns:
            FsspecOutputFile: An FsspecOutputFile instance for the given location
        """
        uri = urlparse(location)
        fs = self.get_fs(uri.scheme)
        return FsspecOutputFile(location=location, fs=fs)

    def delete(self, location: Union[str, InputFile, OutputFile]) -> None:
        """Delete the file at the given location

        Args:
            location(str, InputFile, OutputFile): The URI to the file--if an InputFile instance or an
            OutputFile instance is provided, the location attribute for that instance is used as the location
            to delete
        """
        if isinstance(location, (InputFile, OutputFile)):
            str_location = location.location  # Use InputFile or OutputFile location
        else:
            str_location = location

        uri = urlparse(str_location)
        fs = self.get_fs(uri.scheme)
        fs.rm(str_location)

    def _get_fs(self, scheme: str) -> AbstractFileSystem:
        """Get a filesystem for a specific scheme"""
        if scheme not in SCHEME_TO_FS:
            raise ValueError(f"No registered filesystem for scheme: {scheme}")
        return SCHEME_TO_FS[scheme](self.properties)
