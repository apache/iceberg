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
"""FileIO implementation for reading and writing table files that uses s3fs"""

from typing import Optional, Union

from s3fs import S3FileSystem

from pyiceberg.io.base import (
    FileIO,
    InputFile,
    InputStream,
    OutputFile,
    OutputStream,
)


class S3fsInputStream(InputStream):
    """A seekable wrapper for reading an S3 Object that abides by the InputStream protocol

    Args:
        s3_object(s3fs.core.S3File): An s3 object
    """

    def __init__(self, s3_object):
        self._s3_object = s3_object

    def read(self, size: int = -1) -> bytes:
        """Read the byte content of the s3 object

        Args:
            size (int, optional): The number of bytes to read. Defaults to -1 which reads the entire file.

        Returns:
            bytes: The byte content of the file
        """
        return self._s3_object.read(length=size)

    def seek(self, offset: int, whence: int = 0) -> int:
        return self._s3_object.seek(loc=offset, whence=whence)

    def tell(self) -> int:
        return self._s3_object.tell()

    def closed(self) -> bool:
        return self._s3_object.closed

    def close(self) -> None:
        self._s3_object.close()


class S3fsOutputStream(OutputStream):
    """A wrapper for writing an S3 Object that abides by the OutputStream protocol

    Args:
        s3_object(s3fs.core.S3FileSystem): An s3 object
    """

    def __init__(self, s3_object):
        self._s3_object = s3_object

    def write(self, b: bytes) -> int:
        """Write to the S3 Object

        Args:
            b(bytes): The bytes to write to the S3 Object

        Returns:
            int: The number of bytes written

        Raises:
            ValueError: When the file is closed
        """
        return self._s3_object.write(b)

    def closed(self) -> bool:
        """Returns whether the stream is closed or not"""
        return self._s3_object.closed

    def close(self) -> None:
        """Closes the stream and uploads the bytes to S3"""
        self._s3_object.close()


class S3fsInputFile(InputFile):
    """An input file implementation for the S3fsFileIO

    Args:
        location(str): An S3 URI

    Attributes:
        location(str): An S3 URI
    """

    def __init__(self, location: str, s3):
        self._s3 = s3
        super().__init__(location=location)

    def __len__(self) -> int:
        """Returns the total length of the file, in bytes"""
        object_info = self._s3.info(self.location)
        if size := object_info.get("Size"):  # s3fs versions seem inconsistent on the case used for size
            return object_info["Size"]
        elif object_info.get("size"):
            return object_info["size"]
        raise RuntimeError(f"Cannot retrieve object info: {self.location}")

    def exists(self) -> bool:
        """Checks whether the location exists"""
        return self._s3.lexists(self.location)

    def open(self) -> S3fsInputStream:
        """Create an S3fsInputStream for reading the contents of the file

        Returns:
            S3fsInputStream: An S3fsInputStream instance for the file located at `self.location`
        """
        return S3fsInputStream(s3_object=self._s3.open(self.location, "rb"))


class S3fsOutputFile(OutputFile):
    """An output file implementation for the S3fsFileIO

    Args:
        location(str): An S3 URI

    Attributes:
        location(str): An S3 URI
    """

    def __init__(self, location: str, s3):
        self._s3 = s3
        super().__init__(location=location)

    def __len__(self) -> int:
        """Returns the total length of the file, in bytes"""
        object_info = self._s3.info(self.location)
        if object_info.get("Size"):  # s3fs versions seem inconsistent on the case used for size
            return object_info["Size"]
        elif object_info.get("size"):
            return object_info["size"]
        raise RuntimeError(f"Cannot retrieve object info: {self.location}")

    def exists(self) -> bool:
        """Checks whether the location exists"""
        return self._s3.lexists(self.location)

    def create(self, overwrite: bool = False) -> S3fsOutputStream:
        """Create an S3fsOutputStream for reading the contents of the file

        Args:
            overwrite(bool): Whether to overwrite the file if it already exists

        Returns:
            S3fsOutputStream: A wrapper for writing to an s3 object located at self.location

        Raises:
            FileExistsError: If the file already exists at `self.location` and `overwrite` is False

        Note:
            If overwrite is set to False, a check is first performed to verify that the file does not exist.
            This is not thread-safe and a possibility does exist that the file can be created by a concurrent
            process after the existence check yet before the output stream is created. In such a case, the default
            behavior will truncate the contents of the existing file when opening the output stream.
        """
        if not overwrite and self.exists():
            raise FileExistsError(f"Cannot create file, file already exists: {self.location}")
        self._s3.touch(self.location)
        return S3fsOutputStream(s3_object=self._s3.open(self.location, "wb"))

    def to_input_file(self) -> S3fsInputFile:
        """Returns a new S3fsInputFile for the location at `self.location`"""
        return S3fsInputFile(location=self.location, s3=self._s3)


class S3fsFileIO(FileIO):
    """A FileIO implementation that uses s3fs"""

    def __init__(self, client_kwargs: Optional[dict] = None, **s3fs_filesystem_kwargs):
        if client_kwargs:
            self._s3_filesystem = S3FileSystem(client_kwargs=client_kwargs, **s3fs_filesystem_kwargs)
        else:
            self._s3_filesystem = S3FileSystem(**s3fs_filesystem_kwargs)

    def new_input(self, location: str) -> S3fsInputFile:
        """Get an S3fsInputFile instance to read bytes from the file at the given location

        Args:
            location(str): A URI or a path to a local file

        Returns:
            S3fsInputFile: An S3fsInputFile instance for the given location
        """
        return S3fsInputFile(location=location, s3=self._s3_filesystem)

    def new_output(self, location: str) -> S3fsOutputFile:
        """Get an S3fsOutputFile instance to write bytes to the file at the given location

        Args:
            location(str): A URI or a path to a local file

        Returns:
            S3fsOutputFile: An S3fsOutputFile instance for the given location
        """
        return S3fsOutputFile(location=location, s3=self._s3_filesystem)

    def delete(self, location: Union[str, InputFile, OutputFile]) -> None:
        """Delete the file at the given location

        Args:
            location(str, InputFile, OutputFile): The URI to the file--if an InputFile instance or an
            OutputFile instance is provided, the location attribute for that instance is used as the location
            to delete
        """
        # Create a ParseResult from the uri
        if isinstance(location, (InputFile, OutputFile)):
            str_location = location.location  # Use InputFile or OutputFile location
        else:
            str_location = location

        self._s3_filesystem.rm(str_location)
