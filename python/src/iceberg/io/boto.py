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
"""FileIO implementation for reading and writing table files that uses boto3"""

from io import SEEK_CUR, SEEK_END, SEEK_SET
from typing import Optional, Union
from urllib.parse import urlparse

from boto3 import Session
from botocore.exceptions import ClientError

from iceberg.io.base import FileIO, InputFile, InputStream, OutputFile, OutputStream


class BotoInputStream(InputStream):
    """A seekable wrapper for reading an S3 Object that abides by the InputStream protocol

    Args:
        s3_object(boto3.resources.factory.s3.Object): An s3 object
    """

    def __init__(self, s3_object):
        self._s3_object = s3_object
        self._position = 0

    def read(self, n: int = -1) -> bytes:
        """Read the byte content of the s3 object

        This uses the `Range` argument when reading the S3 Object that allows setting a range of bytes to the headers of the request to S3.

        Args:
            n (int, optional): The number of bytes to read. Defaults to -1 which reads the entire file.

        Returns:
            bytes: The byte content of the file
        """
        if n < 0:  # Read the entire file from the current position
            range_header = f"bytes={self._position}-"
            self.seek(offset=0, whence=2)
        else:
            position_new = self._position + n

            if (
                position_new >= self._s3_object.content_length
            ):  # If more bytes are requested than exists, just read the entire file from the current position
                return self.read(n=-1)

            range_header = f"bytes={self._position}-{position_new -1}"
            self.seek(offset=n, whence=SEEK_CUR)

        return self._s3_object.get(Range=range_header)["Body"].read()

    def seek(self, offset: int, whence: int = 0) -> int:
        if whence not in {SEEK_SET, SEEK_CUR, SEEK_END}:
            raise ValueError(f"Cannot seek to position {offset}, invalid whence: {whence}")

        if whence == SEEK_SET:
            self._position = offset
        elif whence == SEEK_CUR:
            self._position = self._position + offset
        elif whence == SEEK_END:
            self._position = self._s3_object.content_length - offset

        return self._position

    def tell(self) -> int:
        return self._position

    def closed(self) -> bool:
        return False

    def close(self) -> None:
        pass


class BotoOutputStream(OutputStream):
    """A wrapper for writing an S3 Object that abides by the OutputStream protocol

    Args:
    s3_object(boto3.resources.factory.s3.Object): An s3 object
    """

    def __init__(self, s3_object):
        self._s3_object = s3_object

    def write(self, b: bytes) -> None:
        """Write to the S3 Object

        Args:
            b(bytes): The bytes to write to the S3 Object
        """
        self._s3_object.put(Body=b)

    def closed(self) -> bool:
        """Returns where the stream is closed or not

        Since this is a wrapper for requests to S3, there is no concept of closing, therefore this always returns False
        """
        return False

    def close(self) -> None:
        """Closes the stream

        Since this is a wrapper for requests to S3, there is no concept of closing, therefore this method does nothing
        """


class BotoInputFile(InputFile):
    """An input file implementation for the BotoFileIO

    Args:
        location(str): An S3 URI

    Attributes:
        location(str): An S3 URI
    """

    def __init__(self, location: str, session: Session, **client_kwargs):
        parsed_location = urlparse(location)  # Create a ParseResult from the uri

        if not parsed_location.scheme.startswith("s3"):
            raise ValueError(f"Cannot create BotoInputFile, scheme not supported: {parsed_location.scheme}")

        self._bucket = parsed_location.netloc
        self._path = parsed_location.path.strip("/")
        self._session = session
        self._client_kwargs = client_kwargs
        super().__init__(location=location)

    def __len__(self) -> int:
        """Returns the total length of the file, in bytes"""
        return self._session.resource("s3", **self._client_kwargs).Bucket(self._bucket).Object(self._path).content_length

    def exists(self) -> bool:
        """Checks whether the location exists"""
        try:
            self._session.resource("s3", **self._client_kwargs).Bucket(self._bucket).Object(
                self._path
            ).load()  # raises botocore.exceptions.ClientError with a 404 if it does not exist
            return True
        except ClientError as e:
            if e.response["Error"]["Code"] == "404":
                return False
            else:
                raise

    def open(self) -> BotoInputStream:
        """Create a BotoInputStream for reading the contents of the file

        Returns:
            BotoInputStream: A BotoInputStream instance for the file located at `self.location`
        """
        return BotoInputStream(
            s3_object=self._session.resource("s3", **self._client_kwargs).Bucket(self._bucket).Object(self._path)
        )


class BotoOutputFile(OutputFile):
    """An output file implementation for the BotoFileIO

    Args:
        location(str): An S3 URI

    Attributes:
        location(str): An S3 URI
    """

    def __init__(self, location: str, session: Session, **client_kwargs):
        parsed_location = urlparse(location)  # Create a ParseResult from the uri

        if not parsed_location.scheme.startswith("s3"):
            raise ValueError(f"Cannot create BotoOutputFile, scheme not supported: {parsed_location.scheme}")

        self._bucket = parsed_location.netloc
        self._path = parsed_location.path.strip("/")
        self._session = session
        self._client_kwargs = client_kwargs
        super().__init__(location=location)

    def __len__(self) -> int:
        """Returns the total length of the file, in bytes"""
        return self._session.resource("s3", **self._client_kwargs).Bucket(self._bucket).Object(self._path).content_length

    def exists(self) -> bool:
        """Checks whether the location exists"""
        try:
            self._session.resource("s3", **self._client_kwargs).Bucket(self._bucket).Object(
                self._path
            ).load()  # raises botocore.exceptions.ClientError with a 404 if it does not exist
            return True
        except ClientError as e:
            if e.response["Error"]["Code"] == "404":
                return False
            else:
                raise

    def create(self, overwrite: bool = False) -> OutputStream:
        """Create a BotoOutputStream for reading the contents of the file

        Args:
            overwrite(bool): Whether to overwrite the file if it already exists

        Returns:
            BotoOutputStream: A NativeFile instance for the file located at self.location

        Raises:
            FileExistsError: If the file already exists at `self.location` and `overwrite` is False

        Note:
            If overwrite is set to False, a check is first performed to verify that the file does not exist.
            This is not thread-safe and a possibility does exist that the file can be created by a concurrent
            process after the existence check yet before the output stream is created. In such a case, the default
            behavior will truncate the contents of the existing file when opening the output stream.
        """
        return BotoOutputStream(
            s3_object=self._session.resource("s3", **self._client_kwargs).Bucket(self._bucket).Object(self._path)
        )

    def to_input_file(self) -> BotoInputFile:
        """Returns a new BotoInputFile for the location"""
        return BotoInputFile(location=self.location, session=self._session, **self._client_kwargs)


class BotoFileIO(FileIO):
    """A FileIO implementation that uses boto3"""

    def __init__(self, session: Optional[Session] = None, **client_kwargs):
        self._session = session or Session()
        self._client_kwargs = client_kwargs

    def new_input(self, location: str) -> BotoInputFile:
        """Get a BotoInputFile instance to read bytes from the file at the given location

        Args:
            location(str): A URI or a path to a local file

        Returns:
            BotoInputFile: A BotoInputFile instance for the given location
        """
        return BotoInputFile(location, session=self._session, **self._client_kwargs)

    def new_output(self, location: str) -> BotoOutputFile:
        """Get a BotoOutputFile instance to write bytes to the file at the given location

        Args:
            location(str): A URI or a path to a local file

        Returns:
            BotoOutputFile: A BotoOutputFile instance for the given location
        """
        return BotoOutputFile(location, session=self._session, **self._client_kwargs)

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
        # Create a ParseResult from the uri
        if isinstance(location, (InputFile, OutputFile)):
            parsed_location = urlparse(location.location)  # Use InputFile or OutputFile location
        else:
            parsed_location = urlparse(location)

        if not parsed_location.scheme.startswith("s3"):
            raise ValueError(f"Cannot delete location, scheme not supported: {parsed_location.scheme}")

        bucket = parsed_location.netloc
        path = parsed_location.path.strip("/")
        self._session.resource("s3", **self._client_kwargs).Bucket(bucket).Object(path).delete()
