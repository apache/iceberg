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
import importlib
import logging
from abc import ABC, abstractmethod
from io import SEEK_SET
from typing import (
    Dict,
    List,
    Optional,
    Protocol,
    Union,
    runtime_checkable,
)

from pyiceberg.typedef import EMPTY_DICT, Properties

logger = logging.getLogger(__name__)


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

    @abstractmethod
    def __enter__(self):
        ...

    @abstractmethod
    def __exit__(self, exc_type, exc_val, exc_tb):
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

    @abstractmethod
    def __enter__(self):
        ...

    @abstractmethod
    def __exit__(self, exc_type, exc_val, exc_tb):
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

    properties: Properties

    def __init__(self, properties: Properties = EMPTY_DICT):
        self.properties = properties

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
    def delete(self, location: Union[str, InputFile, OutputFile]) -> None:
        """Delete the file at the given path

        Args:
            location(str, InputFile, OutputFile): A URI or a path to a local file--if an InputFile instance or
            an OutputFile instance is provided, the location attribute for that instance is used as the URI to delete

        Raises:
            PermissionError: If the file at location cannot be accessed due to a permission error
            FileNotFoundError: When the file at the provided location does not exist
        """


ARROW_FILE_IO = "pyiceberg.io.pyarrow.PyArrowFileIO"

# Mappings from the Java FileIO impl to a Python one. The list is ordered by preference.
# If a implementation isn't installed, it will fall back to the next one.
JAVA_FILE_IO_MAPPINGS: Dict[str, List[str]] = {
    "org.apache.iceberg.dell.ecs.EcsFileIO": [ARROW_FILE_IO],
    "org.apache.iceberg.gcp.gcs.GCSFileIO": [ARROW_FILE_IO],
    "org.apache.iceberg.hadoop.HadoopFileIO": [ARROW_FILE_IO],
    "org.apache.iceberg.aliyun.oss.OSSFileIO": [ARROW_FILE_IO],
    "org.apache.iceberg.io.ResolvingFileIO": [ARROW_FILE_IO],
    "org.apache.iceberg.aws.s3.S3FileIO": [ARROW_FILE_IO],
}


def _import_file_io(io_impl: str, properties: Properties) -> Optional[FileIO]:
    try:
        path_parts = io_impl.split(".")
        if len(path_parts) < 2:
            raise ValueError(f"py-io-impl should be full path (module.CustomFileIO), got: {io_impl}")
        module_name, class_name = ".".join(path_parts[:-1]), path_parts[-1]
        module = importlib.import_module(module_name)
        class_ = getattr(module, class_name)
        return class_(properties)
    except ImportError:
        logger.exception("Could not initialize FileIO: %s", io_impl)
        return None


PY_IO_IMPL = "py-io-impl"
IO_IMPL = "io-impl"


def load_file_io(properties: Properties) -> FileIO:
    if io_impl := properties.get(PY_IO_IMPL):
        if file_io := _import_file_io(io_impl, properties):
            return file_io
        else:
            raise ValueError(f"Could not initialize FileIO: {io_impl}")
    if java_io_impl := properties.get(IO_IMPL):
        if mapped_impls := JAVA_FILE_IO_MAPPINGS.get(java_io_impl):
            for impl in mapped_impls:
                if file_io := _import_file_io(impl, properties):
                    return file_io
        else:
            logger.warning("Could not convert Java mapping to Python: %s", java_io_impl)

    # Default to PyArrow
    from pyiceberg.io.pyarrow import PyArrowFileIO

    return PyArrowFileIO(properties)
