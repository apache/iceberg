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


"""
Contains Codecs for Python Avro.

Note that the word "codecs" means "compression/decompression algorithms" in the
Avro world (https://avro.apache.org/docs/current/spec.html#Object+Container+Files),
so don't confuse it with the Python's "codecs", which is a package mainly for
converting character sets (https://docs.python.org/3/library/codecs.html).
"""

import binascii
import io
import logging
import struct
import zlib
from abc import ABC, abstractmethod
from typing import Dict, Tuple, Type

from iceberg.avro.decoder import BinaryDecoder
from iceberg.io.memory import MemoryInputStream

logger = logging.getLogger(__name__)

STRUCT_CRC32 = struct.Struct(">I")  # big-endian unsigned int

has_bzip2 = False
try:
    import bz2

    has_bzip2 = True
except ImportError:
    logger.info("bzip2 compression not available, install python with bz2 headers available")
except Exception as e:
    logger.exception(f"snappy compression not available; {e}")

has_snappy = False
try:
    import snappy

    has_snappy = True
except ImportError:
    logger.info("snappy compression not available, please install with python-snappy")
except Exception as e:
    logger.exception(f"snappy compression not available; {e}")

has_zstandard = False
try:
    import zstandard as zstd

    has_zstandard = True
except ImportError:
    logger.info("zstandard compression not available, please install with zstandard")
except Exception as e:
    logger.exception(f"zstandard compression not available; {e}")


def _check_crc32(bytes_: bytes, checksum: bytes) -> None:
    """Incrementally compute CRC-32 from bytes and compare to a checksum

    Args:
      bytes_ (bytes): The bytes to check against `checksum`
      checksum (bytes): Byte representation of a checksum

    Raises:
      ValueError: If the computed CRC-32 does not match the checksum
    """
    if binascii.crc32(bytes_) & 0xFFFFFFFF != STRUCT_CRC32.unpack(checksum)[0]:
        raise ValueError("Checksum failure")


class Codec(ABC):
    """Abstract base class for all Avro codec classes."""

    @staticmethod
    @abstractmethod
    def compress(data: bytes) -> Tuple[bytes, int]:
        ...

    @staticmethod
    @abstractmethod
    def decompress(readers_decoder: BinaryDecoder) -> BinaryDecoder:
        ...


class NullCodec(Codec):
    @staticmethod
    def compress(data: bytes) -> Tuple[bytes, int]:
        return data, len(data)

    @staticmethod
    def decompress(readers_decoder: BinaryDecoder) -> BinaryDecoder:
        _ = readers_decoder.read_long()
        return readers_decoder


class DeflateCodec(Codec):
    @staticmethod
    def compress(data: bytes) -> Tuple[bytes, int]:
        # The first two characters and last character are zlib
        # wrappers around deflate data.
        compressed_data = zlib.compress(data)[2:-1]
        return compressed_data, len(compressed_data)

    @staticmethod
    def decompress(readers_decoder: BinaryDecoder) -> BinaryDecoder:
        # Compressed data is stored as (length, data), which
        # corresponds to how the "bytes" type is encoded.
        data = readers_decoder.read_bytes()
        # -15 is the log of the window size; negative indicates
        # "raw" (no zlib headers) decompression.  See zlib.h.
        uncompressed = zlib.decompress(data, -15)
        return BinaryDecoder(MemoryInputStream(uncompressed))


if has_bzip2:

    class BZip2Codec(Codec):
        @staticmethod
        def compress(data: bytes) -> Tuple[bytes, int]:
            compressed_data = bz2.compress(data)
            return compressed_data, len(compressed_data)

        @staticmethod
        def decompress(readers_decoder: BinaryDecoder) -> BinaryDecoder:
            length = readers_decoder.read_long()
            data = readers_decoder.read(length)
            uncompressed = bz2.decompress(data)
            return BinaryDecoder(MemoryInputStream(uncompressed))


if has_snappy:

    class SnappyCodec(Codec):
        @staticmethod
        def compress(data: bytes) -> Tuple[bytes, int]:
            compressed_data = snappy.compress(data)
            # A 4-byte, big-endian CRC32 checksum
            compressed_data += STRUCT_CRC32.pack(binascii.crc32(data) & 0xFFFFFFFF)
            return compressed_data, len(compressed_data)

        @staticmethod
        def decompress(readers_decoder: BinaryDecoder) -> BinaryDecoder:
            # Compressed data includes a 4-byte CRC32 checksum
            length = readers_decoder.read_long()
            data = readers_decoder.read(length - 4)
            uncompressed = snappy.decompress(data)
            checksum = readers_decoder.read(4)
            _check_crc32(uncompressed, checksum)
            return BinaryDecoder(MemoryInputStream(uncompressed))


if has_zstandard:

    class ZstandardCodec(Codec):
        @staticmethod
        def compress(data: bytes) -> Tuple[bytes, int]:
            compressed_data = zstd.ZstdCompressor().compress(data)
            return compressed_data, len(compressed_data)

        @staticmethod
        def decompress(readers_decoder: BinaryDecoder) -> BinaryDecoder:
            length = readers_decoder.read_long()
            data = readers_decoder.read(length)
            uncompressed = bytearray()
            dctx = zstd.ZstdDecompressor()
            with dctx.stream_reader(io.BytesIO(data)) as reader:
                while True:
                    chunk = reader.read(16384)
                    if not chunk:
                        break
                    uncompressed.extend(chunk)
            return BinaryDecoder(MemoryInputStream(uncompressed))


KNOWN_CODECS: Dict[str, Type[Codec]] = {
    name[:-5].lower(): class_
    for name, class_ in globals().items()
    if class_ != Codec and name.endswith("Codec") and isinstance(class_, type) and issubclass(class_, Codec)
}
