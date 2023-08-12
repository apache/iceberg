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
import cython
from cython.cimports.cpython import array
from datetime import datetime, time
from uuid import UUID
from pyiceberg.avro import STRUCT_DOUBLE, STRUCT_FLOAT
from pyiceberg.utils.datetime import micros_to_time, micros_to_timestamp, micros_to_timestamptz
from pyiceberg.utils.decimal import unscaled_to_decimal
from pyiceberg.io import InputStream
from cpython.mem cimport PyMem_Malloc, PyMem_Realloc, PyMem_Free
from libc.string cimport memcpy
import decimal

import array

cdef extern from "decoder_basic.c":
  void decode_longs(const unsigned char **buffer, unsigned int count, unsigned long *result);
  void skip_int(const unsigned char **buffer);

unsigned_long_array_template = cython.declare(array.array, array.array('L', []))

@cython.final
cdef class CythonBinaryDecoder:
    """Implement a BinaryDecoder that reads from an in-memory buffer.

    """

    # This the data that is duplicated when the decoder is created.
    cdef unsigned char *_data

    # This is the current pointer to the buffer.
    cdef const unsigned char *_current

    # This is the address after the data buffer
    cdef const unsigned char *_end

    # This is the size of the buffer of the data being parsed.
    cdef unsigned int _size

    def __cinit__(self, input_contents: bytes) -> None:
        self._size = len(input_contents)

        # Make a copy of the data so the data can be iterated.
        self._data = <unsigned char *> PyMem_Malloc(self._size * sizeof(char))
        if not self._data:
            raise MemoryError()
        cdef const unsigned char *input_as_array = input_contents
        memcpy(self._data, input_as_array, self._size)
        self._end = self._data + self._size
        self._current = self._data

    def __dealloc__(self):
        PyMem_Free(self._data)

    cpdef unsigned int tell(self):
        """Return the current stream position."""
        return self._current - self._data

    cpdef bytes read(self, n: int):
        """Read n bytes."""
        if n < 0:
            raise ValueError(f"Requested {n} bytes to read, expected positive integer.")
        cdef const unsigned char *r = self._current
        self._current += n
        return r[0:n]

    def read_boolean(self) -> bool:
        """Reads a value from the stream as a boolean.

        A boolean is written as a single byte
        whose value is either 0 (false) or 1 (true).
        """
        self._current += 1;
        return self._current[-1] != 0

    cpdef inline long read_int(self):
        """Reads a value from the stream as an integer.

        int/long values are written using variable-length, zigzag coding.
        """
        cdef unsigned long result;
        if self._current >= self._end:
          raise EOFError(f"EOF: read 1 bytes")
        decode_longs(&self._current, 1, <unsigned long *>&result)
        return result

    def read_ints(self, count: int) -> Tuple[int, ...]:
        """Reads a list of integers."""
        newarray = array.clone(unsigned_long_array_template, count, zero=False)
        if self._current >= self._end:
          raise EOFError(f"EOF: read 1 bytes")
        decode_longs(&self._current, count, newarray.data.as_ulongs)
        return newarray

    cpdef void read_int_bytes_dict(self, count: int, dest: Dict[int, bytes]):
        """Reads a dictionary of integers for keys and bytes for values into a destination dict."""
        cdef unsigned long result[2];
        if self._current >= self._end:
          raise EOFError(f"EOF: read 1 bytes")

        for _ in range(count):
          decode_longs(&self._current, 2, <unsigned long *>&result)
          if result[1] <= 0:
              dest[result[0]] = b""
          else:
              dest[result[0]] = self._current[0:result[1]]
              self._current += result[1]

    cpdef inline bytes read_bytes(self):
        """Bytes are encoded as a long followed by that many bytes of data."""
        cdef unsigned long length;
        if self._current >= self._end:
          raise EOFError(f"EOF: read 1 bytes")

        decode_longs(&self._current, 1, <unsigned long *>&length)

        if length <= 0:
            return b""
        cdef const unsigned char *r = self._current
        self._current += length
        return r[0:length]

    cpdef float read_float(self):
        """Reads a value from the stream as a float.

        A float is written as 4 bytes.
        The float is converted into a 32-bit integer using a method equivalent to
        Java's floatToIntBits and then encoded in little-endian format.
        """
        return float(STRUCT_FLOAT.unpack(self.read(4))[0])

    cpdef float read_double(self):
        """Reads a value from the stream as a double.

        A double is written as 8 bytes.
        The double is converted into a 64-bit integer using a method equivalent to
        Java's doubleToLongBits and then encoded in little-endian format.
        """
        return float(STRUCT_DOUBLE.unpack(self.read(8))[0])

    cpdef str read_utf8(self):
        """Reads a utf-8 encoded string from the stream.

        A string is encoded as a long followed by
        that many bytes of UTF-8 encoded character data.
        """
        return self.read_bytes().decode("utf-8")

    def skip_int(self) -> None:
        skip_int(&self._current)
        return

    def skip(self, n: int) -> None:
        self._current += n

    def skip_boolean(self) -> None:
        self._current += 1

    def skip_float(self) -> None:
        self._current += 4

    def skip_double(self) -> None:
        self._current += 8

    def skip_bytes(self) -> None:
        cdef long result;
        decode_longs(&self._current, 1, <unsigned long *>&result)
        self._current += result

    def skip_utf8(self) -> None:
        self.skip_bytes()
