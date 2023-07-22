import cython
from cython.cimports.cpython import array

import array

cdef extern from "decoder_basic.c":
  void decode_ints(const char *buffer, unsigned int count, unsigned long *result, unsigned int *consumed_bytes);


def read_int(const char *buffer, unsigned int offset) -> Tuple[int, int]:
  cdef unsigned long result;
  cdef unsigned int consumed_bytes;
  decode_ints(buffer+offset, 1, <unsigned long *>&result, &consumed_bytes)
  return (result, consumed_bytes)

unsigned_long_array_template = cython.declare(array.array, array.array('L', []))

def read_ints(const char *buffer, unsigned int offset, unsigned int count) -> Tuple[Tuple[int, ...], int]:
  cdef unsigned int consumed_bytes;
  cdef const char *b = buffer + offset;
  newarray = array.clone(unsigned_long_array_template, count, zero=False)
  decode_ints(b, count, newarray.data.as_ulongs, &consumed_bytes)
  return (newarray, consumed_bytes)

def read_int_bytes_dict(const char *buffer, unsigned int offset, unsigned int count, dict dest) -> int:
    """Reads a dictionary of integers for keys and bytes for values into a destination dict."""
    cdef unsigned long result[2];
    cdef unsigned int consumed_bytes;
    cdef const char *b = buffer + offset;
    cdef const char *start = b;
    for _ in range(count):
      decode_ints(b, 2, <unsigned long *>&result, &consumed_bytes)
      b += consumed_bytes
      if result[0] <= 0:
          dest[result[0]] = b""
      else:
          dest[result[0]] = b[0:result[1]]
          b += result[1]
    return b-start

