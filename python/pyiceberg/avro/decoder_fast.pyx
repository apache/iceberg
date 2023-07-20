cdef extern from "decoder_basic.c":
  void decode_int(const char *buffer, unsigned long *result, unsigned int *consumed_bytes);

def read_int(const char *buffer, unsigned int offset):
  cdef unsigned long result;
  cdef unsigned int consumed_bytes;
  decode_int(buffer+offset, &result, &consumed_bytes)
  return (result, consumed_bytes)

def read_ints(const char *buffer, unsigned int offset):
  cdef unsigned long result[2];
  cdef unsigned int consumed_bytes[2];
  cdef const char *b = buffer + offset;


  decode_int(b, &result[0], &consumed_bytes[0])
  b += consumed_bytes[0]
  decode_int(b, &result[1], &consumed_bytes[1])

  return (result, consumed_bytes[0]+consumed_bytes[1])
