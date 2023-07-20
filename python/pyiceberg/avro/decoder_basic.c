static inline void decode_int(const char *buffer, unsigned long *result, unsigned int *consumed_bytes) {
  const char *start = buffer;
  unsigned int shift = 7;
  register unsigned long n = *buffer & 0x7F;
  while(*buffer & 0x80) {
      buffer += 1;
      n |= (unsigned long)(*buffer & 0x7F) << shift;
      shift += 7;
  }
  *result = (n >> 1) ^ -(n & 1);
  *consumed_bytes = buffer - start + 1;
}
