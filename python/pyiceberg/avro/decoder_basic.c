static inline void decode_ints(const char *buffer, unsigned int count, unsigned long *result, unsigned int *consumed_bytes) {
  const char *start = buffer;
  unsigned int shift;
  unsigned int i;

  for (i = 0; i < count; i++) {
    shift = 7;
    *result = *buffer & 0x7F;
    while(*buffer & 0x80) {
        buffer += 1;
        *result |= (unsigned long)(*buffer & 0x7F) << shift;
        shift += 7;
    }
    *result = (*result >> 1) ^ -(*result & 1);
    result += 1;
    buffer += 1;
  }
  *consumed_bytes = buffer - start;
}
