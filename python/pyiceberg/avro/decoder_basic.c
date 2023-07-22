/*
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing,
 software distributed under the License is distributed on an
 "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 KIND, either express or implied.  See the License for the
 specific language governing permissions and limitations
 under the License.
*/

/*
  Decode an an array of zig-zag encoded integers from a buffer.

  The buffer is advanced to the end of the integers.
  `count` is the number of integers to decode.
  `result` is where the decoded integers are stored.

*/
static inline void decode_ints_with_ptr(const char **buffer, unsigned int count, unsigned long *result) {
  unsigned int shift;
  unsigned int i;

  for (i = 0; i < count; i++) {
    shift = 7;
    *result = **buffer & 0x7F;
    while(**buffer & 0x80) {
        *buffer += 1;
        *result |= (unsigned long)(**buffer & 0x7F) << shift;
        shift += 7;
    }
    *result = (*result >> 1) ^ -(*result & 1);
    result += 1;
    *buffer += 1;
  }
}

/*
  Skip a zig-zag encoded integer in a buffer.

  The buffer is advanced to the end of the integer.
*/
static inline void skip_int(const char **buffer) {
  while(**buffer & 0x80) {
    *buffer += 1;
  }
  *buffer += 1;
}
