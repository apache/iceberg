---
title: "Mumbling Bitmap Spec"
---
<!--
 - Licensed to the Apache Software Foundation (ASF) under one or more
 - contributor license agreements.  See the NOTICE file distributed with
 - this work for additional information regarding copyright ownership.
 - The ASF licenses this file to You under the Apache License, Version 2.0
 - (the "License"); you may not use this file except in compliance with
 - the License.  You may obtain a copy of the License at
 -
 -   http://www.apache.org/licenses/LICENSE-2.0
 -
 - Unless required by applicable law or agreed to in writing, software
 - distributed under the License is distributed on an "AS IS" BASIS,
 - WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 - See the License for the specific language governing permissions and
 - limitations under the License.
 -->

# Mumbling Bitmap Spec

This is a specification for the Mumbling compressed bitmap format designed for
use cases with bounded total size, like a deletion vector. This is based on
[Roaring Bitmap][roaring].

This spec is for version 1.

[roaring]: https://roaringbitmap.org/


## Overview

Mumbling bitmaps are based on the same idea as Roaring bitmaps: a bitmap is
divided into fixed-size (256 bit) regions, called _containers_. Each container
is either _sparse_ to store offsets (0-255) or _dense_ to store a bit set. The
main difference from Roaring is that Mumbling bitmaps use a smaller scale where
each container is at most 32 bytes, and are limited to at most 2,097,152
values.

Containers are tracked by an array of descriptor bytes, one per container. The
position of the descriptor byte in the array encodes the most significant bits
of the positions stored in its corresponding container (the _key_ in Roaring
bitmap). The descriptor encodes the container size for sparse containers (0-31
values), or that a container is dense (32). Because the format uses a
descriptor array instead of keys and offsets, descriptor bytes are stored
as a PFOR-encoded array.


## Design choices

Unlike Roaring, this format uses a descriptor array instead of storing a key,
cardinality, and offset for each container.

The Iceberg use case is for small, embedded deletion vectors. These vectors
will be small, likely less than 100,000 entries. But, limiting the
per-container overhead by using a single byte for the key (256 containers)
would be too limiting (only 65,536 total entries). As a result, this would
require 2-byte keys and 2-byte offsets (8192 containers * 32 bytes /
container).

The descriptor array avoids 4 bytes of overhead for each 32-byte container. The
key is implicit and is the offset into the descriptor array. Descriptors encode
the length of a container instead of its offset, requiring only one byte.

The first trade-off of this approach is that each descriptor must be present,
even for 0-length (empty) containers. And because an empty state is needed,
descriptors cannot encode the container cardinality (up to 256). PFOR encoding
is used to reduce this overhead.

The second trade-off is that offsets are not directly stored. However, offsets
can be computed from the relatively small descriptor array and finding the
descriptor for a key uses direct array indexing.

An alternative to the descriptor array is to store the offsets, and use the
difference between offsets to find container length. This approach was not
chosen because array encoding would be worse (values are increasing), and the
remaining descriptor bits cannot be used.


## Format

A Mumbling bitmap consists of 3 concatenated sections:

* Header
* Descriptor array
* Containers

Throughout the format, integers are unsigned and stored as little endian.


### Header

The Mumbling header is made up of the following fields:

| Field           | Size    | Description |
|-----------------|---------|-------------|
| Format version  | 1 byte  | `0x01` for version 1 |
| Cardinality     | 3 bytes | The number of set bits |
| Container count | 2 bytes | The number of containers in the bitmap |

Because the container count is limited to 8,192, cardinality is limited to
2,097,152 (8,192 containers of 256 bits).


### Descriptor array

The descriptor array contains one descriptor byte per container. Its length is
the container count.

The 3 most significant bits of the descriptor byte determine the container type
and how to interpret the remaining least significant bits of the descriptor.

| MSB pattern | Container type | Description                      | Remaining bits |
|-------------|----------------|----------------------------------|----------------|
| `000`       | Sparse         | A sparse container of 0-31 bytes | Container length / number of bits set |
| `001`       | Dense          | A dense container of 32 bytes    | Must be 0 |

In v1, the descriptor byte encodes the size of its corresponding container.
The two most significant bits are reserved for future use and implementations
must ignore the least-significant bits for a dense container if they are set.

Example descriptors:

| Hex  | Binary      | Interpretation |
|------|-------------|----------------|
| `00` | `0000 0000` | Sparse container of 0 values |
| `05` | `0000 0101` | Sparse container of 5 values |
| `1F` | `0001 1111` | Sparse container of 31 values |
| `20` | `0010 0000` | Dense container stored in 32 bytes |

The descriptor array is encoded using patched frame of reference (PFOR)
documented in [Appendix A](pfor). PFOR was chosen because it can efficiently
store mostly uniform container sizes along with occasional larger values. The
binary representation for descriptors also allows saving at least 2 bits per
value.

[pfor]: #appendix-a-pfor-encoding-for-unsigned-bytes


### Containers

The containers section consists of concatenated containers. Each container
stores a 256 bit region of the bitmap.

Positions in the bitmap are split into the first 16 bits that identify the
container and the last 8 bits that identify the corresponding position within
the container.

Containers may be sparse or dense. This type is encoded by the container's
corresponding descriptor byte. Containers with less than 32 bits set must be
sparse and containers with 32 or more bits set must be dense.


#### Sparse containers

A sparse container encodes up to 31 set positions. All other positions are
unset. Each set position is stored as an unsigned byte (0-255), relative to the
start of the container.

The list of positions must be sorted in ascending order. This allows checking
whether a specific position is set to stop when a higher position is reached or
the last position is reached.

A sparse container's length is not stored in the container. It is the value of
the corresponding descriptor byte.

Examples:

| Descriptor | Container bytes   | Set positions |
|------------|-------------------|---------------|
| 0          | (zero bytes)      | None |
| 3          | `00 22 FF`        | 0, 34, 255 |
| 31         | `00 01 02 ... 1E` | 0, 1, 2, ..., 30 |


### Dense containers

A dense container encodes each bit of the container as 0 (unset) or 1 (set) in
a 32-byte array.

The first byte in the byte array contains bits 0-7, the next byte contains
8-15, etc. Bit positions are ordered from most significant to least. As a
result, the 0th position in the container is the most significant bit of the
first byte, and the 255th position in the container is the least-significant
bit of the last byte.

Examples:

| Descriptor | Container bytes         | Set positions |
|------------|-------------------------|---------------|
| 32         | `FF FF FF FF 00 ... 00` | 0-31 |
| 32         | `FF FF FF FF 80 ... 00` | 0-32 |
| 32         | `FF FF 00 ... 00 FF FF` | 0-15, 240-255 |
| 32         | `AA AA ... AA AA`       | Even positions: 0, 2, 4, ... |


## Working with bitmaps

Mumbling bitmaps use a descriptor array to track containers. For quick lookups,
implementations should decode the descriptor array and use it to produce an
offset array.

The container for a position is accessed by finding its type and offset using
the descriptor array. The index into the descriptor array is the position
divided by 256:

```
let container_index: u16 = (pos >> 8) as u16
```

The offset of the container is the sum of the lengths of previous containers,
as determined by container descriptors.

```
let descriptor: u8 = descriptors[container_index]
let offset: usize = offsets[container_index]
```

The corresponding position within a container is the least significant 8 bits
of the bitmap position:

```
let pos_in_container: u8 = (pos & 0xFF) as u8
```


# Appendix A: PFOR encoding for unsigned bytes

The unsigned byte PFOR encoding splits the value array into 256-value chunks.
The length of each chunk is 256 values until the last chunk, which is the
remainder. Length is not encoded in each chunk.

Chunks are encoded separately using a PFOR scheme for single-byte values.
First, the chunk's min value is subtracted from every value in the chunk to
normalize values.  Second, a bit width, `b1`, is chosen so that most values in
the chunk can be stored in `b1` bits. Next, the least-significant `b1` bits of
each value are packed into the _primary_ array.  Finally, the positions of
_exception_ values that do not fit in `b1` bits are tracked in an offset array,
and the remaining bits of the exceptions are packed into an exception array.


## PFOR encoding

Each chunk is stored using the following concatenated sections:

* Header (3 bytes)
* Primary value array
* Exception offsets
* Exception value array


### Header

The header stores information about the chunk encoding:

* `b1`: The bit width of values in the primary value array
* `b2`: The bit width of values in the exception array; at most `8 - b1`
* `e`: The number of exception values that do not fit in `b1` bits
* `m`: A constant that has been subtracted from each value, usually the min

The header layout packs `b1` and `b2` in one byte, followed by `e` and `m`.

| Byte | Bits | Field |
|------|------|-------|
| 0    | 0-3  | `b1` |
| 0    | 4-7  | `b2` |
| 1    | 0-7  | `e` |
| 2    | 0-7  | `m` |


### Encoding

To encode a chunk of values:

1. Find the minimum value, `m`
2. Subtract `m` from each value in the chunk
3. Choose `b1` and `b2` (see below)
4. Pack the least-significant `b1` bits of each value into `32 * b1` bytes
5. Collect exception values that do not fit into `b1` bits, and their offsets
6. Write the exception offset array (1 byte per exception)
7. For each exception, pack the remaining `b2` bits into `ceil(e*b2/8)` bytes

The recommended way to choose bit widths `b1` and `b2` is:

1. For each value, find the number of bits required to store it
2. Count the values requiring each bit width, 0-8, and find the largest width
3. For each bit width, `b`, calculate the total size for that width:
    * Let `e` be the number of exceptions, the sum of counts for larger widths
    * Let `b2` be the exception bit width, the largest width minus `b`
    * The total size is `32*b + e + ceil(e*b2/8)`
4. Choose a bit width `b1` that minimizes the total size

Values are packed using the most significant bits for the first value. If a bit
packed array is not full, each packed section is padded with 0s to the next
byte.

For example, for bit width `b = 2`, the array [ 3, 2, 1, 2, 3 ] is stored as
binary `1110 0110 1100 0000` or hex `E6 C0`.

When `b1` is 8, values are each stored in a byte and there are no exceptions.
In this case, `e` must be 0, `b2` must be 0, and it is recommended that
implementations store the original values (`m` is 0).


### Examples

| Length | Encoded byte array hex | Decoded values             | Description |
|--------|------------------------|----------------------------|-------------|
| 256    | `00 00 00`             | 256 values, all = 0        | 0 bits per value, `m` = 0, no exceptions |
| 51     | `00 00 05`             | 51 values, all = 5         | 0 bits per value, `m` = 5, no exceptions |
| 8      | `80 02 00 04 07 FF FE` | [0, 0, 0, 0, FF, 0, 0, FE] | 0 bits per value, `m` = 0, 2 exceptions, 8 bits per exception |
| 3      | `02 00 06 18`          | [6, 7, 8]                  | 2 bits per value, `m` = 6, no exceptions |
| 4      | `32 01 06 09 01 E0`    | [6, 34, 8, 7]              | 2 bits per value, `m` = 6, 1 exception, 3 bits per exception |


