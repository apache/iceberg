---
title: "Puffin Spec"
url: puffin-spec
toc: true
disableSidebar: true
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

# Puffin file format

This is a specification for Puffin, a file format designed to store
information such as indexes and statistics about data managed in an
Iceberg table that cannot be stored directly within the Iceberg manifest. A
Puffin file contains arbitrary pieces of information (here called "blobs"),
along with metadata necessary to interpret them. The blobs supported by Iceberg
are documented at [Blob types](#blob-types).

## Format specification

A file conforming to the Puffin file format specification should have the structure
as described below.

### Versions

Currently, there is a single version of the Puffin file format, described below.

### File structure

The Puffin file has the following structure

```
Magic Blob₁ Blob₂ ... Blobₙ Footer
```

where

- `Magic` is four bytes 0x50, 0x46, 0x41, 0x31 (short for: Puffin _Fratercula
  arctica_, version 1),
- `Blobᵢ` is i-th blob contained in the file, to be interpreted by application
  according to the footer,
- `Footer` is defined below.

### Footer structure

Footer has the following structure

```
Magic FooterPayload FooterPayloadSize Flags Magic
```

where

- `Magic`: four bytes, same as at the beginning of the file
- `FooterPayload`: optionally compressed, UTF-8 encoded JSON payload describing the
  blobs in the file, with the structure described below
- `FooterPayloadSize`: a length in bytes of the `FooterPayload` (after compression,
  if compressed), stored as 4 byte integer
- `Flags`: 4 bytes for boolean flags
  - byte 0 (first)
    - bit 0 (lowest bit): whether `FooterPayload` is compressed
    - all other bits are reserved for future use and should be set to 0 on write
  - all other bytes are reserved for future use and should be set to 0 on write

A 4 byte integer is always signed, in a two's complement representation, stored
little-endian.

### Footer Payload

Footer payload bytes is either uncompressed or LZ4-compressed (as a single
[LZ4 compression frame](https://github.com/lz4/lz4/blob/77d1b93f72628af7bbde0243b4bba9205c3138d9/doc/lz4_Frame_format.md)
with content size present), UTF-8 encoded JSON payload representing a single
`FileMetadata` object.

#### FileMetadata

`FileMetadata` has the following fields


| Field Name | Field Type                              | Required | Description |
| ---------- | --------------------------------------- | -------- | ----------- |
| blobs      | list of BlobMetadata objects            | yes      |
| properties | JSON object with string property values | no       | storage for arbitrary meta-information, like writer identification/version. See [Common properties](#common-properties) for properties that are recommended to be set by a writer.

#### BlobMetadata

`BlobMetadata` has the following fields

| Field Name        | Field Type                              | Required | Description |
|-------------------|-----------------------------------------|----------| ----------- |
| type              | JSON string                             | yes      | See [Blob types](#blob-types)
| fields            | JSON list of ints                       | yes      | List of field IDs the blob was computed for; the order of items is used to compute sketches stored in the blob.
| snapshot-id       | JSON long                               | yes      | ID of the Iceberg table's snapshot the blob was computed from.
| sequence-number   | JSON long                               | yes      | Sequence number of the Iceberg table's snapshot the blob was computed from.
| offset            | JSON long                               | yes      | The offset in the file where the blob contents start
| length            | JSON long                               | yes      | The length of the blob stored in the file (after compression, if compressed)
| compression-codec | JSON string                             | no       | See [Compression codecs](#compression-codecs). If omitted, the data is assumed to be uncompressed.
| properties        | JSON object with string property values | no       | storage for arbitrary meta-information about the blob

### Blob types

The blobs can be of a type listed below

#### `apache-datasketches-theta-v1` blob type

A serialized form of a "compact" Theta sketch produced by the [Apache
DataSketches](https://datasketches.apache.org/) library. The sketch is obtained by
constructing Alpha family sketch with default seed, and feeding it with individual
distinct values converted to bytes using Iceberg's single-value serialization.

The blob metadata for this blob may include following properties:

- `ndv`: estimate of number of distinct values, derived from the sketch.

### Compression codecs

The data can also be uncompressed. If it is compressed the codec should be one of
codecs listed below. For maximal interoperability, other codecs are not supported.

| Codec name | Description                                                                                                                                                                                     |
|------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| lz4        | Single [LZ4 compression frame](https://github.com/lz4/lz4/blob/77d1b93f72628af7bbde0243b4bba9205c3138d9/doc/lz4_Frame_format.md), with content size present                                     |
| zstd       | Single [Zstandard compression frame](https://github.com/facebook/zstd/blob/8af64f41161f6c2e0ba842006fe238c664a6a437/doc/zstd_compression_format.md#zstandard-frames), with content size present |
__

### Common properties

When writing a Puffin file it is recommended to set the following fields in the
[FileMetadata](#filemetadata)'s `properties` field.

- `created-by` - human-readable identification of the application writing the file,
  along with its version. Example "Trino version 381".
