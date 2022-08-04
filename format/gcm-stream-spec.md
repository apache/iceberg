---
title: "AES GCM Stream Spec"
url: gcm-stream-spec
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

# AES GCM Stream (AGS) file format extension

## Background and Motivation

Iceberg supports a number of data file formats. Two of these formats (Parquet and ORC) have built-in encryption capabilities, that allow to protect sensitive information in the data files. However, besides the data files, Iceberg tables also have metadata files, that keep sensitive information too (e.g., min/max values in manifest files, or bloom filter bitsets in puffin files). Metadata file formats (AVRO, JSON, Puffin) don't have encryption support.

Moreover, with the exception of Parquet, no Iceberg data or metadata file format supports integrity verification, required for end-to-end tamper proofing of Iceberg tables.

This document specifies details of a simple file format extension that adds encryption and tamper-proofing to any existing file format.

## Goals

* Metadata encryption: enable encryption of manifests, manifest lists, snapshots and stats.
* Avro data encryption: enable encryption of data files in tables that use the Avro format.
* Tamper proofing of Iceberg data and metadata files.

## Technical approach

AGS leverages the Iceberg EncryptionManager interface that converts OutputFile objects (produced by Iceberg Avro and other writers) into EncryptedOutputFile objects. The PositionOutputStream, produced by the AGS EncryptedOutputFile, splits the output bytes into equal-size blocks (plus residue), and encrypts/signs the blocks with a given encryption key. 
On the reader side, the Iceberg EncryptionManager converts EncryptedInputFile objects into InputFile objects consumable by Iceberg Avro and other readers. The AGS SeekableInputStream decrypts the stream blocks and verifies their integrity, using the encryption key.
The encryption, decryption and tamper proofing operations are transparent to Iceberg Avro, JSON and Puffin libraries.

### Encryption algorithm

AGS uses the standard AEG GCM cipher, and supports all AES key sizes: 128, 192 and 256 bits.

AES GCM is an authenticated encryption. Besides data confidentiality (encryption), it supports two levels of integrity verification (authentication): of the data (default), and of the data combined with an optional AAD (“additional authenticated data”). An AAD is a free text to be authenticated, together with the data. The structure of AGS AADs is described below.

AES GCM requires a unique vector to be provided for each encrypted block. In this document, the unique input to GCM encryption is called nonce (“number used once”). AGS encryption uses the RBG-based (random bit generator) nonce construction as defined in the section 8.2.2 of the NIST SP 800-38D document. For each encrypted block, AGS generates a unique nonce with a length of 12 bytes (96 bits).

## Format specification

### File structure

The AGS-encrypted files have the following structure

```
Magic BlockLength CipherBlock₁ CipherBlock₂ ... CipherBlockₙ
```

where

- `Magic` is four bytes 0x41, 0x47, 0x53, 0x31 ("AGS1", short for: AES GCM Stream, version 1)
- `BlockLength` is four bytes (little endian) integer keeping the length of the equal-size split blocks before encryption. The length is specified in bytes.
- `CipherBlockᵢ` is the i-th encrypted block in the file, with the structure defined below.

### Cipher Block structure

Cipher blocks have the following structure

| nonce | ciphertext | tag |
|-------|------------|-----|

where

- `nonce` is the AES GCM nonce, with a length of 12 bytes.
- `ciphertext` is the encrypted block. Its length is identical to the length of the block before encryption ("plaintext"). The length of all plaintext blocks, except the last, is `BlockLength` bytes. The last block keeps the data residue, with a length <= `BlockLength`.
- `tag` is the AES GCM tag, with a length of 16 bytes.

AGS encrypts all blocks by the GCM cipher, without padding. The AES GCM cipher must be implemented by a cryptographic provider according to the NIST SP 800-38D specification. In AGS, an input to the GCM cipher is an AES encryption key, a nonce, a plaintext and an AAD (described below). The output is a ciphertext with the length equal to that of plaintext, and a 16-byte authentication tag used to verify the ciphertext and AAD integrity.

### Additional Authenticated Data

The AES GCM cipher protects against byte replacement inside a ciphertext block - but, without an AAD, it can't prevent replacement of one ciphertext block with another (encrypted with the same key). AGS leverages AADs to protect against swapping ciphertext blocks inside a file or between files. AGS can also protect against swapping full files - for example, replacement of a metadata file with an old version. AADs are built to reflects the identity of a file and of the blocks inside the file.

AGS constructs a block AAD from two components: an AAD prefix - a string provided by Iceberg for the file (with the file ID), and an AAD suffix - the block sequence number in the file, as an int in a 4-byte little-endian form. The block AAD is a direct concatenation of the prefix and suffix parts.
