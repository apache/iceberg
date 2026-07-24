---
title: "Encryption Spec"
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

# Iceberg Encryption Spec

This document specifies the standard encryption scheme for Apache Iceberg tables. It defines the interoperable, cross-implementation binary format for per-file encryption key material referenced by the `key_metadata` fields in the [table spec](spec.md), along with the key hierarchy used to manage those keys.

Encrypted key material is tracked in two places:

* The `key_metadata` fields in [manifest entries](spec.md#manifests), [manifest list entries](spec.md#manifest-lists), and [statistics files](spec.md#table-metadata-fields) store the per-file key material.
* The table metadata [`encryption-keys`](spec.md#encryption-keys) list tracks the key hierarchy used to protect that per-file material.

## Standard Key Metadata

The `key_metadata` field in manifest entries stores per-file encryption key material as a binary blob. To enable cross-implementation interoperability, the standard encryption scheme defines the following binary format for this field:

```
VersionByte Payload
```

where:

* `VersionByte` is a single byte indicating the key metadata schema version. Currently, the only valid version is `0x01`.
* `Payload` is an Avro binary-encoded record (not a container file — only the raw binary encoding of the fields) using the schema for the given version.

The Avro schema for version 1 is a record with the following fields, in order:

| Field name | Avro type | Required | Description |
|---|---|---|---|
| **`encryption_key`** | `bytes` | _required_ | The data encryption key (DEK) for this file. Must be 16, 24, or 32 bytes (corresponding to AES-128, AES-192, or AES-256). |
| **`aad_prefix`** | `bytes` | _optional_ | Random AAD prefix used for encryption integrity protection. For [AES GCM Stream](gcm-stream-spec.md) files, the prefix is combined with a block index to form the per-block AAD. For [Parquet modular encryption](https://parquet.apache.org/docs/file-format/data-pages/encryption/), the prefix is passed as the AAD prefix parameter, which is combined with a module AAD suffix to form the full AAD for each Parquet module. |
| **`file_length`** | `long` | _optional_ | The encrypted file length in bytes. Required for [AES GCM Stream](gcm-stream-spec.md) encrypted files to detect truncation attacks (see [AES GCM Stream file length](gcm-stream-spec.md#file-length)). Not set for Parquet encrypted files. |

The usage of the `encryption_key` and `aad_prefix` fields depends on the file format:

* **AES GCM Stream files**:
  - Manifest lists
  - Manifests
  - Avro data files
  - Puffin files

  The `encryption_key` is used directly as the AES-GCM key. The `aad_prefix` is combined with a 4-byte little-endian block index to form the AAD for each cipher block, as described in the [AES GCM Stream AAD section](gcm-stream-spec.md#additional-authenticated-data). The `file_length` field stores the encrypted file length for truncation detection.

* **Parquet encrypted files**: The `encryption_key` and `aad_prefix` are provided to Parquet readers and writers, which delegate encryption to the [Parquet modular encryption](https://parquet.apache.org/docs/file-format/data-pages/encryption/) format.

### Encryption Key Hierarchy

The standard encryption scheme uses a two-tier key hierarchy tracked in the table metadata [`encryption-keys`](spec.md#encryption-keys) list:

1. **Key Encryption Keys (KEKs):** Entries where `encrypted-by-id` equals the table's encryption key ID (configured via `encryption.key-id`). The `encrypted-key-metadata` contains the KEK wrapped by the KMS and is opaque to Iceberg — its format is determined by the KMS provider. KEK entries must include a `KEY_TIMESTAMP` property recording the creation time in milliseconds since epoch; this timestamp is used as the AAD when encrypting manifest list key metadata.

2. **Manifest List Keys:** Entries where `encrypted-by-id` references a KEK. The `encrypted-key-metadata` contains the Standard Key Metadata (defined above) encrypted with AES GCM using the referenced unwrapped KEK. The ciphertext format is:

    ```
    Nonce Ciphertext Tag
    ```

where `Nonce` is 12 bytes, `Ciphertext` is the encrypted Standard Key Metadata payload, and `Tag` is the 16-byte GCM authentication tag. The AAD for this encryption is the KEK's `KEY_TIMESTAMP` property value encoded as UTF-8 bytes.

The snapshot field `key-id` references the encryption key entry used to encrypt that snapshot's manifest list key metadata.
