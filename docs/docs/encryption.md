---
title: "Encryption"
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

# Encryption

Iceberg table encryption protects confidentiality and integrity of table data in an untrusted storage. The `data`, `delete`, `manifest` and `manifest list` files are encrypted and tamper-proofed before being sent to the storage backend.

The `metadata.json` file does not contain data or stats, and is therefore not encrypted.

Currently, encryption is supported in the Hive and REST catalogs for tables with Parquet and Avro data formats.

Two parameters are required to activate encryption of a table:

1. Catalog property `encryption.kms-impl`, that specifies the class path for a client of a KMS ("key management service").
2. Table property `encryption.key-id`, that specifies the ID of a master key used to encrypt and decrypt the table. Master keys are stored and managed in the KMS.

For more details on table encryption, see the "Appendix: Internals Overview" [subsection](#appendix-internals-overview).

## Example

```sh
spark-sql --packages org.apache.iceberg:iceberg-spark-runtime-{{ sparkVersionMajor }}:{{ icebergVersion }}\
    --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
    --conf spark.sql.catalog.spark_catalog=org.apache.iceberg.spark.SparkSessionCatalog \
    --conf spark.sql.catalog.spark_catalog.type=hive \
    --conf spark.sql.catalog.local=org.apache.iceberg.spark.SparkCatalog \
    --conf spark.sql.catalog.local.type=hive \
    --conf spark.sql.catalog.local.encryption.kms-impl=org.apache.iceberg.aws.AwsKeyManagementClient
```

```sql
CREATE TABLE local.db.table (id bigint, data string) USING iceberg
TBLPROPERTIES ('encryption.key-id'='<master-key-id>');
```

Inserted data will be automatically encrypted,

```sql
INSERT INTO local.db.table VALUES (1, 'a'), (2, 'b'), (3, 'c');
```

To verify encryption, the contents of data, manifest and manifest list files can be dumped in the command line with

```sh
hexdump -C <path/to/file> | more
```

The Parquet files must start with the "PARE" magic string (PARquet Encrypted footer mode), and manifest/list files must start with "AGS1" magic string (Aes Gcm Stream version 1).

Queried data will be automatically decrypted,

```sql
SELECT * FROM local.db.table;
```

## Catalog security requirements

1. Catalogs must ensure the `encryption.key-id` property is not modified or removed during table lifetime.

2. To function properly, Iceberg table encryption requires the catalog implementations not to retrieve the metadata directly from metadata.json files, if these files are kept unprotected in a storage vulnerable to tampering:

    - Catalogs may keep the metadata in a trusted independent object store.
    - Catalogs may work with metadata.json files in a tamper-proof storage.
    - Catalogs may use checksum techniques to verify integrity of metadata.json files in a storage vulnerable to tampering
      (the checksums must be kept in a separate trusted storage).

## Key Management Clients

Currently, Iceberg has clients for the AWS, GCP and Azure KMS systems. A custom client can be built for other key management systems by implementing the `org.apache.iceberg.encryption.KeyManagementClient` interface.

This interface has the following main methods,

```java
  /**
   * Initialize the KMS client with given properties.
   *
   * @param properties kms client properties (taken from catalog properties)
   */
  void initialize(Map<String, String> properties);

  /**
   * Wrap a secret key, using a wrapping/master key which is stored in KMS and referenced by an ID.
   * Wrapping means encryption of the secret key with the master key, and adding optional
   * KMS-specific metadata that allows the KMS to decrypt the secret key in an unwrapping call.
   *
   * @param key a secret key being wrapped
   * @param wrappingKeyId a key ID that represents a wrapping key stored in KMS
   * @return wrapped key material
   */
  ByteBuffer wrapKey(ByteBuffer key, String wrappingKeyId);

  /**
   * Unwrap a secret key, using a wrapping/master key which is stored in KMS and referenced by an
   * ID.
   *
   * @param wrappedKey wrapped key material (encrypted key and optional KMS metadata, returned by
   *     the wrapKey method)
   * @param wrappingKeyId a key ID that represents a wrapping key stored in KMS
   * @return raw key bytes
   */
  ByteBuffer unwrapKey(ByteBuffer wrappedKey, String wrappingKeyId);
```

## Appendix: Internals Overview

The standard Iceberg encryption manager generates an encryption key and a unique file ID ("AAD prefix")
for each data and delete file. The generation is performed in the worker nodes, by using a secure random
number generator. For Parquet data files, these parameters are passed to the native Parquet Modular
Encryption [mechanism](https://parquet.apache.org/docs/file-format/data-pages/encryption). For Avro data files,
these parameters are passed to the AES GCM Stream encryption [mechanism](../../gcm-stream-spec.md).

The parent manifest file stores the encryption key and AAD prefix for each data and delete file in the
`key_metadata` [field](../../spec.md#data-file-fields). For Avro data tables, the data file length
is also added to the `key_metadata`.
The manifest file is encrypted by the AES GCM Stream encryption mechanism, using an encryption key and an
AAD prefix generated by the standard encryption manager. The generation is performed in the driver nodes,
by using a secure random number generator.

The parent manifest list file stores the encryption key, AAD prefix and file length for each manifest file
in the `key_metadata` [field](../../spec.md#manifest-lists). The manifest list file is encrypted by
the AES GCM Stream encryption mechanism,
using an encryption key and an AAD prefix generated by the standard encryption manager.

The manifest list encryption key, AAD prefix and file length are packed in a key metadata object. This object
is serialized and encrypted with a "key encryption key" (KEK), using the KEK creation timestamp as the AES
GCM AAD. A KEK and its unique KEK_ID are generated by using a secure random number generator. For each
snapshot, the KEK_ID of the encryption key that encrypts the manifest list key metadata is kept in the
`key-id` field in the table metadata snapshot [structure](../../spec.md#snapshots). The encrypted
manifest list key metadata is kept in the `encryption-keys` list in the table metadata
[structure](../../spec.md#table-metadata-fields).

The KEK is encrypted by the table master key via the KMS client. The result is kept in the `encryption-keys`
list in the table metadata structure. The KEK is re-used for a period allowed by the NIST SP 800-57
specification. Then, it is rotated - a new KEK and KEK_ID are generated for encryption of new manifest list
key metadata objects. The new KEK is encrypted by the table master key and stored in the `encryption-keys`
list in the table metadata structure. The previous KEKs are retained for the existing table snapshots.
