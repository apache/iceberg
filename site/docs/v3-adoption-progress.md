---
title: "Apache Iceberg V3 Table Format Adoption Progress"
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

# Apache Iceberg V3 Table Format Adoption Progress

This document tracks the implementation status of Apache Iceberg V3 Table Format features across different engines and components.

## Overview

Apache Iceberg V3 Table Format introduces several new features including new data types, row lineage, deletion vectors, default values, and table encryption. This document provides a comprehensive comparison of V3 feature implementation status across Apache Iceberg Core, Apache Spark, and Apache Flink.

## V3 Features Implementation Status

| **V3 Feature** | **Core** | **Spark v3.4** | **Spark v3.5** | **Spark v4.0** | **Flink v1.20** | **Flink v2.0** | **Flink v2.1** |
|---|---|---|---|---|---|---|---|
| **New Data Types** | | | | | | | |
| `unknown` type | ✅ | ❌ | ❌ | ✅ | ✅ | ✅ | ✅ |
| `timestamp_ns` (nanosecond timestamps) | ✅ | ❌ | ❌ | ❌ | ✅ | ✅ | ✅ |
| `variant` type | ⚠️ Avro supported, Parquet supported, ORC not supported | ⚠️ Read supported, Write shredded not supported | ⚠️ Read supported, Write shredded not supported | ⚠️ Read supported, Write shredded not supported | ❌ | ❌ | ❌ |
| `geometry` type | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `geography` type | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| **Deletion Vectors** | | | | | | | |
| Binary Deletion vector support | ✅ | ❌ | ❌ | ✅ | ❌ | ❌ | ❌ |
| **Default Values** | | | | | | | |
| Field default values | ✅ | ⚠️ Read/Write supported, DDL not supported | ⚠️ Read/Write supported, DDL not supported | ⚠️ Read/Write supported, DDL not supported | ✅ | ✅ | ✅ |
| **Table Encryption** | | | | | | | |
| AES-GCM encryption | ✅ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| **Multi-Argument Transforms** | | | | | | | |
| Z-order partitioning/sorting | ✅ | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ |
| **Row Lineage** | | | | | | | |
| `_row_id` column | ✅ | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ |
| `_last_updated_sequence_number` | ✅ | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ |

### Legend
- ✅ **Fully Implemented**
- ⚠️ **Partially Implemented** 
- ❌ **Not Implemented**

### Implementation Notes

**Core:**
- `variant` type: Avro ✅, Parquet ✅, ORC ❌
- `geometry`/`geography` types: Type definitions exist but no data processing implementation

**Spark (all versions):**
- `variant` type: Can read shredded and unshredded variant, missing writing shredded variant
- Field default values: Reader/writer support ✅, DDL integration (CREATE TABLE, ALTER TABLE) ❌

**Flink (all versions):**
- Full support includes both `timestamp_ns` and `timestamptz_ns`

## Implementation Status Summary

### Apache Iceberg Core
- **Status**: ✅ **Mostly Complete**
- **Implemented**: All core V3 features including new data types (except geometry/geography data processing), row lineage, deletion vectors, default values, and table encryption
- **Missing**: Actual data processing implementation for geometry and geography types (type definitions exist but no functional implementation)

### Apache Spark
- **Status**: ⚠️ **Partial Support** (v4.0+)
- **Implemented**: Row lineage, deletion vectors, default values, unknown type, and basic V3 features
- **Missing**: Variant type, geometry/geography types, table encryption, nanosecond timestamps

### Apache Flink
- **Status**: ⚠️ **Basic Support**
- **Implemented**: Unknown type, nanosecond timestamps, default values, and basic V3 features
- **Missing**: Row lineage, deletion vectors, variant type, geometry/geography types, table encryption

## File Format Support Details

### Variant Type File Format Support in Core

| **File Format** | **Core Support** | **Implementation Status** | **Notes** |
|---|---|---|---|
| **Avro** | ✅ **Full Support** | Complete with `VariantConversion` and `VariantLogicalType` | Full read/write support |
| **Parquet** | ✅ **Full Support** | Complete with `VariantWriterBuilder` and `VariantReaderBuilder` | Supports both serialized and shredded variants |
| **ORC** | ❌ **Not Implemented** | Schema conversion exists but throws `UnsupportedOperationException` | No actual read/write implementation |

### Geometry/Geography Type File Format Support

| **File Format** | **Core Support** | **Implementation Status** | **Notes** |
|---|---|---|---|
| **Avro** | ❌ **Not Implemented** | Type definitions exist but no data processing | No conversion or logical type support |
| **Parquet** | ❌ **Not Implemented** | Type definitions exist but no data processing | No read/write implementation |
| **ORC** | ❌ **Not Implemented** | Type definitions exist but no data processing | No read/write implementation |

### Timestamp Nanoseconds Support

| **Engine** | **timestamp_ns** (nanos) | **timestamptz_ns** (nanos) | **Notes** |
|---|---|---|---|
| **Core** | ✅ **Full Support** | ✅ **Full Support** | Complete support for all timestamp types and precisions |
| **Spark** | ❌ **Not Implemented** | ❌ **Not Implemented** | Only supports microsecond precision timestamps |
| **Flink** | ✅ **Full Support** | ✅ **Full Support** | Complete support for all timestamp types and precisions |

### Unknown Type Support

| **Engine** | **Core Support** | **Implementation Status** | **Notes** |
|---|---|---|---|
| **Core** | ✅ **Full Support** | Complete type system and schema evolution support | Supports optional unknown fields, proper validation |
| **Spark v4.0+** | ✅ **Full Support** | Complete type mapping to `NullType` | Full support with comprehensive tests |
| **Spark v3.4/v3.5** | ❌ **Not Implemented** | Missing from type conversion | Throws "Cannot convert unknown type to Spark" error |
| **Flink** | ✅ **Full Support** | Complete type mapping to `NullType` | Full support across all versions (v1.20, v2.0, v2.1) |

### Multi-Argument Transforms Support

| **Engine** | **Z-Order Support** | **Implementation Status** | **Notes** |
|---|---|---|---|
| **Core** | ✅ **Full Support** | Complete `Zorder` class implementation | Full support for multi-column transforms in partitioning and sorting |
| **Spark** | ✅ **Full Support** | Complete `SparkZOrderFileRewriteRunner` implementation | Full support across all versions (v3.4, v3.5, v4.0) with file rewriting |
| **Flink** | ❌ **Not Implemented** | No multi-argument transform support | Only single-argument transforms supported |

### Default Values Support

| **Engine** | **initial-default** | **write-default** | **Notes** |
|---|---|---|---|
| **Core** | ✅ **Full Support** | ✅ **Full Support** | Complete support for both types with schema evolution |
| **Spark** | ✅ **Full Support** | ✅ **Full Support** | Full support across all versions (v3.4, v3.5, v4.0) |
| **Flink** | ✅ **Full Support** | ✅ **Full Support** | Full support across all versions (v1.20, v2.0, v2.1) |

## Critical Missing Features

### 1. Geometry and Geography Types
- **Status**: ❌ **Not Implemented** across all engines
- **Issue**: While type definitions exist in the API module, no engine has actual data processing implementation
- **Impact**: Spatial data operations are not supported

### 2. Table Encryption
- **Status**: ❌ **Only Core supports it**
- **Issue**: Spark and Flink don't support AES-GCM table encryption
- **Impact**: Security features are limited to Core usage

### 3. Variant Type
- **Status**: ⚠️ **Partially Implemented in Core**
- **Issue**: Core supports Avro and Parquet but not ORC; Spark and Flink have no support
- **Impact**: Semi-structured data handling is limited to specific file formats

### 4. Nanosecond Timestamps
- **Status**: ❌ **Spark doesn't support them**
- **Issue**: Spark connector doesn't support nanosecond precision timestamps
- **Impact**: High-precision timestamp operations are not available in Spark

## Recommendations for Users

1. **Core**: Use Core for full V3 feature support, especially for security and advanced data types
   - **Variant Type**: Use Avro or Parquet file formats (ORC not supported)
   - **Geometry/Geography**: Not yet supported in any file format
2. **Spark**: Use v4.0+ for row lineage and deletion vectors, but be aware of missing features
3. **Flink**: Use for basic V3 support, but avoid features requiring row lineage or deletion vectors

## References

- [Iceberg V3 Table Format Specification](../format/spec.md)

---

*This document is maintained by the Apache Iceberg community. Please contribute updates as implementation status changes.*
