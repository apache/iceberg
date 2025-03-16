/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iceberg.parquet;

import java.util.Deque;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.iceberg.parquet.ParquetVariantReaders.VariantValueReader;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Streams;
import org.apache.iceberg.variants.PhysicalType;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation.DateLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.DecimalLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.IntLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.LogicalTypeAnnotationVisitor;
import org.apache.parquet.schema.LogicalTypeAnnotation.StringLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.TimestampLogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;

public class VariantReaderBuilder extends ParquetVariantVisitor<ParquetValueReader<?>> {
  private final MessageType schema;
  private final Iterable<String> basePath;
  private final Deque<String> fieldNames = Lists.newLinkedList();

  public VariantReaderBuilder(MessageType schema, Iterable<String> basePath) {
    this.schema = schema;
    this.basePath = basePath;
  }

  @Override
  public void beforeField(Type type) {
    fieldNames.addLast(type.getName());
  }

  @Override
  public void afterField(Type type) {
    fieldNames.removeLast();
  }

  private String[] currentPath() {
    return Streams.concat(Streams.stream(basePath), fieldNames.stream()).toArray(String[]::new);
  }

  private String[] path(String name) {
    return Streams.concat(Streams.stream(basePath), fieldNames.stream(), Stream.of(name))
        .toArray(String[]::new);
  }

  @Override
  public ParquetValueReader<?> variant(
      GroupType variant, ParquetValueReader<?> metadataReader, ParquetValueReader<?> valueReader) {
    return ParquetVariantReaders.variant(metadataReader, valueReader);
  }

  @Override
  public ParquetValueReader<?> metadata(PrimitiveType metadata) {
    ColumnDescriptor desc = schema.getColumnDescription(currentPath());
    return ParquetVariantReaders.metadata(desc);
  }

  @Override
  public VariantValueReader serialized(PrimitiveType value) {
    ColumnDescriptor desc = schema.getColumnDescription(currentPath());
    return ParquetVariantReaders.serialized(desc);
  }

  @Override
  public VariantValueReader primitive(PrimitiveType primitive) {
    ColumnDescriptor desc = schema.getColumnDescription(currentPath());

    if (primitive.getLogicalTypeAnnotation() != null) {
      Optional<VariantValueReader> reader =
          primitive.getLogicalTypeAnnotation().accept(new LogicalTypeToVariantReader(desc));
      if (reader.isPresent()) {
        return reader.get();
      }

    } else {
      switch (primitive.getPrimitiveTypeName()) {
        case BINARY:
          return ParquetVariantReaders.asVariant(
              PhysicalType.BINARY, ParquetValueReaders.byteBuffers(desc));
        case BOOLEAN:
          // the actual boolean type will be fixed in PrimitiveWrapper
          return ParquetVariantReaders.asVariant(
              PhysicalType.BOOLEAN_TRUE, ParquetValueReaders.unboxed(desc));
        case INT32:
          return ParquetVariantReaders.asVariant(
              PhysicalType.INT32, ParquetValueReaders.unboxed(desc));
        case INT64:
          return ParquetVariantReaders.asVariant(
              PhysicalType.INT64, ParquetValueReaders.unboxed(desc));
        case FLOAT:
          return ParquetVariantReaders.asVariant(
              PhysicalType.FLOAT, ParquetValueReaders.unboxed(desc));
        case DOUBLE:
          return ParquetVariantReaders.asVariant(
              PhysicalType.DOUBLE, ParquetValueReaders.unboxed(desc));
      }
    }

    // note that both FIXED_LEN_BYTE_ARRAY and INT96 are not valid Variant primitives
    throw new UnsupportedOperationException("Unsupported shredded value type: " + primitive);
  }

  @Override
  public VariantValueReader value(
      GroupType group, ParquetValueReader<?> valueReader, ParquetValueReader<?> typedReader) {
    int valueDL =
        valueReader != null ? schema.getMaxDefinitionLevel(path(VALUE)) - 1 : Integer.MAX_VALUE;
    int typedDL =
        typedReader != null
            ? schema.getMaxDefinitionLevel(path(TYPED_VALUE)) - 1
            : Integer.MAX_VALUE;
    return ParquetVariantReaders.shredded(valueDL, valueReader, typedDL, typedReader);
  }

  @Override
  public VariantValueReader object(
      GroupType group,
      ParquetValueReader<?> valueReader,
      List<ParquetValueReader<?>> fieldResults) {
    int valueDL =
        valueReader != null ? schema.getMaxDefinitionLevel(path(VALUE)) - 1 : Integer.MAX_VALUE;
    int fieldsDL = schema.getMaxDefinitionLevel(path(TYPED_VALUE)) - 1;

    List<String> shreddedFieldNames =
        group.getType(TYPED_VALUE).asGroupType().getFields().stream()
            .map(Type::getName)
            .collect(Collectors.toList());
    List<VariantValueReader> fieldReaders =
        fieldResults.stream().map(VariantValueReader.class::cast).collect(Collectors.toList());

    return ParquetVariantReaders.objects(
        valueDL, valueReader, fieldsDL, shreddedFieldNames, fieldReaders);
  }

  @Override
  public VariantValueReader array(
      GroupType array, ParquetValueReader<?> valueResult, ParquetValueReader<?> elementResult) {
    throw new UnsupportedOperationException("Array is not yet supported");
  }

  private static class LogicalTypeToVariantReader
      implements LogicalTypeAnnotationVisitor<VariantValueReader> {
    private final ColumnDescriptor desc;

    private LogicalTypeToVariantReader(ColumnDescriptor desc) {
      this.desc = desc;
    }

    @Override
    public Optional<VariantValueReader> visit(StringLogicalTypeAnnotation ignored) {
      VariantValueReader reader =
          ParquetVariantReaders.asVariant(PhysicalType.STRING, ParquetValueReaders.strings(desc));

      return Optional.of(reader);
    }

    @Override
    public Optional<VariantValueReader> visit(DecimalLogicalTypeAnnotation logical) {
      PhysicalType variantType = variantDecimalType(desc.getPrimitiveType());
      VariantValueReader reader =
          ParquetVariantReaders.asVariant(variantType, ParquetValueReaders.bigDecimals(desc));

      return Optional.of(reader);
    }

    @Override
    public Optional<VariantValueReader> visit(DateLogicalTypeAnnotation ignored) {
      VariantValueReader reader =
          ParquetVariantReaders.asVariant(PhysicalType.DATE, ParquetValueReaders.unboxed(desc));

      return Optional.of(reader);
    }

    @Override
    public Optional<VariantValueReader> visit(TimestampLogicalTypeAnnotation logical) {
      PhysicalType variantType =
          logical.isAdjustedToUTC() ? PhysicalType.TIMESTAMPTZ : PhysicalType.TIMESTAMPNTZ;

      VariantValueReader reader =
          ParquetVariantReaders.asVariant(variantType, ParquetValueReaders.timestamps(desc));

      return Optional.of(reader);
    }

    @Override
    public Optional<VariantValueReader> visit(IntLogicalTypeAnnotation logical) {
      if (!logical.isSigned()) {
        // unsigned ints are not allowed for shredded fields
        throw new UnsupportedOperationException("Unsupported shredded value type: " + logical);
      }

      VariantValueReader reader;
      switch (logical.getBitWidth()) {
        case 64:
          reader =
              ParquetVariantReaders.asVariant(
                  PhysicalType.INT64, ParquetValueReaders.unboxed(desc));
          break;
        case 32:
          reader =
              ParquetVariantReaders.asVariant(
                  PhysicalType.INT32, ParquetValueReaders.unboxed(desc));
          break;
        case 16:
          reader =
              ParquetVariantReaders.asVariant(
                  PhysicalType.INT16, ParquetValueReaders.intsAsShort(desc));
          break;
        case 8:
          reader =
              ParquetVariantReaders.asVariant(
                  PhysicalType.INT8, ParquetValueReaders.intsAsByte(desc));
          break;
        default:
          throw new IllegalArgumentException("Invalid bit width for int: " + logical.getBitWidth());
      }

      return Optional.of(reader);
    }

    private static PhysicalType variantDecimalType(PrimitiveType primitive) {
      switch (primitive.getPrimitiveTypeName()) {
        case FIXED_LEN_BYTE_ARRAY:
        case BINARY:
          return PhysicalType.DECIMAL16;
        case INT64:
          return PhysicalType.DECIMAL8;
        case INT32:
          return PhysicalType.DECIMAL4;
      }

      throw new IllegalArgumentException("Invalid primitive type for decimal: " + primitive);
    }
  }
}
