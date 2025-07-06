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

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.io.IOUtil;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.variants.PhysicalType;
import org.apache.iceberg.variants.ShreddedObject;
import org.apache.iceberg.variants.Variant;
import org.apache.iceberg.variants.VariantArray;
import org.apache.iceberg.variants.VariantMetadata;
import org.apache.iceberg.variants.VariantObject;
import org.apache.iceberg.variants.VariantValue;
import org.apache.iceberg.variants.Variants;
import org.apache.parquet.column.ColumnWriteStore;

class ParquetVariantWriters {
  private ParquetVariantWriters() {}

  @SuppressWarnings("unchecked")
  static ParquetValueWriter<Variant> variant(
      ParquetValueWriter<?> metadataWriter, ParquetValueWriter<?> valueWriter) {
    return new VariantWriter(
        (ParquetValueWriter<VariantMetadata>) metadataWriter,
        (ParquetValueWriter<VariantValue>) valueWriter);
  }

  @SuppressWarnings("unchecked")
  static ParquetValueWriter<VariantMetadata> metadata(ParquetValueWriter<?> bytesWriter) {
    return new VariantMetadataWriter((ParquetValueWriter<ByteBuffer>) bytesWriter);
  }

  @SuppressWarnings("unchecked")
  static ParquetValueWriter<VariantValue> value(ParquetValueWriter<?> bytesWriter) {
    return new VariantValueWriter((ParquetValueWriter<ByteBuffer>) bytesWriter);
  }

  static ParquetValueWriter<VariantValue> primitive(
      ParquetValueWriter<?> writer, PhysicalType... types) {
    return new PrimitiveWriter<>(writer, Sets.immutableEnumSet(Arrays.asList(types)));
  }

  @SuppressWarnings("unchecked")
  static ParquetValueWriter<VariantValue> shredded(
      int valueDefinitionLevel,
      ParquetValueWriter<?> valueWriter,
      int typedDefinitionLevel,
      ParquetValueWriter<?> typedWriter) {
    return new ShreddedVariantWriter(
        valueDefinitionLevel,
        (ParquetValueWriter<VariantValue>) valueWriter,
        typedDefinitionLevel,
        (TypedWriter) typedWriter);
  }

  @SuppressWarnings("unchecked")
  static ParquetValueWriter<VariantValue> objects(
      int valueDefinitionLevel,
      ParquetValueWriter<?> valueWriter,
      int typedDefinitionLevel,
      int fieldDefinitionLevel,
      List<String> fieldNames,
      List<ParquetValueWriter<?>> fieldWriters) {
    ImmutableMap.Builder<String, ParquetValueWriter<VariantValue>> builder = ImmutableMap.builder();
    for (int i = 0; i < fieldNames.size(); i += 1) {
      builder.put(fieldNames.get(i), (ParquetValueWriter<VariantValue>) fieldWriters.get(i));
    }

    return new ShreddedObjectWriter(
        valueDefinitionLevel,
        (ParquetValueWriter<VariantValue>) valueWriter,
        typedDefinitionLevel,
        fieldDefinitionLevel,
        builder.build());
  }

  @SuppressWarnings("unchecked")
  public static ParquetValueWriter<VariantValue> array(
      int repeatedDefinitionLevel,
      int repeatedRepetitionLevel,
      ParquetValueWriter<?> elementWriter) {
    return new ArrayWriter(
        repeatedDefinitionLevel,
        repeatedRepetitionLevel,
        (ParquetValueWriter<VariantValue>) elementWriter);
  }

  private static class VariantWriter implements ParquetValueWriter<Variant> {
    private final ParquetValueWriter<VariantMetadata> metadataWriter;
    private final ParquetValueWriter<VariantValue> valueWriter;
    private final List<TripleWriter<?>> children;

    private VariantWriter(
        ParquetValueWriter<VariantMetadata> metadataWriter,
        ParquetValueWriter<VariantValue> valueWriter) {
      this.metadataWriter = metadataWriter;
      this.valueWriter = valueWriter;
      this.children = children(metadataWriter, valueWriter);
    }

    @Override
    public void write(int repetitionLevel, Variant variant) {
      metadataWriter.write(repetitionLevel, variant.metadata());
      valueWriter.write(repetitionLevel, variant.value());
    }

    @Override
    public List<TripleWriter<?>> columns() {
      return children;
    }

    @Override
    public void setColumnStore(ColumnWriteStore columnStore) {
      metadataWriter.setColumnStore(columnStore);
      valueWriter.setColumnStore(columnStore);
    }
  }

  private abstract static class VariantBinaryWriter<T> implements ParquetValueWriter<T> {
    private final ParquetValueWriter<ByteBuffer> bytesWriter;
    private ByteBuffer reusedBuffer = ByteBuffer.allocate(2048).order(ByteOrder.LITTLE_ENDIAN);

    private VariantBinaryWriter(ParquetValueWriter<ByteBuffer> bytesWriter) {
      this.bytesWriter = bytesWriter;
    }

    protected abstract int sizeInBytes(T value);

    protected abstract int writeTo(ByteBuffer buffer, int offset, T value);

    @Override
    public void write(int repetitionLevel, T value) {
      bytesWriter.write(repetitionLevel, serialize(value));
    }

    @Override
    public List<TripleWriter<?>> columns() {
      return bytesWriter.columns();
    }

    @Override
    public void setColumnStore(ColumnWriteStore columnStore) {
      bytesWriter.setColumnStore(columnStore);
    }

    private void ensureCapacity(int requiredSize) {
      if (reusedBuffer.capacity() < requiredSize) {
        int newCapacity = IOUtil.capacityFor(requiredSize);
        this.reusedBuffer = ByteBuffer.allocate(newCapacity).order(ByteOrder.LITTLE_ENDIAN);
      } else {
        reusedBuffer.limit(requiredSize);
      }
    }

    private ByteBuffer serialize(T value) {
      ensureCapacity(sizeInBytes(value));
      int size = writeTo(reusedBuffer, 0, value);
      reusedBuffer.position(0);
      reusedBuffer.limit(size);
      return reusedBuffer;
    }
  }

  private static class VariantMetadataWriter extends VariantBinaryWriter<VariantMetadata> {
    private VariantMetadataWriter(ParquetValueWriter<ByteBuffer> bytesWriter) {
      super(bytesWriter);
    }

    @Override
    protected int sizeInBytes(VariantMetadata metadata) {
      return metadata.sizeInBytes();
    }

    @Override
    protected int writeTo(ByteBuffer buffer, int offset, VariantMetadata metadata) {
      return metadata.writeTo(buffer, offset);
    }
  }

  private static class VariantValueWriter extends VariantBinaryWriter<VariantValue> {
    private VariantValueWriter(ParquetValueWriter<ByteBuffer> bytesWriter) {
      super(bytesWriter);
    }

    @Override
    protected int sizeInBytes(VariantValue value) {
      return value.sizeInBytes();
    }

    @Override
    protected int writeTo(ByteBuffer buffer, int offset, VariantValue value) {
      return value.writeTo(buffer, offset);
    }
  }

  private interface TypedWriter extends ParquetValueWriter<VariantValue> {
    Set<PhysicalType> types();
  }

  private static class PrimitiveWriter<T> implements TypedWriter {
    private final Set<PhysicalType> types;
    private final ParquetValueWriter<T> writer;

    private PrimitiveWriter(ParquetValueWriter<T> writer, Set<PhysicalType> types) {
      this.types = types;
      this.writer = writer;
    }

    @Override
    public Set<PhysicalType> types() {
      return types;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void write(int repetitionLevel, VariantValue value) {
      writer.write(repetitionLevel, (T) value.asPrimitive().get());
    }

    @Override
    public List<TripleWriter<?>> columns() {
      return writer.columns();
    }

    @Override
    public void setColumnStore(ColumnWriteStore columnStore) {
      writer.setColumnStore(columnStore);
    }
  }

  private static class ShreddedVariantWriter implements ParquetValueWriter<VariantValue> {
    private final int valueDefinitionLevel;
    private final ParquetValueWriter<VariantValue> valueWriter;
    private final int typedDefinitionLevel;
    private final TypedWriter typedWriter;
    private final List<TripleWriter<?>> children;

    private ShreddedVariantWriter(
        int valueDefinitionLevel,
        ParquetValueWriter<VariantValue> valueWriter,
        int typedDefinitionLevel,
        TypedWriter typedWriter) {
      this.valueDefinitionLevel = valueDefinitionLevel;
      this.valueWriter = valueWriter;
      this.typedDefinitionLevel = typedDefinitionLevel;
      this.typedWriter = typedWriter;
      this.children = children(valueWriter, typedWriter);
    }

    @Override
    public void write(int repetitionLevel, VariantValue value) {
      if (typedWriter.types().contains(value.type())) {
        typedWriter.write(repetitionLevel, value);
        writeNull(valueWriter, repetitionLevel, valueDefinitionLevel);
      } else {
        valueWriter.write(repetitionLevel, value);
        writeNull(typedWriter, repetitionLevel, typedDefinitionLevel);
      }
    }

    @Override
    public List<TripleWriter<?>> columns() {
      return children;
    }

    @Override
    public void setColumnStore(ColumnWriteStore columnStore) {
      valueWriter.setColumnStore(columnStore);
      typedWriter.setColumnStore(columnStore);
    }
  }

  private static class ShreddedObjectWriter implements ParquetValueWriter<VariantValue> {
    private final int valueDefinitionLevel;
    private final ParquetValueWriter<VariantValue> valueWriter;
    private final int typedDefinitionLevel;
    private final int fieldDefinitionLevel;
    private final Map<String, ParquetValueWriter<VariantValue>> typedWriters;
    private final List<TripleWriter<?>> children;

    private ShreddedObjectWriter(
        int valueDefinitionLevel,
        ParquetValueWriter<VariantValue> valueWriter,
        int typedDefinitionLevel,
        int fieldDefinitionLevel,
        Map<String, ParquetValueWriter<VariantValue>> typedWriters) {
      this.valueDefinitionLevel = valueDefinitionLevel;
      this.valueWriter = valueWriter;
      this.typedDefinitionLevel = typedDefinitionLevel;
      this.fieldDefinitionLevel = fieldDefinitionLevel;
      this.typedWriters = typedWriters;
      this.children =
          children(
              Iterables.concat(
                  ImmutableList.of(valueWriter), ImmutableList.copyOf(typedWriters.values())));
    }

    @Override
    public void write(int repetitionLevel, VariantValue value) {
      if (value.type() != PhysicalType.OBJECT) {
        valueWriter.write(repetitionLevel, value);

        // write null for the typed_value group
        for (ParquetValueWriter<?> writer : typedWriters.values()) {
          writeNull(writer, repetitionLevel, typedDefinitionLevel);
        }

      } else {
        VariantObject object = value.asObject();
        ShreddedObject shredded = Variants.object(object);
        for (Map.Entry<String, ParquetValueWriter<VariantValue>> entry : typedWriters.entrySet()) {
          String fieldName = entry.getKey();
          ParquetValueWriter<VariantValue> writer = entry.getValue();

          VariantValue fieldValue = object.get(fieldName);
          if (fieldValue != null) {
            // shredded: suppress the field in the object and write it to the value pair
            shredded.remove(fieldName);
            writer.write(repetitionLevel, fieldValue);
          } else {
            // missing: write null to both value and typed_value
            writeNull(writer, repetitionLevel, fieldDefinitionLevel);
          }
        }

        if (shredded.numFields() > 0) {
          // partially shredded: write the unshredded fields
          valueWriter.write(repetitionLevel, shredded);
        } else {
          // completely shredded: omit the empty value
          writeNull(valueWriter, repetitionLevel, valueDefinitionLevel);
        }
      }
    }

    @Override
    public List<TripleWriter<?>> columns() {
      return children;
    }

    @Override
    public void setColumnStore(ColumnWriteStore columnStore) {
      valueWriter.setColumnStore(columnStore);
      for (ParquetValueWriter<?> fieldWriter : typedWriters.values()) {
        fieldWriter.setColumnStore(columnStore);
      }
    }
  }

  private static class ArrayWriter implements TypedWriter {
    private final int definitionLevel;
    private final int repetitionLevel;
    private final ParquetValueWriter<VariantValue> writer;
    private final List<TripleWriter<?>> children;

    protected ArrayWriter(
        int definitionLevel, int repetitionLevel, ParquetValueWriter<VariantValue> writer) {
      this.definitionLevel = definitionLevel;
      this.repetitionLevel = repetitionLevel;
      this.writer = writer;
      this.children = writer.columns();
    }

    @Override
    public Set<PhysicalType> types() {
      return Set.of(PhysicalType.ARRAY);
    }

    @Override
    public void write(int parentRepetition, VariantValue value) {
      VariantArray arr = value.asArray();
      if (arr.numElements() == 0) {
        writeNull(writer, parentRepetition, definitionLevel);
      } else {
        for (int i = 0; i < arr.numElements(); i++) {
          VariantValue element = arr.get(i);

          int rl = repetitionLevel;
          if (i == 0) {
            rl = parentRepetition;
          }

          writer.write(rl, element);
        }
      }
    }

    @Override
    public List<TripleWriter<?>> columns() {
      return children;
    }

    @Override
    public void setColumnStore(ColumnWriteStore columnStore) {
      writer.setColumnStore(columnStore);
    }
  }

  private static void writeNull(
      ParquetValueWriter<?> writer, int repetitionLevel, int definitionLevel) {
    for (TripleWriter<?> column : writer.columns()) {
      column.writeNull(repetitionLevel, definitionLevel - 1);
    }
  }

  private static List<TripleWriter<?>> children(ParquetValueWriter<?>... writers) {
    return children(Arrays.asList(writers));
  }

  private static List<TripleWriter<?>> children(Iterable<ParquetValueWriter<?>> writers) {
    return ImmutableList.copyOf(
        Iterables.concat(Iterables.transform(writers, ParquetValueWriter::columns)));
  }
}
