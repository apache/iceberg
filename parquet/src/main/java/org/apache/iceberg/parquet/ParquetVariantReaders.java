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
import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.commons.lang3.stream.Streams;
import org.apache.iceberg.parquet.ParquetValueReaders.PrimitiveReader;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.variants.PhysicalType;
import org.apache.iceberg.variants.ShreddedObject;
import org.apache.iceberg.variants.Variant;
import org.apache.iceberg.variants.VariantMetadata;
import org.apache.iceberg.variants.VariantObject;
import org.apache.iceberg.variants.VariantValue;
import org.apache.iceberg.variants.Variants;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.page.PageReadStore;

public class ParquetVariantReaders {
  private ParquetVariantReaders() {}

  public interface VariantValueReader extends ParquetValueReader<VariantValue> {
    @Override
    default VariantValue read(VariantValue reuse) {
      throw new UnsupportedOperationException("Variants must be read using read(VariantMetadata)");
    }

    /** Reads a variant value */
    VariantValue read(VariantMetadata metadata);
  }

  private static final VariantValue MISSING = null;

  @SuppressWarnings("unchecked")
  public static ParquetValueReader<Variant> variant(
      ParquetValueReader<?> metadata, ParquetValueReader<?> value) {
    return new VariantReader(
        (ParquetValueReader<VariantMetadata>) metadata, (VariantValueReader) value);
  }

  public static ParquetValueReader<VariantMetadata> metadata(ColumnDescriptor desc) {
    return new VariantMetadataReader(desc);
  }

  public static VariantValueReader serialized(ColumnDescriptor desc) {
    return new SerializedVariantReader(desc);
  }

  public static VariantValueReader shredded(
      int valueDefinitionLevel,
      ParquetValueReader<?> valueReader,
      int typedDefinitionLevel,
      ParquetValueReader<?> typedReader) {
    return new ShreddedVariantReader(
        valueDefinitionLevel,
        (VariantValueReader) valueReader,
        typedDefinitionLevel,
        (VariantValueReader) typedReader);
  }

  public static VariantValueReader objects(
      int valueDefinitionLevel,
      ParquetValueReader<?> valueReader,
      int typedDefinitionLevel,
      List<String> fieldNames,
      List<VariantValueReader> fieldReaders) {
    return new ShreddedObjectReader(
        valueDefinitionLevel,
        (VariantValueReader) valueReader,
        typedDefinitionLevel,
        fieldNames,
        fieldReaders);
  }

  public static VariantValueReader asVariant(PhysicalType type, ParquetValueReader<?> reader) {
    return new ValueAsVariantReader<>(type, reader);
  }

  private abstract static class DelegatingValueReader<S, T> implements ParquetValueReader<T> {
    private final ParquetValueReader<S> reader;

    private DelegatingValueReader(ParquetValueReader<S> reader) {
      this.reader = reader;
    }

    protected S readFromDelegate(S reuse) {
      return reader.read(reuse);
    }

    @Override
    public TripleIterator<?> column() {
      return reader.column();
    }

    @Override
    public List<TripleIterator<?>> columns() {
      return reader.columns();
    }

    @Override
    public void setPageSource(PageReadStore pageStore) {
      reader.setPageSource(pageStore);
    }
  }

  private static ByteBuffer readBinary(ColumnIterator<?> column) {
    ByteBuffer data = column.nextBinary().toByteBuffer();
    byte[] array = new byte[data.remaining()];
    data.get(array, 0, data.remaining());
    return ByteBuffer.wrap(array).order(ByteOrder.LITTLE_ENDIAN);
  }

  private static class VariantMetadataReader extends PrimitiveReader<VariantMetadata> {
    private VariantMetadataReader(ColumnDescriptor desc) {
      super(desc);
    }

    @Override
    public VariantMetadata read(VariantMetadata reuse) {
      return Variants.metadata(readBinary(column));
    }
  }

  private static class SerializedVariantReader extends PrimitiveReader<VariantValue>
      implements VariantValueReader {
    private SerializedVariantReader(ColumnDescriptor desc) {
      super(desc);
    }

    @Override
    public VariantValue read(VariantMetadata metadata) {
      return Variants.value(metadata, readBinary(column));
    }
  }

  private static class ValueAsVariantReader<V> extends DelegatingValueReader<V, VariantValue>
      implements VariantValueReader {
    private final PhysicalType type;

    private ValueAsVariantReader(PhysicalType type, ParquetValueReader<V> reader) {
      super(reader);
      this.type = type;
    }

    @Override
    public VariantValue read(VariantMetadata ignored) {
      return Variants.of(type, readFromDelegate(null));
    }
  }

  /**
   * A Variant reader that combines value and typed_value columns from Parquet.
   *
   * <p>This reader does not handle merging partially shredded objects. To handle shredded objects,
   * use {@link ShreddedObjectReader}.
   */
  private static class ShreddedVariantReader implements VariantValueReader {
    private final int valueDefinitionLevel;
    private final VariantValueReader valueReader;
    private final int typeDefinitionLevel;
    private final VariantValueReader typedReader;
    private final TripleIterator<?> column;
    private final List<TripleIterator<?>> children;

    private ShreddedVariantReader(
        int valueDefinitionLevel,
        VariantValueReader valueReader,
        int typeDefinitionLevel,
        VariantValueReader typedReader) {
      this.valueDefinitionLevel = valueDefinitionLevel;
      this.valueReader = valueReader;
      this.typeDefinitionLevel = typeDefinitionLevel;
      this.typedReader = typedReader;
      this.column = valueReader != null ? valueReader.column() : typedReader.column();
      this.children = children(valueReader, typedReader);
    }

    @Override
    public VariantValue read(VariantMetadata metadata) {
      VariantValue value = ParquetVariantReaders.read(metadata, valueReader, valueDefinitionLevel);
      VariantValue typed = ParquetVariantReaders.read(metadata, typedReader, typeDefinitionLevel);

      if (typed != null) {
        Preconditions.checkArgument(
            value == null,
            "Invalid variant, conflicting value and typed_value: value=%s typed_value=%s",
            value,
            typed);
        return typed;
      }

      return value;
    }

    @Override
    public TripleIterator<?> column() {
      return column;
    }

    @Override
    public List<TripleIterator<?>> columns() {
      return children;
    }

    @Override
    public void setPageSource(PageReadStore pageStore) {
      if (valueReader != null) {
        valueReader.setPageSource(pageStore);
      }

      if (typedReader != null) {
        typedReader.setPageSource(pageStore);
      }
    }
  }

  /**
   * A Variant reader that combines value and partially shredded object columns.
   *
   * <p>This reader handles partially shredded objects. For shredded values, use {@link
   * ShreddedVariantReader} instead.
   */
  private static class ShreddedObjectReader implements VariantValueReader {
    private final int valueDefinitionLevel;
    private final VariantValueReader valueReader;
    private final int fieldsDefinitionLevel;
    private final String[] fieldNames;
    private final VariantValueReader[] fieldReaders;
    private final TripleIterator<?> valueColumn;
    private final TripleIterator<?> fieldColumn;
    private final List<TripleIterator<?>> children;

    private ShreddedObjectReader(
        int valueDefinitionLevel,
        VariantValueReader valueReader,
        int fieldsDefinitionLevel,
        List<String> fieldNames,
        List<VariantValueReader> fieldReaders) {
      this.valueDefinitionLevel = valueDefinitionLevel;
      this.valueReader = valueReader;
      this.fieldsDefinitionLevel = fieldsDefinitionLevel;
      this.fieldNames = fieldNames.toArray(String[]::new);
      this.fieldReaders = fieldReaders.toArray(VariantValueReader[]::new);
      this.fieldColumn = this.fieldReaders[0].column();
      this.valueColumn = valueReader != null ? valueReader.column() : fieldColumn;
      this.children = children(Iterables.concat(Arrays.asList(valueReader), fieldReaders));
    }

    @Override
    public VariantValue read(VariantMetadata metadata) {
      boolean isObject = fieldColumn.currentDefinitionLevel() > fieldsDefinitionLevel;
      VariantValue value = ParquetVariantReaders.read(metadata, valueReader, valueDefinitionLevel);

      if (isObject) {
        ShreddedObject object;
        if (value == MISSING) {
          object = Variants.object(metadata);
        } else {
          Preconditions.checkArgument(
              value.type() == PhysicalType.OBJECT,
              "Invalid variant, non-object value with shredded fields: %s",
              value);
          object = Variants.object(metadata, (VariantObject) value);
        }

        for (int i = 0; i < fieldReaders.length; i += 1) {
          // each field is a ShreddedVariantReader or ShreddedObjectReader that handles DL
          String name = fieldNames[i];
          VariantValue fieldValue = fieldReaders[i].read(metadata);
          if (fieldValue == MISSING) {
            object.remove(name);
          } else {
            object.put(name, fieldValue);
          }
        }

        return object;
      }

      // for non-objects, advance the field iterators
      for (VariantValueReader reader : fieldReaders) {
        for (TripleIterator<?> child : reader.columns()) {
          child.nextNull();
        }
      }

      return value;
    }

    @Override
    public TripleIterator<?> column() {
      return valueColumn;
    }

    @Override
    public List<TripleIterator<?>> columns() {
      return children;
    }

    @Override
    public void setPageSource(PageReadStore pageStore) {
      if (valueReader != null) {
        valueReader.setPageSource(pageStore);
      }

      for (VariantValueReader reader : fieldReaders) {
        reader.setPageSource(pageStore);
      }
    }
  }

  private static class VariantReader implements ParquetValueReader<Variant> {
    private final ParquetValueReader<VariantMetadata> metadataReader;
    private final VariantValueReader valueReader;
    private final TripleIterator<?> column;
    private final List<TripleIterator<?>> children;

    private VariantReader(
        ParquetValueReader<VariantMetadata> metadataReader, VariantValueReader valueReader) {
      this.metadataReader = metadataReader;
      this.valueReader = valueReader;
      // metadata is always non-null so its column can be used for the variant
      this.column = metadataReader.column();
      this.children = children(metadataReader, valueReader);
    }

    @Override
    public Variant read(Variant ignored) {
      VariantMetadata metadata = metadataReader.read(null);
      VariantValue value = valueReader.read(metadata);
      if (value == MISSING) {
        return new Variant() {
          @Override
          public VariantMetadata metadata() {
            return metadata;
          }

          @Override
          public VariantValue value() {
            return Variants.ofNull();
          }
        };
      }

      return new Variant() {
        @Override
        public VariantMetadata metadata() {
          return metadata;
        }

        @Override
        public VariantValue value() {
          return value;
        }
      };
    }

    @Override
    public TripleIterator<?> column() {
      return column;
    }

    @Override
    public List<TripleIterator<?>> columns() {
      return children;
    }

    @Override
    public void setPageSource(PageReadStore pageStore) {
      metadataReader.setPageSource(pageStore);
      valueReader.setPageSource(pageStore);
    }
  }

  private static VariantValue read(
      VariantMetadata metadata, VariantValueReader reader, int definitionLevel) {
    if (reader != null) {
      if (reader.column().currentDefinitionLevel() > definitionLevel) {
        return reader.read(metadata);
      }

      for (TripleIterator<?> child : reader.columns()) {
        child.nextNull();
      }
    }

    return MISSING;
  }

  private static List<TripleIterator<?>> children(ParquetValueReader<?>... readers) {
    return children(Arrays.asList(readers));
  }

  private static List<TripleIterator<?>> children(Iterable<ParquetValueReader<?>> readers) {
    return ImmutableList.copyOf(
        Iterables.concat(
            Iterables.transform(
                Streams.of(readers).filter(Objects::nonNull).collect(Collectors.toList()),
                ParquetValueReader::columns)));
  }
}
