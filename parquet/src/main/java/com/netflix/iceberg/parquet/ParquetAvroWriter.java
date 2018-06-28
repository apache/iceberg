/*
 * Copyright 2017 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.netflix.iceberg.parquet;

import com.google.common.collect.Lists;
import com.netflix.iceberg.parquet.ParquetValueWriters.BytesWriter;
import com.netflix.iceberg.parquet.ParquetValueWriters.FixedDecimalWriter;
import com.netflix.iceberg.parquet.ParquetValueWriters.FixedWriter;
import com.netflix.iceberg.parquet.ParquetValueWriters.IntegerDecimalWriter;
import com.netflix.iceberg.parquet.ParquetValueWriters.LongDecimalWriter;
import com.netflix.iceberg.parquet.ParquetValueWriters.RepeatedKeyValueWriter;
import com.netflix.iceberg.parquet.ParquetValueWriters.RepeatedWriter;
import com.netflix.iceberg.parquet.ParquetValueWriters.StringWriter;
import com.netflix.iceberg.parquet.ParquetValueWriters.StructWriter;
import com.netflix.iceberg.parquet.ParquetValueWriters.UnboxedWriter;
import org.apache.avro.generic.IndexedRecord;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.ColumnWriteStore;
import org.apache.parquet.schema.DecimalMetadata;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static com.netflix.iceberg.parquet.ParquetValueWriters.option;

public class ParquetAvroWriter<T> {

  private final ParquetValueWriter<T> valueWriter;
  private ColumnWriteStore writeStore = null;

  ParquetAvroWriter(MessageType type) {
    this.valueWriter = buildWriter(type);
  }

  public void write(T value) {
    valueWriter.write(0, value);
    writeStore.endRecord();
  }

  public void setColumnStore(ColumnWriteStore store) {
    this.writeStore = store;
    this.valueWriter.setColumnStore(store);
  }

  @SuppressWarnings("unchecked")
  private static <T> ParquetValueWriter<T> buildWriter(MessageType type) {
    return (ParquetValueWriter<T>) ParquetTypeVisitor.visit(type, new WriteBuilder(type));
  }

  private static class WriteBuilder extends ParquetTypeVisitor<ParquetValueWriter<?>> {
    private final MessageType type;

    WriteBuilder(MessageType type) {
      this.type = type;
    }

    @Override
    public ParquetValueWriter<?> message(MessageType message,
                                         List<ParquetValueWriter<?>> fieldWriters) {
      return struct(message.asGroupType(), fieldWriters);
    }

    @Override
    public ParquetValueWriter<?> struct(GroupType struct,
                                        List<ParquetValueWriter<?>> fieldWriters) {
      List<Type> fields = struct.getFields();
      List<ParquetValueWriter<?>> writers = Lists.newArrayListWithExpectedSize(fieldWriters.size());
      for (int i = 0; i < fields.size(); i += 1) {
        Type fieldType = struct.getType(i);
        int fieldD = type.getMaxDefinitionLevel(path(fieldType.getName()));
        writers.add(option(fieldType, fieldD, fieldWriters.get(i)));
      }

      return new RecordWriter(writers);
    }

    @Override
    public ParquetValueWriter<?> list(GroupType array, ParquetValueWriter<?> elementWriter) {
      GroupType repeated = array.getFields().get(0).asGroupType();
      String[] repeatedPath = currentPath();

      int repeatedD = type.getMaxDefinitionLevel(repeatedPath);
      int repeatedR = type.getMaxRepetitionLevel(repeatedPath);

      org.apache.parquet.schema.Type elementType = repeated.getType(0);
      int elementD = type.getMaxDefinitionLevel(path(elementType.getName()));

      return lists(repeatedD, repeatedR, option(elementType, elementD, elementWriter));
    }

    @Override
    public ParquetValueWriter<?> map(GroupType map,
                                     ParquetValueWriter<?> keyWriter,
                                     ParquetValueWriter<?> valueWriter) {
      GroupType repeatedKeyValue = map.getFields().get(0).asGroupType();
      String[] repeatedPath = currentPath();

      int repeatedD = type.getMaxDefinitionLevel(repeatedPath);
      int repeatedR = type.getMaxRepetitionLevel(repeatedPath);

      org.apache.parquet.schema.Type keyType = repeatedKeyValue.getType(0);
      int keyD = type.getMaxDefinitionLevel(path(keyType.getName()));
      org.apache.parquet.schema.Type valueType = repeatedKeyValue.getType(1);
      int valueD = type.getMaxDefinitionLevel(path(valueType.getName()));

      return new MapWriter<>(repeatedD, repeatedR,
          option(keyType, keyD, keyWriter), option(valueType, valueD, valueWriter));
    }

    @Override
    public ParquetValueWriter<?> primitive(PrimitiveType primitive) {
      ColumnDescriptor desc = type.getColumnDescription(currentPath());

      if (primitive.getOriginalType() != null) {
        switch (primitive.getOriginalType()) {
          case ENUM:
          case JSON:
          case UTF8:
            return new StringWriter(desc);
          case DATE:
          case INT_8:
          case INT_16:
          case INT_32:
          case INT_64:
          case TIME_MICROS:
          case TIMESTAMP_MICROS:
            return new UnboxedWriter<>(desc);
          case DECIMAL:
            DecimalMetadata decimal = primitive.getDecimalMetadata();
            switch (primitive.getPrimitiveTypeName()) {
              case INT32:
                return new IntegerDecimalWriter(desc, decimal.getPrecision(), decimal.getScale());
              case INT64:
                return new LongDecimalWriter(desc, decimal.getPrecision(), decimal.getScale());
              case BINARY:
              case FIXED_LEN_BYTE_ARRAY:
                return new FixedDecimalWriter(desc, decimal.getPrecision(), decimal.getScale());
              default:
                throw new UnsupportedOperationException(
                    "Unsupported base type for decimal: " + primitive.getPrimitiveTypeName());
            }
          case BSON:
            return new BytesWriter(desc);
          default:
            throw new UnsupportedOperationException(
                "Unsupported logical type: " + primitive.getOriginalType());
        }
      }

      switch (primitive.getPrimitiveTypeName()) {
        case FIXED_LEN_BYTE_ARRAY:
          return new FixedWriter(desc);
        case BINARY:
          return new BytesWriter(desc);
        case BOOLEAN:
        case INT32:
        case INT64:
        case FLOAT:
        case DOUBLE:
          return new UnboxedWriter<>(desc);
        default:
          throw new UnsupportedOperationException("Unsupported type: " + primitive);
      }
    }

    private String[] currentPath() {
      String[] path = new String[fieldNames.size()];
      if (!fieldNames.isEmpty()) {
        Iterator<String> iter = fieldNames.descendingIterator();
        for (int i = 0; iter.hasNext(); i += 1) {
          path[i] = iter.next();
        }
      }

      return path;
    }

    private String[] path(String name) {
      String[] path = new String[fieldNames.size() + 1];
      path[fieldNames.size()] = name;

      if (!fieldNames.isEmpty()) {
        Iterator<String> iter = fieldNames.descendingIterator();
        for (int i = 0; iter.hasNext(); i += 1) {
          path[i] = iter.next();
        }
      }

      return path;
    }
  }

  private static <E> ListWriter<E> lists(int dl, int rl, ParquetValueWriter<E> writer) {
    return new ListWriter<>(dl, rl, writer);
  }

  private static class ListWriter<E> extends RepeatedWriter<List<E>, E> {
    ListWriter(int definitionLevel, int repetitionLevel, ParquetValueWriter<E> writer) {
      super(definitionLevel, repetitionLevel, writer);
    }

    @Override
    protected Iterator<E> elements(List<E> list) {
      return list.iterator();
    }
  }

  private static class MapWriter<K, V> extends RepeatedKeyValueWriter<Map<K, V>, K, V> {
    MapWriter(int definitionLevel, int repetitionLevel,
              ParquetValueWriter<K> keyWriter, ParquetValueWriter<V> valueWriter) {
      super(definitionLevel, repetitionLevel, keyWriter, valueWriter);
    }

    @Override
    protected Iterator<Map.Entry<K, V>> pairs(Map<K, V> map) {
      return map.entrySet().iterator();
    }
  }

  private static class RecordWriter extends StructWriter<IndexedRecord> {
    RecordWriter(List<ParquetValueWriter<?>> writers) {
      super(writers);
    }

    @Override
    protected Object get(IndexedRecord struct, int index) {
      return struct.get(index);
    }
  }
}
