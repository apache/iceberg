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
package org.apache.iceberg;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.StringJoiner;
import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.util.Utf8;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

public class PartitionData
    implements IndexedRecord, StructLike, SpecificData.SchemaConstructable, Serializable {

  /**
   * The Avro schema for a partition type is a pure function of that type, so it's memoized here
   * once per type rather than left to whichever {@link PartitionData} constructor happens to run
   * first. {@link #equals(Object)}/{@link #hashCode()} ignore {@link #schema}, so without this,
   * value-equal instances built from unrelated templates (different scans, different manifests)
   * could carry different, merely structurally-equivalent {@code Schema} objects, and {@link
   * #PARTITION_DATA_CACHE} would non-deterministically pick whichever one was cached first.
   * Resolving every constructor through this single cache instead means any two instances for the
   * same partition type are guaranteed to share one {@code Schema} instance - which {@code
   * PartitionsTable} relies on - independent of caching order.
   *
   * <p>Weak values let an entry be collected once nothing - no live {@link PartitionData}, no other
   * caller - still references that {@code Schema}, the same cleanup story as {@link
   * #PARTITION_DATA_CACHE} below, and the same weak-values pattern {@link SchemaParser} and {@link
   * PartitionSpecParser} already use to cache their own parsed results.
   *
   * <p>Declared before {@link #EMPTY} since its initializer constructs a {@link PartitionData} that
   * reads from this cache.
   */
  private static final Cache<Types.StructType, Schema> SCHEMA_CACHE =
      Caffeine.newBuilder().weakValues().build();

  static final PartitionData EMPTY =
      new PartitionData(Types.StructType.of()) {
        @Override
        public PartitionData copy() {
          return this;
        }
      };

  /**
   * Tables with a small number of distinct partition values can produce a very large number of
   * {@link PartitionData} copies while planning (one per manifest entry). Caching collapses copies
   * that are equal (same partition spec and values) into a single shared instance so that manifest
   * entries sharing a partition don't each pin their own duplicate {@code Object[]}. Weak values
   * mean an entry is evicted automatically once nothing references it anymore, the same pattern
   * {@link #SCHEMA_CACHE} above and {@link SchemaParser}/{@link PartitionSpecParser} already use,
   * which keeps this safe for long-lived processes that plan scans across many tables.
   *
   * <p>Only {@link #copy()}/{@link #copyFor(StructLike)} ever populate this cache, and they always
   * do so with a {@link ReadOnly} instance: sharing a single instance across unrelated callers is
   * only safe because that instance can never be mutated afterward, so a bug that would otherwise
   * silently corrupt every other partition sharing it instead fails loudly at the call site.
   */
  private static final Cache<PartitionData, PartitionData> PARTITION_DATA_CACHE =
      Caffeine.newBuilder().weakValues().build();

  static Schema partitionDataSchema(Types.StructType partitionType) {
    return SCHEMA_CACHE.get(
        partitionType, type -> AvroSchemaUtil.convert(type, PartitionData.class.getName()));
  }

  private final Types.StructType partitionType;
  private final int size;
  private final Object[] data;
  private final String stringSchema;
  private transient Schema schema;

  /** Used by Avro reflection to instantiate this class when reading manifest files. */
  PartitionData(Schema schema) {
    this.partitionType = AvroSchemaUtil.convert(schema).asNestedType().asStructType();
    this.size = partitionType.fields().size();
    this.data = new Object[size];
    this.schema = partitionDataSchema(partitionType);
    this.stringSchema = this.schema.toString();
  }

  public PartitionData(Types.StructType partitionType) {
    for (Types.NestedField field : partitionType.fields()) {
      Preconditions.checkArgument(
          field.type().isPrimitiveType(),
          "Partitions cannot contain nested types: %s",
          field.type());
    }

    this.partitionType = partitionType;
    this.size = partitionType.fields().size();
    this.data = new Object[size];
    this.schema = partitionDataSchema(partitionType);
    this.stringSchema = schema.toString();
  }

  /** Copy constructor */
  private PartitionData(PartitionData toCopy) {
    this.partitionType = toCopy.partitionType;
    this.size = toCopy.size;
    this.data = copyData(toCopy.partitionType, toCopy.data);
    this.stringSchema = toCopy.stringSchema;
    this.schema = toCopy.schema;
  }

  /** Copy constructor that inherits the state but replaces the partition values */
  private PartitionData(PartitionData toCopy, StructLike partition) {
    this.partitionType = toCopy.partitionType;
    this.size = toCopy.size;
    this.data = copyData(partitionType, partition);
    this.stringSchema = toCopy.stringSchema;
    this.schema = toCopy.schema;
  }

  public Types.StructType getPartitionType() {
    return partitionType;
  }

  @Override
  public Schema getSchema() {
    if (schema == null) {
      this.schema = new Schema.Parser().parse(stringSchema);
    }
    return schema;
  }

  public Type getType(int pos) {
    return partitionType.fields().get(pos).type();
  }

  public void clear() {
    Arrays.fill(data, null);
  }

  @Override
  public int size() {
    return size;
  }

  @Override
  public <T> T get(int pos, Class<T> javaClass) {
    Object value = get(pos);
    if (value == null || javaClass.isInstance(value)) {
      return javaClass.cast(value);
    }

    throw new IllegalArgumentException(
        String.format(
            "Wrong class, expected %s, but was %s, for object: %s",
            javaClass.getName(), value.getClass().getName(), value));
  }

  @Override
  public Object get(int pos) {
    if (pos >= data.length) {
      return null;
    }

    if (data[pos] instanceof byte[]) {
      byte[] copied = Arrays.copyOf((byte[]) data[pos], ((byte[]) data[pos]).length);
      return ByteBuffer.wrap(copied);
    }

    return data[pos];
  }

  @Override
  public <T> void set(int pos, T value) {
    data[pos] = toInternalValue(value);
  }

  @Override
  public void put(int i, Object v) {
    set(i, v);
  }

  @Override
  public String toString() {
    List<Types.NestedField> fields = partitionType.fields();
    StringJoiner joiner = new StringJoiner(", ", "PartitionData{", "}");
    for (int pos = 0; pos < fields.size(); pos += 1) {
      joiner.add(fields.get(pos).name() + "=" + data[pos]);
    }

    return joiner.toString();
  }

  public PartitionData copy() {
    PartitionData readOnly = new ReadOnly(this);
    return PARTITION_DATA_CACHE.get(readOnly, key -> key);
  }

  public PartitionData copyFor(StructLike partition) {
    PartitionData readOnly = new ReadOnly(this, partition);
    return PARTITION_DATA_CACHE.get(readOnly, key -> key);
  }

  /**
   * The instance type that {@link PartitionData#copy()}/{@link PartitionData#copyFor(StructLike)}
   * hand to {@link PartitionData#PARTITION_DATA_CACHE}. Rejecting mutation here is what makes
   * sharing one instance across unrelated, unaware callers safe: every existing caller only reads a
   * copied partition, but nothing stops a future one from calling {@link #set(int, Object)} by
   * mistake, which - without this - would silently corrupt every other partition that happens to
   * share the same values.
   */
  private static final class ReadOnly extends PartitionData {
    private ReadOnly(PartitionData toCopy) {
      super(toCopy);
    }

    private ReadOnly(PartitionData toCopy, StructLike partition) {
      super(toCopy, partition);
    }

    @Override
    public <T> void set(int pos, T value) {
      throw new UnsupportedOperationException(
          "Cannot modify a PartitionData returned by copy()/copyFor(); it may be shared");
    }

    @Override
    public void clear() {
      throw new UnsupportedOperationException(
          "Cannot modify a PartitionData returned by copy()/copyFor(); it may be shared");
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    } else if (!(o instanceof PartitionData)) {
      return false;
    }

    PartitionData that = (PartitionData) o;
    return partitionType.equals(that.partitionType) && Arrays.deepEquals(data, that.data);
  }

  @Override
  public int hashCode() {
    int result = partitionType.hashCode();
    return 31 * result + Arrays.deepHashCode(data);
  }

  public static Object[] copyData(Types.StructType type, Object[] data) {
    List<Types.NestedField> fields = type.fields();
    Object[] copy = new Object[data.length];
    for (int i = 0; i < data.length; i += 1) {
      if (data[i] == null) {
        copy[i] = null;
      } else {
        Types.NestedField field = fields.get(i);
        switch (field.type().typeId()) {
          case STRUCT:
          case LIST:
          case MAP:
            throw new IllegalArgumentException("Unsupported type in partition data: " + type);
          case BINARY:
          case FIXED:
            byte[] buffer = (byte[]) data[i];
            copy[i] = Arrays.copyOf(buffer, buffer.length);
            break;
          case STRING:
            copy[i] = data[i].toString();
            break;
          default:
            // no need to copy the object
            copy[i] = data[i];
        }
      }
    }

    return copy;
  }

  private static Object[] copyData(Types.StructType type, StructLike partition) {
    List<Types.NestedField> fields = type.fields();
    Object[] data = new Object[fields.size()];

    for (int pos = 0; pos < fields.size(); pos++) {
      Types.NestedField field = fields.get(pos);
      Class<?> javaClass = field.type().typeId().javaClass();
      data[pos] = toInternalValue(partition.get(pos, javaClass));
    }

    return data;
  }

  private static Object toInternalValue(Object value) {
    if (value instanceof Utf8) {
      // Utf8 is not Serializable
      return value.toString();
    } else if (value instanceof ByteBuffer) {
      // ByteBuffer is not Serializable
      ByteBuffer buffer = (ByteBuffer) value;
      byte[] bytes = new byte[buffer.remaining()];
      buffer.duplicate().get(bytes);
      return bytes;
    } else {
      return value;
    }
  }
}
