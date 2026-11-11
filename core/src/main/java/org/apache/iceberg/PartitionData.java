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
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Interner;
import org.apache.iceberg.relocated.com.google.common.collect.Interners;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

public class PartitionData
    implements IndexedRecord, StructLike, SpecificData.SchemaConstructable, Serializable {

  /**
   * The Avro schema for a partition type is a pure function of that type, so it's memoized here
   * once per type rather than left to whichever {@link PartitionData} constructor happens to run
   * first. {@link #equals(Object)}/{@link #hashCode()} ignore {@link #schema}, so without this,
   * value-equal instances built from unrelated templates (different scans, different manifests)
   * could carry different, merely structurally-equivalent {@code Schema} objects. Resolving every
   * constructor through this single cache instead means any two instances for the same partition
   * type are guaranteed to share one {@code Schema} instance - which {@code PartitionsTable} relies
   * on.
   *
   * <p>Weak values let an entry be collected once nothing - no live {@link PartitionData}, no other
   * caller - still references that {@code Schema}, the same pattern {@link SchemaParser} and {@link
   * PartitionSpecParser} already use to cache their own parsed results. This works because the key
   * ({@code Types.StructType}) and value ({@code Schema}) are different objects - see {@link
   * #VALUES_INTERNER} below for why a self-referencing cache needs weak keys instead.
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
   * {@link PartitionData} copies while planning (one per manifest entry). Each copy only needs its
   * own {@link PartitionData} wrapper - a few fields, cheap to allocate per file - but the backing
   * {@code Object[]} of values is what actually dominates memory for high-file, low-cardinality
   * tables, so only {@link Values} (type + backing array) is interned here, not the whole {@link
   * PartitionData}. {@link PartitionData} instances are never shared across callers: {@link
   * #copy()} and {@link #copyFor(StructLike)} always return a fresh wrapper, so nothing about
   * object identity, {@link #schema} resolution, or mutability changes from before this cache
   * existed - only the {@code Object[]} two value-equal copies point at is now the same instance.
   *
   * <p>This is a self-referencing cache - the interned value IS the key - so a plain {@code
   * Cache<Values, Values>} with Caffeine's {@code weakValues()} would not actually clean up: the
   * key slot holds a strong reference to the very same object the value slot only weakly
   * references, so nothing ever becomes collectible (verified directly - see
   * TestPartitionData#copyIsEvictedOnceUnreferenced). A Guava {@code Interner} is the purpose-built
   * tool for this case instead - it weakly references the interned instance itself - so an entry is
   * evicted once nothing - no live {@link PartitionData}, no other caller - still references that
   * {@link Values} instance, which keeps this safe for long-lived processes that plan scans across
   * many tables.
   *
   * <p>{@link Values} itself is never exposed outside {@link #copy()}/{@link #copyFor(StructLike)};
   * once interned, its backing array must never be mutated, since it may be shared by unrelated
   * callers. {@link #set(int, Object)}/{@link #clear()} reject mutation on any {@link
   * PartitionData} built from an interned {@link Values} so a bug that would otherwise silently
   * corrupt every other partition sharing it instead fails loudly at the call site.
   */
  private static final Interner<Values> VALUES_INTERNER = Interners.newWeakInterner();

  static Schema partitionDataSchema(Types.StructType partitionType) {
    return SCHEMA_CACHE.get(
        partitionType, type -> AvroSchemaUtil.convert(type, PartitionData.class.getName()));
  }

  private final Types.StructType partitionType;
  private final int size;
  private final Values values;
  private final boolean valuesShared;
  private final String stringSchema;
  private transient Schema schema;

  /** Used by Avro reflection to instantiate this class when reading manifest files. */
  PartitionData(Schema schema) {
    this.partitionType = AvroSchemaUtil.convert(schema).asNestedType().asStructType();
    this.size = partitionType.fields().size();
    this.values = new Values(partitionType, new Object[size]);
    this.valuesShared = false;
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
    this.values = new Values(partitionType, new Object[size]);
    this.valuesShared = false;
    this.schema = partitionDataSchema(partitionType);
    this.stringSchema = schema.toString();
  }

  /** Copy constructor */
  private PartitionData(PartitionData toCopy) {
    this.partitionType = toCopy.partitionType;
    this.size = toCopy.size;
    this.values =
        VALUES_INTERNER.intern(
            new Values(toCopy.partitionType, copyData(toCopy.partitionType, toCopy.rawData())));
    this.valuesShared = true;
    this.stringSchema = toCopy.stringSchema;
    this.schema = toCopy.schema;
  }

  /** Copy constructor that inherits the state but replaces the partition values */
  private PartitionData(PartitionData toCopy, StructLike partition) {
    this.partitionType = toCopy.partitionType;
    this.size = toCopy.size;
    this.values =
        VALUES_INTERNER.intern(new Values(partitionType, copyData(partitionType, partition)));
    this.valuesShared = true;
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
    checkMutable();
    Arrays.fill(values.data, null);
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
    Object[] data = values.data;
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
    checkMutable();
    values.data[pos] = toInternalValue(value);
  }

  @Override
  public void put(int i, Object v) {
    set(i, v);
  }

  private void checkMutable() {
    if (valuesShared) {
      throw new UnsupportedOperationException(
          "Cannot modify a PartitionData returned by copy()/copyFor(); its values may be shared");
    }
  }

  @VisibleForTesting
  Object[] rawData() {
    return values.data;
  }

  @Override
  public String toString() {
    List<Types.NestedField> fields = partitionType.fields();
    StringJoiner joiner = new StringJoiner(", ", "PartitionData{", "}");
    Object[] data = values.data;
    for (int pos = 0; pos < fields.size(); pos += 1) {
      joiner.add(fields.get(pos).name() + "=" + data[pos]);
    }

    return joiner.toString();
  }

  public PartitionData copy() {
    return new PartitionData(this);
  }

  public PartitionData copyFor(StructLike partition) {
    return new PartitionData(this, partition);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    } else if (!(o instanceof PartitionData)) {
      return false;
    }

    PartitionData that = (PartitionData) o;
    return partitionType.equals(that.partitionType)
        && Arrays.deepEquals(values.data, that.values.data);
  }

  @Override
  public int hashCode() {
    int result = partitionType.hashCode();
    return 31 * result + Arrays.deepHashCode(values.data);
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

  /**
   * Wraps the partition type and backing value array so they have proper, structural {@link
   * #equals(Object)}/{@link #hashCode()} - Java arrays only support identity equality, so {@code
   * Object[]} can't be interned directly. This is the only thing {@link #VALUES_INTERNER} shares
   * across unrelated {@link PartitionData} instances.
   */
  private static final class Values implements Serializable {
    private final Types.StructType partitionType;
    private final Object[] data;

    private Values(Types.StructType partitionType, Object[] data) {
      this.partitionType = partitionType;
      this.data = data;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      } else if (!(o instanceof Values)) {
        return false;
      }

      Values that = (Values) o;
      return partitionType.equals(that.partitionType) && Arrays.deepEquals(data, that.data);
    }

    @Override
    public int hashCode() {
      return 31 * partitionType.hashCode() + Arrays.deepHashCode(data);
    }
  }
}
