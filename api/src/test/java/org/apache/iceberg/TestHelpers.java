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

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.serializers.ClosureSerializer;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.invoke.SerializedLambda;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;
import org.apache.iceberg.expressions.BoundPredicate;
import org.apache.iceberg.expressions.BoundSetPredicate;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.ExpressionVisitors;
import org.apache.iceberg.expressions.UnboundPredicate;
import org.apache.iceberg.util.ByteBuffers;
import org.assertj.core.api.Assertions;
import org.junit.Assert;
import org.objenesis.strategy.StdInstantiatorStrategy;

public class TestHelpers {

  private TestHelpers() {}

  /** Wait in a tight check loop until system clock is past {@code timestampMillis} */
  public static long waitUntilAfter(long timestampMillis) {
    long current = System.currentTimeMillis();
    while (current <= timestampMillis) {
      current = System.currentTimeMillis();
    }
    return current;
  }

  public static <T> T assertAndUnwrap(Expression expr, Class<T> expected) {
    Assert.assertTrue(
        "Expression should have expected type: " + expected, expected.isInstance(expr));
    return expected.cast(expr);
  }

  @SuppressWarnings("unchecked")
  public static <T> BoundPredicate<T> assertAndUnwrap(Expression expr) {
    Assert.assertTrue(
        "Expression should be a bound predicate: " + expr, expr instanceof BoundPredicate);
    return (BoundPredicate<T>) expr;
  }

  @SuppressWarnings("unchecked")
  public static <T> BoundSetPredicate<T> assertAndUnwrapBoundSet(Expression expr) {
    Assert.assertTrue(
        "Expression should be a bound set predicate: " + expr, expr instanceof BoundSetPredicate);
    return (BoundSetPredicate<T>) expr;
  }

  @SuppressWarnings("unchecked")
  public static <T> UnboundPredicate<T> assertAndUnwrapUnbound(Expression expr) {
    Assert.assertTrue(
        "Expression should be an unbound predicate: " + expr, expr instanceof UnboundPredicate);
    return (UnboundPredicate<T>) expr;
  }

  public static void assertAllReferencesBound(String message, Expression expr) {
    ExpressionVisitors.visit(expr, new CheckReferencesBound(message));
  }

  @SuppressWarnings("unchecked")
  public static <T> T roundTripSerialize(T type) throws IOException, ClassNotFoundException {
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    try (ObjectOutputStream out = new ObjectOutputStream(bytes)) {
      out.writeObject(type);
    }

    try (ObjectInputStream in =
        new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
      return (T) in.readObject();
    }
  }

  public static void assertSameSchemaList(List<Schema> list1, List<Schema> list2) {
    Assertions.assertThat(list1)
        .as("Should have same number of schemas in both lists")
        .hasSameSizeAs(list2);

    IntStream.range(0, list1.size())
        .forEach(
            index -> {
              Schema schema1 = list1.get(index);
              Schema schema2 = list2.get(index);
              Assert.assertEquals(
                  "Should have matching schema id", schema1.schemaId(), schema2.schemaId());
              Assert.assertEquals(
                  "Should have matching schema struct", schema1.asStruct(), schema2.asStruct());
            });
  }

  public static void assertSerializedMetadata(Table expected, Table actual) {
    Assert.assertEquals("Name must match", expected.name(), actual.name());
    Assert.assertEquals("Location must match", expected.location(), actual.location());
    Assert.assertEquals("Props must match", expected.properties(), actual.properties());
    Assert.assertEquals(
        "Schema must match", expected.schema().asStruct(), actual.schema().asStruct());
    Assert.assertEquals("Spec must match", expected.spec(), actual.spec());
    Assert.assertEquals("Sort order must match", expected.sortOrder(), actual.sortOrder());
  }

  public static void assertSerializedAndLoadedMetadata(Table expected, Table actual) {
    assertSerializedMetadata(expected, actual);
    Assert.assertEquals("Specs must match", expected.specs(), actual.specs());
    Assert.assertEquals("Sort orders must match", expected.sortOrders(), actual.sortOrders());
    Assert.assertEquals(
        "Current snapshot must match", expected.currentSnapshot(), actual.currentSnapshot());
    Assert.assertEquals("Snapshots must match", expected.snapshots(), actual.snapshots());
    Assert.assertEquals("History must match", expected.history(), actual.history());
  }

  public static void assertSameSchemaMap(Map<Integer, Schema> map1, Map<Integer, Schema> map2) {
    Assertions.assertThat(map1)
        .as("Should have same number of schemas in both maps")
        .hasSameSizeAs(map2);

    map1.forEach(
        (schemaId, schema1) -> {
          Schema schema2 = map2.get(schemaId);
          Assert.assertNotNull(
              String.format("Schema ID %s does not exist in map: %s", schemaId, map2), schema2);

          Assert.assertEquals(
              "Should have matching schema id", schema1.schemaId(), schema2.schemaId());
          Assert.assertTrue(
              String.format(
                  "Should be the same schema. Schema 1: %s, schema 2: %s", schema1, schema2),
              schema1.sameSchema(schema2));
        });
  }

  public static class KryoHelpers {
    private KryoHelpers() {}

    @SuppressWarnings("unchecked")
    public static <T> T roundTripSerialize(T obj) throws IOException {
      Kryo kryo = new Kryo();

      // required for avoiding requirement of zero arg constructor
      kryo.setInstantiatorStrategy(
          new Kryo.DefaultInstantiatorStrategy(new StdInstantiatorStrategy()));

      // required for serializing and deserializing $$Lambda$ Anonymous Classes
      kryo.register(SerializedLambda.class);
      kryo.register(ClosureSerializer.Closure.class, new ClosureSerializer());

      ByteArrayOutputStream bytes = new ByteArrayOutputStream();

      try (Output out = new Output(new ObjectOutputStream(bytes))) {
        kryo.writeClassAndObject(out, obj);
      }

      try (Input in =
          new Input(new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray())))) {
        return (T) kryo.readClassAndObject(in);
      }
    }
  }

  private static class CheckReferencesBound extends ExpressionVisitors.ExpressionVisitor<Void> {
    private final String message;

    CheckReferencesBound(String message) {
      this.message = message;
    }

    @Override
    public <T> Void predicate(UnboundPredicate<T> pred) {
      Assert.fail(message + ": Found unbound predicate: " + pred);
      return null;
    }
  }

  /** Implements {@link StructLike#get} for passing data in tests. */
  public static class Row implements StructLike {
    public static Row of(Object... values) {
      return new Row(values);
    }

    private final Object[] values;

    private Row(Object... values) {
      this.values = values;
    }

    @Override
    public int size() {
      return values.length;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T get(int pos, Class<T> javaClass) {
      return javaClass.cast(values[pos]);
    }

    @Override
    public <T> void set(int pos, T value) {
      values[pos] = value;
    }

    @Override
    public boolean equals(Object other) {
      if (this == other) {
        return true;
      }
      if (other == null || getClass() != other.getClass()) {
        return false;
      }

      Row that = (Row) other;

      return Arrays.equals(values, that.values);
    }

    @Override
    public int hashCode() {
      return Arrays.hashCode(values);
    }
  }

  public static class TestFieldSummary implements ManifestFile.PartitionFieldSummary {
    private final boolean containsNull;
    private final Boolean containsNaN;
    private final ByteBuffer lowerBound;
    private final ByteBuffer upperBound;

    public TestFieldSummary(boolean containsNull, ByteBuffer lowerBound, ByteBuffer upperBound) {
      this(containsNull, null, lowerBound, upperBound);
    }

    public TestFieldSummary(
        boolean containsNull, Boolean containsNaN, ByteBuffer lowerBound, ByteBuffer upperBound) {
      this.containsNull = containsNull;
      this.containsNaN = containsNaN;
      this.lowerBound = lowerBound;
      this.upperBound = upperBound;
    }

    @Override
    public boolean containsNull() {
      return containsNull;
    }

    @Override
    public Boolean containsNaN() {
      return containsNaN;
    }

    @Override
    public ByteBuffer lowerBound() {
      return lowerBound;
    }

    @Override
    public ByteBuffer upperBound() {
      return upperBound;
    }

    @Override
    public ManifestFile.PartitionFieldSummary copy() {
      return this;
    }
  }

  public static class TestManifestFile implements ManifestFile {
    private final String path;
    private final long length;
    private final int specId;
    private final ManifestContent content;
    private final Long snapshotId;
    private final Integer addedFiles;
    private final Long addedRows;
    private final Integer existingFiles;
    private final Long existingRows;
    private final Integer deletedFiles;
    private final Long deletedRows;
    private final List<PartitionFieldSummary> partitions;
    private final byte[] keyMetadata;

    public TestManifestFile(
        String path,
        long length,
        int specId,
        Long snapshotId,
        Integer addedFiles,
        Integer existingFiles,
        Integer deletedFiles,
        List<PartitionFieldSummary> partitions,
        ByteBuffer keyMetadata) {
      this.path = path;
      this.length = length;
      this.specId = specId;
      this.content = ManifestContent.DATA;
      this.snapshotId = snapshotId;
      this.addedFiles = addedFiles;
      this.addedRows = null;
      this.existingFiles = existingFiles;
      this.existingRows = null;
      this.deletedFiles = deletedFiles;
      this.deletedRows = null;
      this.partitions = partitions;
      this.keyMetadata = ByteBuffers.toByteArray(keyMetadata);
    }

    public TestManifestFile(
        String path,
        long length,
        int specId,
        ManifestContent content,
        Long snapshotId,
        Integer addedFiles,
        Long addedRows,
        Integer existingFiles,
        Long existingRows,
        Integer deletedFiles,
        Long deletedRows,
        List<PartitionFieldSummary> partitions,
        ByteBuffer keyMetadata) {
      this.path = path;
      this.length = length;
      this.specId = specId;
      this.content = content;
      this.snapshotId = snapshotId;
      this.addedFiles = addedFiles;
      this.addedRows = addedRows;
      this.existingFiles = existingFiles;
      this.existingRows = existingRows;
      this.deletedFiles = deletedFiles;
      this.deletedRows = deletedRows;
      this.partitions = partitions;
      this.keyMetadata = ByteBuffers.toByteArray(keyMetadata);
    }

    @Override
    public String path() {
      return path;
    }

    @Override
    public long length() {
      return length;
    }

    @Override
    public int partitionSpecId() {
      return specId;
    }

    @Override
    public ManifestContent content() {
      return content;
    }

    @Override
    public long sequenceNumber() {
      return 0;
    }

    @Override
    public long minSequenceNumber() {
      return 0;
    }

    @Override
    public Long snapshotId() {
      return snapshotId;
    }

    @Override
    public Integer addedFilesCount() {
      return addedFiles;
    }

    @Override
    public Long addedRowsCount() {
      return addedRows;
    }

    @Override
    public Integer existingFilesCount() {
      return existingFiles;
    }

    @Override
    public Long existingRowsCount() {
      return existingRows;
    }

    @Override
    public Integer deletedFilesCount() {
      return deletedFiles;
    }

    @Override
    public Long deletedRowsCount() {
      return deletedRows;
    }

    @Override
    public List<PartitionFieldSummary> partitions() {
      return partitions;
    }

    @Override
    public ByteBuffer keyMetadata() {
      return keyMetadata == null ? null : ByteBuffer.wrap(keyMetadata);
    }

    @Override
    public ManifestFile copy() {
      return this;
    }
  }

  public static class TestDataFile implements DataFile {
    private final String path;
    private final StructLike partition;
    private final long recordCount;
    private final Map<Integer, Long> valueCounts;
    private final Map<Integer, Long> nullValueCounts;
    private final Map<Integer, Long> nanValueCounts;
    private final Map<Integer, ByteBuffer> lowerBounds;
    private final Map<Integer, ByteBuffer> upperBounds;

    public TestDataFile(String path, StructLike partition, long recordCount) {
      this(path, partition, recordCount, null, null, null, null, null);
    }

    public TestDataFile(
        String path,
        StructLike partition,
        long recordCount,
        Map<Integer, Long> valueCounts,
        Map<Integer, Long> nullValueCounts,
        Map<Integer, Long> nanValueCounts,
        Map<Integer, ByteBuffer> lowerBounds,
        Map<Integer, ByteBuffer> upperBounds) {
      this.path = path;
      this.partition = partition;
      this.recordCount = recordCount;
      this.valueCounts = valueCounts;
      this.nullValueCounts = nullValueCounts;
      this.nanValueCounts = nanValueCounts;
      this.lowerBounds = lowerBounds;
      this.upperBounds = upperBounds;
    }

    @Override
    public Long pos() {
      return null;
    }

    @Override
    public int specId() {
      return 0;
    }

    @Override
    public CharSequence path() {
      return path;
    }

    @Override
    public FileFormat format() {
      return FileFormat.fromFileName(path());
    }

    @Override
    public StructLike partition() {
      return partition;
    }

    @Override
    public long recordCount() {
      return recordCount;
    }

    @Override
    public long fileSizeInBytes() {
      return 0;
    }

    @Override
    public Map<Integer, Long> columnSizes() {
      return null;
    }

    @Override
    public Map<Integer, Long> valueCounts() {
      return valueCounts;
    }

    @Override
    public Map<Integer, Long> nullValueCounts() {
      return nullValueCounts;
    }

    @Override
    public Map<Integer, Long> nanValueCounts() {
      return nanValueCounts;
    }

    @Override
    public Map<Integer, ByteBuffer> lowerBounds() {
      return lowerBounds;
    }

    @Override
    public Map<Integer, ByteBuffer> upperBounds() {
      return upperBounds;
    }

    @Override
    public ByteBuffer keyMetadata() {
      return null;
    }

    @Override
    public DataFile copy() {
      return this;
    }

    @Override
    public DataFile copyWithoutStats() {
      return this;
    }

    @Override
    public List<Long> splitOffsets() {
      return null;
    }
  }
}
