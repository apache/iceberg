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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.expressions.BoundPredicate;
import org.apache.iceberg.expressions.BoundSetPredicate;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.ExpressionVisitors;
import org.apache.iceberg.expressions.UnboundPredicate;
import org.junit.Assert;

public class TestHelpers {

  private TestHelpers() {}

  public static <T> T assertAndUnwrap(Expression expr, Class<T> expected) {
    Assert.assertTrue("Expression should have expected type: " + expected,
        expected.isInstance(expr));
    return expected.cast(expr);
  }

  @SuppressWarnings("unchecked")
  public static <T> BoundPredicate<T> assertAndUnwrap(Expression expr) {
    Assert.assertTrue("Expression should be a bound predicate: " + expr,
        expr instanceof BoundPredicate);
    return (BoundPredicate<T>) expr;
  }

  @SuppressWarnings("unchecked")
  public static <T> BoundSetPredicate<T> assertAndUnwrapBoundSet(Expression expr) {
    Assert.assertTrue("Expression should be a bound set predicate: " + expr,
        expr instanceof BoundSetPredicate);
    return (BoundSetPredicate<T>) expr;
  }

  @SuppressWarnings("unchecked")
  public static <T> UnboundPredicate<T> assertAndUnwrapUnbound(Expression expr) {
    Assert.assertTrue("Expression should be an unbound predicate: " + expr,
        expr instanceof UnboundPredicate);
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

    try (ObjectInputStream in = new ObjectInputStream(
        new ByteArrayInputStream(bytes.toByteArray()))) {
      return (T) in.readObject();
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

  /**
   * Implements {@link StructLike#get} for passing data in tests.
   */
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
      throw new UnsupportedOperationException("Setting values is not supported");
    }
  }

  public static class TestFieldSummary implements ManifestFile.PartitionFieldSummary {
    private final boolean containsNull;
    private final ByteBuffer lowerBound;
    private final ByteBuffer upperBound;

    public TestFieldSummary(boolean containsNull, ByteBuffer lowerBound, ByteBuffer upperBound) {
      this.containsNull = containsNull;
      this.lowerBound = lowerBound;
      this.upperBound = upperBound;
    }

    @Override
    public boolean containsNull() {
      return containsNull;
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
    private final Long snapshotId;
    private final Integer addedFiles;
    private final Integer existingFiles;
    private final Integer deletedFiles;
    private final List<PartitionFieldSummary> partitions;

    public TestManifestFile(String path, long length, int specId, Long snapshotId,
                            Integer addedFiles, Integer existingFiles, Integer deletedFiles,
                            List<PartitionFieldSummary> partitions) {
      this.path = path;
      this.length = length;
      this.specId = specId;
      this.snapshotId = snapshotId;
      this.addedFiles = addedFiles;
      this.existingFiles = existingFiles;
      this.deletedFiles = deletedFiles;
      this.partitions = partitions;
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
    public Long snapshotId() {
      return snapshotId;
    }

    @Override
    public Integer addedFilesCount() {
      return addedFiles;
    }

    @Override
    public Integer existingFilesCount() {
      return existingFiles;
    }

    @Override
    public Integer deletedFilesCount() {
      return deletedFiles;
    }

    @Override
    public List<PartitionFieldSummary> partitions() {
      return partitions;
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
    private final Map<Integer, ByteBuffer> lowerBounds;
    private final Map<Integer, ByteBuffer> upperBounds;

    public TestDataFile(String path, StructLike partition, long recordCount) {
      this(path, partition, recordCount, null, null, null, null);
    }

    public TestDataFile(String path, StructLike partition, long recordCount,
                        Map<Integer, Long> valueCounts,
                        Map<Integer, Long> nullValueCounts,
                        Map<Integer, ByteBuffer> lowerBounds,
                        Map<Integer, ByteBuffer> upperBounds) {
      this.path = path;
      this.partition = partition;
      this.recordCount = recordCount;
      this.valueCounts = valueCounts;
      this.nullValueCounts = nullValueCounts;
      this.lowerBounds = lowerBounds;
      this.upperBounds = upperBounds;
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
    public Integer fileOrdinal() {
      return null;
    }

    @Override
    public List<Integer> sortColumns() {
      return null;
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
