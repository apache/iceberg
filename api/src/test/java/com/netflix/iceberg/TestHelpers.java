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

package com.netflix.iceberg;

import com.netflix.iceberg.encryption.EncryptionTypes;
import com.netflix.iceberg.expressions.BoundPredicate;
import com.netflix.iceberg.expressions.Expression;
import com.netflix.iceberg.expressions.ExpressionVisitors;
import com.netflix.iceberg.expressions.UnboundPredicate;
import org.junit.Assert;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

public class TestHelpers {
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

    public CheckReferencesBound(String message) {
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

  /**
   * A convenience method to avoid a large number of @Test(expected=...) tests
   * @param message A String message to describe this assertion
   * @param expected An Exception class that the Runnable should throw
   * @param containedInMessage A String that should be contained by the thrown
   *                           exception's message
   * @param callable A Callable that is expected to throw the exception
   */
  public static void assertThrows(String message,
                                  Class<? extends Exception> expected,
                                  String containedInMessage,
                                  Callable callable) {
    try {
      callable.call();
      Assert.fail("No exception was thrown (" + message + "), expected: " +
          expected.getName());
    } catch (Exception actual) {
      handleException(message, expected, containedInMessage, actual);
    }
  }

  /**
   * A convenience method to avoid a large number of @Test(expected=...) tests
   * @param message A String message to describe this assertion
   * @param expected An Exception class that the Runnable should throw
   * @param containedInMessage A String that should be contained by the thrown
   *                           exception's message
   * @param runnable A Runnable that is expected to throw the runtime exception
   */
  public static void assertThrows(String message,
                                  Class<? extends Exception> expected,
                                  String containedInMessage,
                                  Runnable runnable) {
    try {
      runnable.run();
      Assert.fail("No exception was thrown (" + message + "), expected: " +
          expected.getName());
    } catch (Exception actual) {
      handleException(message, expected, containedInMessage, actual);
    }
  }

  private static void handleException(String message,
                                      Class<? extends Exception> expected,
                                      String containedInMessage,
                                      Exception actual) {
    try {
      Assert.assertEquals(message, expected, actual.getClass());
      Assert.assertTrue(
          "Expected exception message (" + containedInMessage + ") missing: " +
              actual.getMessage(),
          actual.getMessage().contains(containedInMessage)
      );
    } catch (AssertionError e) {
      e.addSuppressed(actual);
      throw e;
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
    private final EncryptionTypes.FileEncryptionMetadata encryptionMetadata;

    public TestDataFile(String path, StructLike partition, long recordCount) {
      this(
          path,
          partition,
          recordCount,
          null,
          null,
          null,
          null,
          null);
    }

    public TestDataFile(String path, StructLike partition, long recordCount,
                        Map<Integer, Long> valueCounts,
                        Map<Integer, Long> nullValueCounts,
                        Map<Integer, ByteBuffer> lowerBounds,
                        Map<Integer, ByteBuffer> upperBounds,
                        EncryptionTypes.FileEncryptionMetadata encryptionMetadata) {
      this.path = path;
      this.partition = partition;
      this.recordCount = recordCount;
      this.valueCounts = valueCounts;
      this.nullValueCounts = nullValueCounts;
      this.lowerBounds = lowerBounds;
      this.upperBounds = upperBounds;
      this.encryptionMetadata = encryptionMetadata;
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
    public long blockSizeInBytes() {
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
    public EncryptionTypes.FileEncryptionMetadata fileEncryptionMetadata() {
      return encryptionMetadata;
    }

    @Override
    public DataFile copy() {
      return this;
    }
  }

  public static final class TestKeyDescription implements EncryptionTypes.KeyDescription {
    private final String keyName;
    private final int keyVersion;

    public TestKeyDescription(String keyName, int keyVersion) {
      this.keyName = keyName;
      this.keyVersion = keyVersion;
    }

    @Override
    public String keyName() {
      return keyName;
    }

    @Override
    public int keyVersion() {
      return keyVersion;
    }

    @Override
    public EncryptionTypes.KeyDescription copy() {
      return new TestKeyDescription(keyName, keyVersion);
    }
  }

  public static final class TestFileEncryptionMetadata implements EncryptionTypes.FileEncryptionMetadata {

    private final EncryptionTypes.KeyDescription keyDescription;
    private final String keyAlgorithm;
    private final ByteBuffer iv;
    private final ByteBuffer encryptedKey;

    public TestFileEncryptionMetadata(
        EncryptionTypes.KeyDescription keyDescription,
        String keyAlgorithm,
        byte[] iv,
        byte[] encryptedKey) {
      this.keyDescription = keyDescription;
      this.keyAlgorithm = keyAlgorithm;
      this.iv = ByteBuffer.wrap(iv);
      this.encryptedKey = ByteBuffer.wrap(encryptedKey);
    }

    @Override
    public EncryptionTypes.KeyDescription keyDescription() {
      return keyDescription;
    }

    @Override
    public String keyAlgorithm() {
      return keyAlgorithm;
    }

    @Override
    public ByteBuffer iv() {
      return iv;
    }

    @Override
    public ByteBuffer encryptedKey() {
      return encryptedKey;
    }

    @Override
    public EncryptionTypes.FileEncryptionMetadata copy() {
      return new TestFileEncryptionMetadata(
          keyDescription,
          keyAlgorithm,
          iv.array(),
          encryptedKey.array());
    }
  }
}
