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

package com.netflix.iceberg;

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
    @SuppressWarnings("unchecked")
    public <T> T get(int pos, Class<T> javaClass) {
      return (T) values[pos];
    }

    @Override
    public <T> void set(int pos, T value) {
      throw new UnsupportedOperationException("Setting values is not supported");
    }
  }
}
