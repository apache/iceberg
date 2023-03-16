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

import java.util.concurrent.Callable;
import org.apache.avro.AvroRuntimeException;
import org.apache.avro.generic.GenericRecord;
import org.assertj.core.api.AbstractThrowableAssert;
import org.assertj.core.api.Assertions;
import org.assertj.core.api.ThrowableAssert;

/**
 * This class is deprecated. Please use {@link
 * Assertions#assertThatThrownBy(ThrowableAssert.ThrowingCallable)} directly as shown below:
 *
 * <pre>
 * Assertions.assertThatThrownBy(() -> throwingCallable)
 *    .isInstanceOf(ExpectedException.class)
 *    .hasMessage(expectedErrorMsg)
 * </pre>
 *
 * @deprecated Use {@link Assertions#assertThatThrownBy(ThrowableAssert.ThrowingCallable)} directly
 *     as it provides a more fluent way of asserting on exceptions.
 */
@Deprecated
public class AssertHelpers {

  private AssertHelpers() {}

  /**
   * A convenience method to avoid a large number of @Test(expected=...) tests
   *
   * @param message A String message to describe this assertion
   * @param expected An Exception class that the Runnable should throw
   * @param containedInMessage A String that should be contained by the thrown exception's message
   * @param callable A Callable that is expected to throw the exception
   * @deprecated Use {@link Assertions#assertThatThrownBy(ThrowableAssert.ThrowingCallable)}
   *     directly as it provides a more fluent way of asserting on exceptions.
   */
  @Deprecated
  public static void assertThrows(
      String message,
      Class<? extends Exception> expected,
      String containedInMessage,
      Callable callable) {
    AbstractThrowableAssert<?, ? extends Throwable> check =
        Assertions.assertThatThrownBy(callable::call).as(message).isInstanceOf(expected);
    if (null != containedInMessage) {
      check.hasMessageContaining(containedInMessage);
    }
  }

  /**
   * A convenience method to avoid a large number of @Test(expected=...) tests
   *
   * @param message A String message to describe this assertion
   * @param expected An Exception class that the Runnable should throw
   * @param containedInMessage A String that should be contained by the thrown exception's message
   * @param runnable A Runnable that is expected to throw the runtime exception
   * @deprecated Use {@link Assertions#assertThatThrownBy(ThrowableAssert.ThrowingCallable)}
   *     directly as it provides a more fluent way of asserting on exceptions.
   */
  @Deprecated
  public static void assertThrows(
      String message,
      Class<? extends Exception> expected,
      String containedInMessage,
      Runnable runnable) {
    AbstractThrowableAssert<?, ? extends Throwable> check =
        Assertions.assertThatThrownBy(runnable::run).as(message).isInstanceOf(expected);
    if (null != containedInMessage) {
      check.hasMessageContaining(containedInMessage);
    }
  }

  /**
   * A convenience method to avoid a large number of @Test(expected=...) tests
   *
   * @param message A String message to describe this assertion
   * @param expected An Exception class that the Runnable should throw
   * @param callable A Callable that is expected to throw the exception
   * @deprecated Use {@link Assertions#assertThatThrownBy(ThrowableAssert.ThrowingCallable)}
   *     directly as it provides a more fluent way of asserting on exceptions.
   */
  @Deprecated
  public static void assertThrows(
      String message, Class<? extends Exception> expected, Callable callable) {
    assertThrows(message, expected, null, callable);
  }

  /**
   * A convenience method to avoid a large number of @Test(expected=...) tests
   *
   * @param message A String message to describe this assertion
   * @param expected An Exception class that the Runnable should throw
   * @param runnable A Runnable that is expected to throw the runtime exception
   * @deprecated Use {@link Assertions#assertThatThrownBy(ThrowableAssert.ThrowingCallable)}
   *     directly as it provides a more fluent way of asserting on exceptions.
   */
  @Deprecated
  public static void assertThrows(
      String message, Class<? extends Exception> expected, Runnable runnable) {
    assertThrows(message, expected, null, runnable);
  }

  /**
   * A convenience method to assert the cause of thrown exception.
   *
   * @param message A String message to describe this assertion
   * @param expected An Exception class that the cause of the Runnable should throw
   * @param containedInMessage A String that should be contained by the cause of the thrown
   *     exception's message
   * @param runnable A Runnable that is expected to throw the runtime exception
   * @deprecated Use {@link Assertions#assertThatThrownBy(ThrowableAssert.ThrowingCallable)}
   *     directly as it provides a more fluent way of asserting on exceptions.
   */
  @Deprecated
  public static void assertThrowsCause(
      String message,
      Class<? extends Exception> expected,
      String containedInMessage,
      Runnable runnable) {
    Assertions.assertThatThrownBy(runnable::run)
        .as(message)
        .getCause()
        .isInstanceOf(expected)
        .hasMessageContaining(containedInMessage);
  }

  /**
   * A convenience method to assert both the thrown exception and the cause of thrown exception.
   *
   * @param message A String message to describe this assertion
   * @param expected An Exception class that the Runnable should throw
   * @param expectedContainedInMessage A String that should be contained by the thrown exception's
   *     message, will be skipped if null.
   * @param cause An Exception class that the cause of the Runnable should throw
   * @param causeContainedInMessage A String that should be contained by the cause of the thrown
   *     exception's message, will be skipped if null.
   * @param runnable A Runnable that is expected to throw the runtime exception
   * @deprecated Use {@link Assertions#assertThatThrownBy(ThrowableAssert.ThrowingCallable)}
   *     directly as it provides a more fluent way of asserting on exceptions.
   */
  @Deprecated
  public static void assertThrowsWithCause(
      String message,
      Class<? extends Exception> expected,
      String expectedContainedInMessage,
      Class<? extends Exception> cause,
      String causeContainedInMessage,
      Runnable runnable) {
    AbstractThrowableAssert<?, ?> chain =
        Assertions.assertThatThrownBy(runnable::run).as(message).isInstanceOf(expected);

    if (expectedContainedInMessage != null) {
      chain = chain.hasMessageContaining(expectedContainedInMessage);
    }

    chain = chain.getCause().isInstanceOf(cause);
    if (causeContainedInMessage != null) {
      chain.hasMessageContaining(causeContainedInMessage);
    }
  }

  /**
   * A convenience method to check if an Avro field is empty.
   *
   * @param record The record to read from
   * @param field The name of the field
   * @deprecated Use {@link Assertions#assertThatThrownBy(ThrowableAssert.ThrowingCallable)}
   *     directly as it provides a more fluent way of asserting on exceptions.
   */
  @Deprecated
  public static void assertEmptyAvroField(GenericRecord record, String field) {
    AssertHelpers.assertThrows(
        "Not a valid schema field: " + field, AvroRuntimeException.class, () -> record.get(field));
  }

  /**
   * Same as {@link AssertHelpers#assertThrowsCause}, but this method compares root cause.
   *
   * @deprecated Use {@link Assertions#assertThatThrownBy(ThrowableAssert.ThrowingCallable)}
   *     directly as it provides a more fluent way of asserting on exceptions.
   */
  @Deprecated
  public static void assertThrowsRootCause(
      String message,
      Class<? extends Exception> expected,
      String containedInMessage,
      Runnable runnable) {
    Assertions.assertThatThrownBy(runnable::run)
        .as(message)
        .getRootCause()
        .isInstanceOf(expected)
        .hasMessageContaining(containedInMessage);
  }
}
