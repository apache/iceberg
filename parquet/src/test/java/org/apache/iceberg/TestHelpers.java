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

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.concurrent.Callable;
import org.apache.avro.AvroRuntimeException;
import org.apache.avro.generic.GenericRecord;
import org.assertj.core.api.AbstractThrowableAssert;

public class TestHelpers {

  private TestHelpers() {}

  /**
   * A convenience method to avoid a large number of @Test(expected=...) tests
   *
   * @param message A String message to describe this assertion
   * @param expected An Exception class that the Runnable should throw
   * @param containedInMessage A String that should be contained by the thrown exception's message
   * @param callable A Callable that is expected to throw the exception
   */
  public static void assertThrows(
      String message,
      Class<? extends Exception> expected,
      String containedInMessage,
      Callable callable) {
    AbstractThrowableAssert<?, ? extends Throwable> check =
        assertThatThrownBy(callable::call).as(message).isInstanceOf(expected);
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
   */
  public static void assertThrows(
      String message,
      Class<? extends Exception> expected,
      String containedInMessage,
      Runnable runnable) {
    AbstractThrowableAssert<?, ? extends Throwable> check =
        assertThatThrownBy(runnable::run).as(message).isInstanceOf(expected);
    if (null != containedInMessage) {
      check.hasMessageContaining(containedInMessage);
    }
  }

  /**
   * A convenience method to assert if an Avro field is empty
   *
   * @param record The record to read from
   * @param field The name of the field
   */
  public static void assertEmptyAvroField(GenericRecord record, String field) {
    TestHelpers.assertThrows(
        "Not a valid schema field: " + field,
        AvroRuntimeException.class,
        "Not a valid schema field: " + field,
        () -> record.get(field));
  }
}
