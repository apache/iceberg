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
import org.junit.Assert;

public class AssertHelpers {

  private AssertHelpers() {}

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

  /**
   * A convenience method to avoid a large number of @Test(expected=...) tests
   * @param message A String message to describe this assertion
   * @param expected An Exception class that the Runnable should throw
   * @param callable A Callable that is expected to throw the exception
   */
  public static void assertThrows(String message,
                                  Class<? extends Exception> expected,
                                  Callable callable) {
    assertThrows(message, expected, null, callable);
  }

  /**
   * A convenience method to avoid a large number of @Test(expected=...) tests
   * @param message A String message to describe this assertion
   * @param expected An Exception class that the Runnable should throw
   * @param runnable A Runnable that is expected to throw the runtime exception
   */
  public static void assertThrows(String message,
                                  Class<? extends Exception> expected,
                                  Runnable runnable) {
    assertThrows(message, expected, null, runnable);
  }

  /**
   * A convenience method to assert the cause of thrown exception.
   * @param message A String message to describe this assertion
   * @param expected An Exception class that the cause of the Runnable should throw
   * @param containedInMessage A String that should be contained by the cause of the thrown
   *                           exception's message
   * @param runnable A Runnable that is expected to throw the runtime exception
   */
  public static void assertThrowsCause(String message,
                                       Class<? extends Exception> expected,
                                       String containedInMessage,
                                       Runnable runnable) {
    try {
      runnable.run();
      Assert.fail("No exception was thrown (" + message + "), expected: " +
          expected.getName());
    } catch (Exception actual) {
      Throwable cause = actual.getCause();
      if (cause instanceof Exception) {
        handleException(message, expected, containedInMessage, (Exception) actual.getCause());
      } else {
        Assert.fail("Occur non-exception cause: " + cause);
      }
    }
  }

  private static void handleException(String message,
                                      Class<? extends Exception> expected,
                                      String containedInMessage,
                                      Exception actual) {
    try {
      Assert.assertEquals(message, expected, actual.getClass());
      if (containedInMessage != null) {
        Assert.assertTrue(
            "Expected exception message (" + containedInMessage + ") missing: " +
                actual.getMessage(),
            actual.getMessage().contains(containedInMessage)
        );
      }
    } catch (AssertionError e) {
      e.addSuppressed(actual);
      throw e;
    }
  }
}
