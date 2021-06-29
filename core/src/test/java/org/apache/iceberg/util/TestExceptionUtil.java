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

package org.apache.iceberg.util;

import java.io.IOException;
import org.assertj.core.api.Assertions;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestExceptionUtil {
  private static final Logger LOG = LoggerFactory.getLogger(TestExceptionUtil.class);

  private static class CustomCheckedException extends Exception {
    private CustomCheckedException(String message) {
      super(message);
    }
  }

  @Test
  public void testRunSafely() {
    CustomCheckedException exc = new CustomCheckedException("test");
    try {
      ExceptionUtil.runSafely(() -> {
            throw exc;
          }, e -> {
            throw new Exception("test catch suppression");
          }, () -> {
            throw new RuntimeException("test finally suppression");
          }, CustomCheckedException.class
      );

      Assert.fail("Should have thrown CustomCheckedException");

    } catch (CustomCheckedException e) {
      LOG.info("Final exception", e);
      Assert.assertEquals("Should throw correct exception instance", exc, e);
      Assert.assertEquals("Should not alter exception message", "test", e.getMessage());
      Assert.assertEquals("Should have 2 suppressed exceptions", 2, e.getSuppressed().length);

      Throwable throwSuppressed = e.getSuppressed()[0];
      Assertions.assertThat(throwSuppressed).as("Should be an Exception").isInstanceOf(Exception.class);
      Assert.assertEquals("Should have correct message", "test catch suppression", throwSuppressed.getMessage());

      Throwable finallySuppressed = e.getSuppressed()[1];
      Assertions.assertThat(finallySuppressed).as("Should be a RuntimeException").isInstanceOf(RuntimeException.class);
      Assert.assertEquals("Should have correct message", "test finally suppression", finallySuppressed.getMessage());
    }
  }

  @Test
  public void testRunSafelyTwoExceptions() {
    CustomCheckedException exc = new CustomCheckedException("test");
    try {
      ExceptionUtil.runSafely((ExceptionUtil.Block<Void, CustomCheckedException, IOException, RuntimeException>) () -> {
            throw exc;
          }, e -> {
            throw new Exception("test catch suppression");
          }, () -> {
            throw new RuntimeException("test finally suppression");
          }, CustomCheckedException.class, IOException.class
      );

      Assert.fail("Should have thrown CustomCheckedException");

    } catch (IOException e) {
      Assert.fail("Should not have thrown exception class: " + e.getClass().getName());

    } catch (CustomCheckedException e) {
      LOG.info("Final exception", e);
      Assert.assertEquals("Should throw correct exception instance", exc, e);
      Assert.assertEquals("Should not alter exception message", "test", e.getMessage());
      Assert.assertEquals("Should have 2 suppressed exceptions", 2, e.getSuppressed().length);

      Throwable throwSuppressed = e.getSuppressed()[0];
      Assertions.assertThat(throwSuppressed).as("Should be an Exception").isInstanceOf(Exception.class);
      Assert.assertEquals("Should have correct message", "test catch suppression", throwSuppressed.getMessage());

      Throwable finallySuppressed = e.getSuppressed()[1];
      Assertions.assertThat(finallySuppressed).as("Should be a RuntimeException").isInstanceOf(RuntimeException.class);
      Assert.assertEquals("Should have correct message", "test finally suppression", finallySuppressed.getMessage());
    }
  }

  @Test
  public void testRunSafelyThreeExceptions() {
    CustomCheckedException exc = new CustomCheckedException("test");
    try {
      ExceptionUtil.runSafely((ExceptionUtil.Block<Void, CustomCheckedException, IOException, ClassNotFoundException>)
          () -> {
            throw exc;
          }, e -> {
            throw new Exception("test catch suppression");
          }, () -> {
            throw new RuntimeException("test finally suppression");
          }, CustomCheckedException.class, IOException.class, ClassNotFoundException.class
      );

      Assert.fail("Should have thrown CustomCheckedException");

    } catch (IOException | ClassNotFoundException e) {
      Assert.fail("Should not have thrown exception class: " + e.getClass().getName());

    } catch (CustomCheckedException e) {
      LOG.info("Final exception", e);
      Assert.assertEquals("Should throw correct exception instance", exc, e);
      Assert.assertEquals("Should not alter exception message", "test", e.getMessage());
      Assert.assertEquals("Should have 2 suppressed exceptions", 2, e.getSuppressed().length);

      Throwable throwSuppressed = e.getSuppressed()[0];
      Assertions.assertThat(throwSuppressed).as("Should be an Exception").isInstanceOf(Exception.class);
      Assert.assertEquals("Should have correct message", "test catch suppression", throwSuppressed.getMessage());

      Throwable finallySuppressed = e.getSuppressed()[1];
      Assertions.assertThat(finallySuppressed).as("Should be a RuntimeException").isInstanceOf(RuntimeException.class);
      Assert.assertEquals("Should have correct message", "test finally suppression", finallySuppressed.getMessage());
    }
  }

  @Test
  public void testRunSafelyRuntimeExceptions() {
    RuntimeException exc = new RuntimeException("test");
    try {
      ExceptionUtil.runSafely(() -> {
            throw exc;
          }, e -> {
            throw new Exception("test catch suppression");
          }, () -> {
            throw new CustomCheckedException("test finally suppression");
          }
      );

      Assert.fail("Should have thrown RuntimeException");

    } catch (RuntimeException e) {
      LOG.info("Final exception", e);
      Assert.assertEquals("Should throw correct exception instance", exc, e);
      Assert.assertEquals("Should not alter exception message", "test", e.getMessage());
      Assert.assertEquals("Should have 2 suppressed exceptions", 2, e.getSuppressed().length);

      Throwable throwSuppressed = e.getSuppressed()[0];
      Assertions.assertThat(throwSuppressed).as("Should be an Exception").isInstanceOf(Exception.class);
      Assert.assertEquals("Should have correct message", "test catch suppression", throwSuppressed.getMessage());

      Throwable finallySuppressed = e.getSuppressed()[1];
      Assertions.assertThat(finallySuppressed).as("Should be a CustomCheckedException")
          .isInstanceOf(CustomCheckedException.class);
      Assert.assertEquals("Should have correct message", "test finally suppression", finallySuppressed.getMessage());
    }
  }

}
