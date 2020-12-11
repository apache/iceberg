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
      Assert.assertTrue("Should be a CustomCheckedException", throwSuppressed instanceof Exception);
      Assert.assertEquals("Should have correct message", "test catch suppression", throwSuppressed.getMessage());

      Throwable finallySuppressed = e.getSuppressed()[1];
      Assert.assertTrue("Should be a CustomCheckedException", finallySuppressed instanceof RuntimeException);
      Assert.assertEquals("Should have correct message", "test finally suppression", finallySuppressed.getMessage());
    }
  }
}
