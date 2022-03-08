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

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.apache.iceberg.relocated.com.google.common.util.concurrent.MoreExecutors;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class TestUpdateProperties extends TableTestBase {
  @Parameterized.Parameters(name = "formatVersion = {0}")
  public static Object[] parameters() {
    return new Object[] {1, 2};
  }

  private static final Function<String, String> plusOneTransform = new Function<String, String>() {
    @Override
    public String apply(String s) {
      if (s == null) {
        return "1";
      }
      return String.valueOf((Integer.parseInt(s) + 1));
    }
  };

  public TestUpdateProperties(int formatVersion) {
    super(formatVersion);
  }

  @Test
  public void testPropertyTransform() {
    TableMetadata base = readMetadata();
    Assert.assertNull("Should not have a current snapshot", base.currentSnapshot());
    Assert.assertEquals("Last sequence number should be 0", 0, base.lastSequenceNumber());

    String counter = "counter";
    Assert.assertFalse("Counter key should not exist initially", table.properties().containsKey(counter));
    table.updateProperties().transform(counter, plusOneTransform).commit();
    Assert.assertEquals("Counter value should be 1", "1", table.properties().get(counter));
    table.updateProperties().transform(counter, plusOneTransform).commit();
    Assert.assertEquals("Counter value should be 2", "2", table.properties().get(counter));
  }

  @Test
  public void testConcurrentPropertyTransform() throws InterruptedException {
    TableMetadata base = readMetadata();
    Assert.assertNull("Should not have a current snapshot", base.currentSnapshot());
    Assert.assertEquals("Last sequence number should be 0", 0, base.lastSequenceNumber());

    String counter = "counter";
    Assert.assertFalse("Counter key should not exist initially", table.properties().containsKey(counter));

    ExecutorService executorService = MoreExecutors.getExitingExecutorService(
        (ThreadPoolExecutor) Executors.newFixedThreadPool(7));
    for (int i = 0; i < 7; i++) {
      executorService.submit(() -> table.updateProperties().transform(counter, plusOneTransform).commit());
    }
    executorService.shutdown();

    Assert.assertTrue("Timeout", executorService.awaitTermination(3, TimeUnit.MINUTES));
    Assert.assertEquals("Counter value should be 7", "7", table.properties().get(counter));
  }
}
