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

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class TestManifestGroupPlanFiles extends TestManifestReader {
  @Parameterized.Parameters(name = "formatVersion = {0}")
  public static Object[] parameters() {
    return new Object[] {1, 2};
  }

  public TestManifestGroupPlanFiles(int formatVersion) {
    super(formatVersion);
  }

  @Test
  public void testCloseParallelIteratorWithoutCompleteIteration()
      throws IOException, IllegalAccessException, NoSuchFieldException {
    ExecutorService executor = Executors.newFixedThreadPool(1);
    ManifestFile manifest = writeManifest(1000L, FILE_A, FILE_B, FILE_C);
    ManifestGroup manifestGroup =
        new ManifestGroup(FILE_IO, ImmutableList.of(manifest))
            .specsById(table.specs())
            .planWith(executor);
    CloseableIterator<FileScanTask> fileScanTaskCloseableIterator =
        manifestGroup.planFiles().iterator();
    Field queueFiled = fileScanTaskCloseableIterator.getClass().getDeclaredField("queue");
    queueFiled.setAccessible(true);
    Object queueObject = queueFiled.get(fileScanTaskCloseableIterator);
    ConcurrentLinkedQueue<FileScanTask> queue = (ConcurrentLinkedQueue<FileScanTask>) queueObject;

    Assert.assertTrue(fileScanTaskCloseableIterator.hasNext());
    Assert.assertNotNull(fileScanTaskCloseableIterator.next());
    Assert.assertFalse(queue.isEmpty());

    fileScanTaskCloseableIterator.close();
    Assert.assertTrue(queue.isEmpty());
  }
}
