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

import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.spark.broadcastvar.expressions.RangeInTestUtils;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class TestMetadataTableFiltersWithRangeIn extends MetadataTableFiltersCommon {

  public TestMetadataTableFiltersWithRangeIn(MetadataTableType type, int formatVersion) {
    super(type, formatVersion);
  }

  @Test
  public void testRangeIn() {
    Table metadataTable = createMetadataTable();

    Expression set =
        RangeInTestUtils.createPredicate(partitionColumn("data_bucket"), new Object[] {2, 3});
    TableScan scan = metadataTable.newScan().filter(set);

    CloseableIterable<FileScanTask> tasks = scan.planFiles();
    Assert.assertEquals(expectedScanTaskCount(2), Iterables.size(tasks));

    validateFileScanTasks(tasks, 2);
    validateFileScanTasks(tasks, 3);
  }
}
