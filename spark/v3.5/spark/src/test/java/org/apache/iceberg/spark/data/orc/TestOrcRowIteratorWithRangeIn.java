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
package org.apache.iceberg.spark.data.orc;

import java.io.IOException;
import org.apache.iceberg.data.orc.OrcRowIteratorBase;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.spark.broadcastvar.expressions.RangeInTestUtils;
import org.apache.spark.sql.types.DataTypes;
import org.junit.Test;

public class TestOrcRowIteratorWithRangeIn extends OrcRowIteratorBase {
  @Test
  public void testReadFilteredRowGroupInMiddle() throws IOException {
    // We skip the 2nd row group [1000, 2000] in Stripe 1
    // With default batch size of 1024, will read the following batches
    // Stripe 1: 1000, 1024, 976
    readAndValidate(
        RangeInTestUtils.createPredicate(
            "id", new Object[] {500L, 2500L, 3500L}, DataTypes.LongType),
        Lists.newArrayList(
            Iterables.concat(DATA_ROWS.subList(0, 1000), DATA_ROWS.subList(2000, 4000))));
  }
}
