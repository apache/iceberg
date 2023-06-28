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
package org.apache.iceberg.data.orc;

import java.io.IOException;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.junit.Test;

public class TestOrcRowIterator extends OrcRowIteratorBase {

  @Test
  public void testReadAllStripes() throws IOException {
    // With default batch size of 1024, will read the following batches
    // Stripe 1: 1024, 1024, 1024, 928
    // Stripe 2: 1024, 1024, 1024, 928
    readAndValidate(Expressions.alwaysTrue(), DATA_ROWS);
  }

  @Test
  public void testReadFilteredRowGroupInMiddle() throws IOException {
    // We skip the 2nd row group [1000, 2000] in Stripe 1
    // With default batch size of 1024, will read the following batches
    // Stripe 1: 1000, 1024, 976
    readAndValidate(
        Expressions.in("id", 500, 2500, 3500),
        Lists.newArrayList(
            Iterables.concat(DATA_ROWS.subList(0, 1000), DATA_ROWS.subList(2000, 4000))));
  }
}
