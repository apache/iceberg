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
package org.apache.iceberg.spark.source;

import java.util.Iterator;
import org.apache.iceberg.RecordWrapperTest;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.data.InternalRecordWrapper;
import org.apache.iceberg.data.RandomGenericData;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.iceberg.spark.data.RandomData;
import org.apache.iceberg.util.StructLikeWrapper;
import org.apache.spark.sql.catalyst.InternalRow;
import org.junit.Assert;
import org.junit.Ignore;

public class TestInternalRowWrapper extends RecordWrapperTest {

  @Ignore
  @Override
  public void testTimestampWithoutZone() {
    // Spark does not support timestamp without zone.
  }

  @Ignore
  @Override
  public void testTime() {
    // Spark does not support time fields.
  }

  @Override
  protected void generateAndValidate(Schema schema, AssertMethod assertMethod) {
    int numRecords = 100;
    Iterable<Record> recordList = RandomGenericData.generate(schema, numRecords, 101L);
    Iterable<InternalRow> rowList = RandomData.generateSpark(schema, numRecords, 101L);

    InternalRecordWrapper recordWrapper = new InternalRecordWrapper(schema.asStruct());
    InternalRowWrapper rowWrapper =
        new InternalRowWrapper(SparkSchemaUtil.convert(schema), schema.asStruct());

    Iterator<Record> actual = recordList.iterator();
    Iterator<InternalRow> expected = rowList.iterator();

    StructLikeWrapper actualWrapper = StructLikeWrapper.forType(schema.asStruct());
    StructLikeWrapper expectedWrapper = StructLikeWrapper.forType(schema.asStruct());
    for (int i = 0; i < numRecords; i++) {
      Assert.assertTrue("Should have more records", actual.hasNext());
      Assert.assertTrue("Should have more InternalRow", expected.hasNext());

      StructLike recordStructLike = recordWrapper.wrap(actual.next());
      StructLike rowStructLike = rowWrapper.wrap(expected.next());

      assertMethod.assertEquals(
          "Should have expected StructLike values",
          actualWrapper.set(recordStructLike),
          expectedWrapper.set(rowStructLike));
    }

    Assert.assertFalse("Shouldn't have more record", actual.hasNext());
    Assert.assertFalse("Shouldn't have more InternalRow", expected.hasNext());
  }
}
