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
package org.apache.iceberg.spark.data;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.BaseFormatModelTests;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.iceberg.spark.SparkUtil;
import org.apache.iceberg.types.Type;
import org.apache.spark.sql.catalyst.InternalRow;

public class TestSparkFormatModel extends BaseFormatModelTests<InternalRow> {

  @Override
  protected Class<InternalRow> engineType() {
    return InternalRow.class;
  }

  @Override
  protected Object engineSchema(Schema schema) {
    return SparkSchemaUtil.convert(schema);
  }

  @Override
  protected InternalRow convertToEngine(Record record, Schema schema) {
    return InternalRowConverter.convert(schema, record);
  }

  @Override
  protected void assertEquals(Schema schema, List<InternalRow> expected, List<InternalRow> actual) {
    assertThat(actual).hasSameSizeAs(expected);
    for (int i = 0; i < expected.size(); i++) {
      TestHelpers.assertEquals(schema, expected.get(i), actual.get(i));
    }
  }

  @Override
  protected Object convertConstantToEngine(Type type, Object value) {
    return SparkUtil.internalToSpark(type, value);
  }
}
