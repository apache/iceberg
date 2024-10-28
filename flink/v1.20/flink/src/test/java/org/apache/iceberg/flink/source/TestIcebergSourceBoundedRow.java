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
package org.apache.iceberg.flink.source;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;
import org.apache.iceberg.ParameterizedTestExtension;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.RandomGenericData;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.TestFixtures;
import org.apache.iceberg.flink.TestHelpers;
import org.apache.iceberg.flink.source.reader.RowConverter;
import org.apache.iceberg.flink.source.reader.RowDataConverter;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.TypeUtil;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ParameterizedTestExtension.class)
public class TestIcebergSourceBoundedRow extends TestIcebergSourceBoundedConverterBase<Row> {

  @TestTemplate
  public void testUnpartitionedTable() throws Exception {
    Table table = getUnpartitionedTable();
    List<Record> expectedRecords = RandomGenericData.generate(TestFixtures.SCHEMA, 2, 0L);
    addRecordsToUnpartitionedTable(table, expectedRecords);
    TestHelpers.assertRecords(run(), expectedRecords, TestFixtures.SCHEMA);
  }

  @TestTemplate
  public void testPartitionedTable() throws Exception {
    String dateStr = "2020-03-20";
    Table table = getPartitionedTable();
    List<Record> expectedRecords = RandomGenericData.generate(TestFixtures.SCHEMA, 2, 0L);
    for (Record expectedRecord : expectedRecords) {
      expectedRecord.setField("dt", dateStr);
    }
    addRecordsToPartitionedTable(table, dateStr, expectedRecords);
    TestHelpers.assertRecords(run(), expectedRecords, TestFixtures.SCHEMA);
  }

  @TestTemplate
  public void testProjection() throws Exception {
    Table table = getPartitionedTable();
    List<Record> expectedRecords = RandomGenericData.generate(TestFixtures.SCHEMA, 2, 0L);
    addRecordsToPartitionedTable(table, "2020-03-20", expectedRecords);
    // select the "data" field (fieldId == 1)
    Schema projectedSchema = TypeUtil.select(TestFixtures.SCHEMA, Sets.newHashSet(1));
    List<Row> expectedRows =
        Arrays.asList(Row.of(expectedRecords.get(0).get(0)), Row.of(expectedRecords.get(1).get(0)));
    TestHelpers.assertRows(
        run(projectedSchema, Collections.emptyList(), Collections.emptyMap()), expectedRows);
  }


  @Override
  protected RowDataConverter<Row> getConverter(Schema icebergSchema, Table table) throws Exception {
    return RowConverter.fromIcebergSchema(icebergSchema);
  }

  @Override
  protected TypeInformation<Row> getTypeInfo(Schema icebergSchema) {
    TableSchema tableSchema = FlinkSchemaUtil.toSchema(icebergSchema);
    return new RowTypeInfo(tableSchema.getFieldTypes(), tableSchema.getFieldNames());
  }

  @Override
  protected DataStream<Row> mapToRow(DataStream<Row> inputStream, Schema icebergSchema) {
    return inputStream;
  }
}
