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

package org.apache.iceberg.flink.scala.source

import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.DataTypes
import org.apache.flink.table.api.TableSchema
import org.apache.flink.table.data.RowData
import org.apache.flink.types.Row
import org.apache.iceberg.Schema
import org.apache.iceberg.Table
import org.apache.iceberg.catalog.TableIdentifier
import org.apache.iceberg.data.GenericAppenderHelper
import org.apache.iceberg.data.RandomGenericData
import org.apache.iceberg.data.Record
import org.apache.iceberg.flink.FlinkSchemaUtil
import org.apache.iceberg.flink.TestHelpers
import org.apache.iceberg.flink.scala.source.FlinkSource._
import org.apache.iceberg.flink.source.FlinkSourceScala
import org.apache.iceberg.flink.source.TestFlinkInputFormat
import org.apache.iceberg.flink.source.TestFlinkScan
import org.apache.iceberg.relocated.com.google.common.collect.Lists
import org.apache.iceberg.types.Types
import org.junit.Test


class TestFlinkSourceScala(file: String) extends TestFlinkInputFormat(file) {

  override def before(): Unit = {
    super.before()
  }

  @Test
  def testScalaApi(): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val writeSchema: Schema = new Schema(
      Types.NestedField.required(0, "id", Types.LongType.get()),
      Types.NestedField.optional(1, "data", Types.StringType.get()),
      Types.NestedField.optional(2, "time", Types.TimestampType.withZone())
    )

    val table: Table = catalog.createTable(TableIdentifier.of("default", "t"), writeSchema)

    val writeRecords: java.util.List[Record] = RandomGenericData.generate(writeSchema, 2, 0L)
    new GenericAppenderHelper(table, fileFormat, TestFlinkScan.TEMPORARY_FOLDER).appendToTable(writeRecords)

    val projectedSchema: TableSchema = TableSchema.builder()
      .field("id", DataTypes.BIGINT())
      .field("data", DataTypes.STRING())
      .build()

    val rowType = FlinkSchemaUtil.convert(FlinkSchemaUtil.convert(projectedSchema));

    val data: DataStream[RowData] = FlinkSourceScala.forRowData()
      .env(env)
      .tableLoader(tableLoader())
      .project(projectedSchema).build()
    val results: java.util.ArrayList[RowData] = Lists.newArrayList()

    data.executeAndCollect().forEachRemaining(one => {
      results.add(TestHelpers.copyRowData(one, rowType))
    })

    val result = TestHelpers.convertRowDataToRow(results, rowType)

    val expected: java.util.List[Row] = Lists.newArrayList();
    writeRecords.forEach(record => {
      expected.add(Row.of(record.get(0), record.get(1)));
    })

    TestHelpers.assertRows(result, expected);
  }
}
