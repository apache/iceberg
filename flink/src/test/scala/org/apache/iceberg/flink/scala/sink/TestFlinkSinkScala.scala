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

package org.apache.iceberg.flink.scala.sink

import org.apache.flink.streaming.api.environment.{StreamExecutionEnvironment => JavaEnv}
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.data.RowData
import org.apache.flink.table.data.util.DataFormatConverters
import org.apache.flink.types.Row
import org.apache.iceberg.FileFormat
import org.apache.iceberg.Table
import org.apache.iceberg.TableProperties
import org.apache.iceberg.flink.MiniClusterResource
import org.apache.iceberg.flink.SimpleDataUtil
import org.apache.iceberg.flink.TableLoader
import org.apache.iceberg.flink.sink.FlinkSinkScala
import org.apache.iceberg.flink.sink.TestFlinkIcebergSink
import org.apache.iceberg.flink.source.BoundedTestSource
import org.apache.iceberg.flink.util.FlinkCompatibilityUtil
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap
import org.apache.iceberg.relocated.com.google.common.collect.Lists
import org.junit.Assert
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.Parameterized
import scala.collection.JavaConverters._

@RunWith(classOf[Parameterized])
class TestFlinkSinkScala(ft: String, parallelism: Int, partitioned: Boolean) extends
  TestFlinkIcebergSink(ft, parallelism, partitioned) {

  val interval = 100
  val javaEnv: JavaEnv = JavaEnv.getExecutionEnvironment(MiniClusterResource.DISABLE_CLASSLOADER_CHECK_CONFIG)
    .enableCheckpointing(interval)
    .setParallelism(parallelism)
    .setMaxParallelism(parallelism);

  private val env: StreamExecutionEnvironment = new StreamExecutionEnvironment(javaEnv)

  private val CONVERTER: DataFormatConverters.RowConverter =
    new DataFormatConverters.RowConverter(SimpleDataUtil.FLINK_SCHEMA.getFieldDataTypes)

  private var tablePath: String = _
  private var table: Table = _
  private var tableLoader: TableLoader = _

  private val format: FileFormat = FileFormat.valueOf(ft.toUpperCase(java.util.Locale.ENGLISH))

  @Before
  override def before(): Unit = {
    val folder = TestFlinkIcebergSink.TEMPORARY_FOLDER.newFolder();
    val warehouse = folder.getAbsolutePath;

    tablePath = warehouse.concat("/test");
    Assert.assertTrue("Should create the table path correctly.", new java.io.File(tablePath).mkdir());

    val props = ImmutableMap.of(TableProperties.DEFAULT_FILE_FORMAT, format.name());
    table = SimpleDataUtil.createTable(tablePath, props, partitioned);

    tableLoader = TableLoader.fromHadoopTable(tablePath);
  }


  @Test
  def testScalaApi(): Unit = {
    val rows: java.util.List[Row] = Lists.newArrayList(
      Row.of(new Integer(1), "hello"),
      Row.of(new Integer(1), "world"),
      Row.of(new Integer(1), "foo")
    )

    val source: BoundedTestSource[Row] = new BoundedTestSource(java.util.Collections.singletonList(rows))

    val dataStream: DataStream[RowData] = env.addSource(source)(SimpleDataUtil.FLINK_SCHEMA.toRowType)
      .map(one => {
        new DataFormatConverters.RowConverter(SimpleDataUtil.FLINK_SCHEMA.getFieldDataTypes).toInternal(one)
      })(FlinkCompatibilityUtil.toTypeInfo(SimpleDataUtil.ROW_TYPE))

    FlinkSinkScala.forRowData(dataStream)
      .table(table)
      .tableLoader(tableLoader)
      .writeParallelism(parallelism)
      .build()

    // Execute the program.
    env.execute("Test Iceberg DataStream.");

    SimpleDataUtil.assertTableRows(tablePath, convertToRowData(rows));
  }

  private def convertToRowData(rows: java.util.List[Row]): java.util.List[RowData] = {
    rows.asScala.map(row => CONVERTER.toInternal(row)).asJava
  }
}
