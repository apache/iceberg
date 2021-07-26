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

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.spark.SparkCatalog;
import org.apache.iceberg.spark.SparkCatalogTestBase;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import scala.Some;

public class TestSparkMetadataTables extends SparkCatalogTestBase {

  public TestSparkMetadataTables(String catalogName, String implementation, Map<String, String> config) {
    super(catalogName, implementation, config);
  }

  @Test
  public void testDataFileProjectionError2() throws Exception {
    // init load
    List<SimpleRecord> records = Lists.newArrayList(new SimpleRecord(1, "1"));
    Dataset<Row> inputDf = spark.createDataFrame(records, SimpleRecord.class);
    inputDf.writeTo(tableName).create();


    Dataset<Row> stringDs = spark.createDataset(Arrays.asList("my_path"), Encoders.STRING())
        .toDF("file_path");

    SparkCatalog catalog = (SparkCatalog) spark.sessionState().catalogManager().catalog(catalogName);
    String[] tableIdentifiers = tableName.split("\\.");
    Identifier metaId = Identifier.of(
        new String[]{tableIdentifiers[1], tableIdentifiers[2]}, "entries");
    SparkTable metaTable = catalog.loadTable(metaId);
    Dataset<Row> entriesDs = Dataset.ofRows(spark, DataSourceV2Relation.create(metaTable, Some.apply(catalog),
        Some.apply(metaId)));

    Column joinCond = entriesDs.col("data_file.file_path").equalTo(stringDs.col("file_path"));
    Dataset<Row> res = entriesDs.join(stringDs, joinCond);
    boolean empty = res.isEmpty();
    Assert.assertEquals(true, empty);
  }

  @Before
  public void dropTables() {
    sql("DROP TABLE IF EXISTS %s", tableName);
  }
}
