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
package org.apache.iceberg.spark.extensions;

import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.METASTOREURIS;

import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.hive.HiveCatalog;
import org.apache.iceberg.hive.TestHiveMetastore;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.spark.ExtendedParser;
import org.apache.iceberg.spark.SparkTestBase;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.SparkSessionExtensions;
import org.apache.spark.sql.catalyst.FunctionIdentifier;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.parser.ParseException;
import org.apache.spark.sql.catalyst.parser.ParserInterface;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.internal.SQLConf;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructType;
import org.junit.After;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;
import scala.Function1;
import scala.Function2;
import scala.collection.Seq;
import scala.runtime.BoxedUnit;

public class TestMultipleExtensionsRewriteSortProcedure extends TestRewriteDataFilesProcedure {
  private static final Random RANDOM = ThreadLocalRandom.current();

  public TestMultipleExtensionsRewriteSortProcedure(
      String catalogName, String implementation, Map<String, String> config) {
    super(catalogName, implementation, config);
  }

  @BeforeClass
  public static void startMetastoreAndSpark() {
    SparkTestBase.metastore = new TestHiveMetastore();
    metastore.start();
    SparkTestBase.hiveConf = metastore.hiveConf();

    SparkTestBase.spark =
        SparkSession.builder()
            .master("local[2]")
            .config("spark.testing", "true")
            .config(SQLConf.PARTITION_OVERWRITE_MODE().key(), "dynamic")
            // Spark will encapsulate multiple extensions classes here, and
            // SparkSession.sessionState().sqlParser() is an instance of the last extension class
            .config(
                "spark.sql.extensions",
                String.join(
                    ",",
                    IcebergSparkSessionExtensions.class.getName(),
                    TestExtensions.class.getName()))
            .config("spark.hadoop." + METASTOREURIS.varname, hiveConf.get(METASTOREURIS.varname))
            .config("spark.sql.shuffle.partitions", "4")
            .config("spark.sql.hive.metastorePartitionPruningFallbackOnException", "true")
            .config("spark.sql.legacy.respectNullabilityInTextDatasetConversion", "true")
            .config(
                SQLConf.ADAPTIVE_EXECUTION_ENABLED().key(), String.valueOf(RANDOM.nextBoolean()))
            .enableHiveSupport()
            .getOrCreate();

    SparkTestBase.catalog =
        (HiveCatalog)
            CatalogUtil.loadCatalog(
                HiveCatalog.class.getName(), "hive", ImmutableMap.of(), hiveConf);
  }

  @After
  public void removeTableAfter() {
    sql("DROP TABLE IF EXISTS %s", tableName);
  }

  @Test
  public void testRewriteWithSortAndNotInstanceOfIcebergExtensions() {
    Assume.assumeTrue(
        "Spark Extensions should not be Iceberg Extensions",
        !(spark.sessionState().sqlParser() instanceof ExtendedParser));
    createTable();
    // create 10 files under non-partitioned table
    insertData(10);
    List<Object[]> expectedRecords = currentData();

    // set sort_order = c1 DESC LAST
    List<Object[]> output =
        sql(
            "CALL %s.system.rewrite_data_files(table => '%s', "
                + "strategy => 'sort', sort_order => 'c1 DESC NULLS LAST')",
            catalogName, tableIdent);

    assertEquals(
        "Action should rewrite 10 data files and add 1 data files",
        ImmutableList.of(row(10, 1)),
        output);

    List<Object[]> actualRecords = currentData();
    assertEquals("Data after compaction should not change", expectedRecords, actualRecords);
  }

  @Test
  public void testRewriteWithZOrderAndNotInstanceOfIcebergExtensions() {
    Assume.assumeTrue(
        "Spark Extensions should not be Iceberg Extensions",
        !(spark.sessionState().sqlParser() instanceof ExtendedParser));
    createTable();
    // create 10 files under non-partitioned table
    insertData(10);

    // set z_order = c1,c2
    List<Object[]> output =
        sql(
            "CALL %s.system.rewrite_data_files(table => '%s', "
                + "strategy => 'sort', sort_order => 'zorder(c1,c2)')",
            catalogName, tableIdent);

    assertEquals(
        "Action should rewrite 10 data files and add 1 data files",
        ImmutableList.of(row(10, 1)),
        output);

    // Due to Z_order, the data written will be in the below order.
    // As there is only one small output file, we can validate the query ordering (as it will not
    // change).
    ImmutableList<Object[]> expectedRows =
        ImmutableList.of(
            row(2, "bar", null),
            row(2, "bar", null),
            row(2, "bar", null),
            row(2, "bar", null),
            row(2, "bar", null),
            row(1, "foo", null),
            row(1, "foo", null),
            row(1, "foo", null),
            row(1, "foo", null),
            row(1, "foo", null));
    assertEquals("Should have expected rows", expectedRows, sql("SELECT * FROM %s", tableName));
  }

  public static class TestExtensions implements Function1<SparkSessionExtensions, BoxedUnit> {
    @Override
    public BoxedUnit apply(SparkSessionExtensions sparkSessionExtensions) {
      sparkSessionExtensions.injectParser(new TestExtensionsParser());
      return BoxedUnit.UNIT;
    }
  }

  public static class TestExtensionsParser
      implements Function2<SparkSession, ParserInterface, ParserInterface> {
    @Override
    public TestSqlExtensionParser apply(
        SparkSession sparkSession, ParserInterface parserInterface) {
      return new TestSqlExtensionParser(parserInterface);
    }
  }

  public static class TestSqlExtensionParser implements ParserInterface {
    private ParserInterface delegate;

    public TestSqlExtensionParser(ParserInterface delegate) {
      this.delegate = delegate;
    }

    @Override
    public LogicalPlan parsePlan(String sqlText) throws ParseException {
      return delegate.parsePlan(sqlText);
    }

    @Override
    public Expression parseExpression(String sqlText) throws ParseException {
      return delegate.parseExpression(sqlText);
    }

    @Override
    public org.apache.spark.sql.catalyst.TableIdentifier parseTableIdentifier(String sqlText)
        throws ParseException {
      return delegate.parseTableIdentifier(sqlText);
    }

    @Override
    public FunctionIdentifier parseFunctionIdentifier(String sqlText) throws ParseException {
      return delegate.parseFunctionIdentifier(sqlText);
    }

    @Override
    public Seq<String> parseMultipartIdentifier(String sqlText) throws ParseException {
      return delegate.parseMultipartIdentifier(sqlText);
    }

    @Override
    public StructType parseTableSchema(String sqlText) throws ParseException {
      return delegate.parseTableSchema(sqlText);
    }

    @Override
    public DataType parseDataType(String sqlText) throws ParseException {
      return delegate.parseDataType(sqlText);
    }

    @Override
    public LogicalPlan parseQuery(String sqlText) throws ParseException {
      return delegate.parseQuery(sqlText);
    }
  }
}
