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
package org.apache.iceberg.spark;

import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.METASTOREURIS;

import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestSparkSessionCatalog extends SparkTestBase {
  private final String envHmsUriKey = "spark.hadoop." + METASTOREURIS.varname;
  private final String catalogHmsUriKey = "spark.sql.catalog.spark_catalog.uri";
  private final String hmsUri = hiveConf.get(METASTOREURIS.varname);

  @BeforeClass
  public static void setUpCatalog() {
    spark
        .conf()
        .set("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog");
    spark.conf().set("spark.sql.catalog.spark_catalog.type", "hive");
  }

  @Before
  public void setupHmsUri() {
    spark.sessionState().catalogManager().reset();
    spark.conf().set(envHmsUriKey, hmsUri);
    spark.conf().set(catalogHmsUriKey, hmsUri);
  }

  @Test
  public void testValidateHmsUri() {
    // HMS uris match
    Assert.assertTrue(
        spark
            .sessionState()
            .catalogManager()
            .v2SessionCatalog()
            .defaultNamespace()[0]
            .equals("default"));

    // HMS uris doesn't match
    spark.sessionState().catalogManager().reset();
    String catalogHmsUri = "RandomString";
    spark.conf().set(envHmsUriKey, hmsUri);
    spark.conf().set(catalogHmsUriKey, catalogHmsUri);
    IllegalArgumentException exception =
        Assert.assertThrows(
            IllegalArgumentException.class,
            () -> spark.sessionState().catalogManager().v2SessionCatalog());
    String errorMessage =
        String.format(
            "Inconsistent Hive metastore URIs: %s (Spark session) != %s (spark_catalog)",
            hmsUri, catalogHmsUri);
    Assert.assertEquals(errorMessage, exception.getMessage());

    // no env HMS uri, only catalog HMS uri
    spark.sessionState().catalogManager().reset();
    spark.conf().set(catalogHmsUriKey, hmsUri);
    spark.conf().unset(envHmsUriKey);
    Assert.assertTrue(
        spark
            .sessionState()
            .catalogManager()
            .v2SessionCatalog()
            .defaultNamespace()[0]
            .equals("default"));

    // no catalog HMS uri, only env HMS uri
    spark.sessionState().catalogManager().reset();
    spark.conf().set(envHmsUriKey, hmsUri);
    spark.conf().unset(catalogHmsUriKey);
    Assert.assertTrue(
        spark
            .sessionState()
            .catalogManager()
            .v2SessionCatalog()
            .defaultNamespace()[0]
            .equals("default"));
  }

  @Test
  public void testLoadFunction() {
    String functionClass = "org.apache.hadoop.hive.ql.udf.generic.GenericUDFUpper";

    // load permanent UDF in Hive via FunctionCatalog
    spark.sql(String.format("CREATE FUNCTION perm_upper AS '%s'", functionClass));
    Assert.assertEquals("Load permanent UDF in Hive", "XYZ", scalarSql("SELECT perm_upper('xyz')"));

    // load temporary UDF in Hive via FunctionCatalog
    spark.sql(String.format("CREATE TEMPORARY FUNCTION temp_upper AS '%s'", functionClass));
    Assert.assertEquals("Load temporary UDF in Hive", "XYZ", scalarSql("SELECT temp_upper('xyz')"));

    // TODO: fix loading Iceberg built-in functions in SessionCatalog
  }
}
