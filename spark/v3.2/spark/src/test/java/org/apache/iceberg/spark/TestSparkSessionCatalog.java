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
import org.junit.Test;

public class TestSparkSessionCatalog extends SparkTestBase {
  @Test
  public void testValidateHmsUri() {
    String envHmsUriKey = "spark.hadoop." + METASTOREURIS.varname;
    String catalogHmsUriKey = "spark.sql.catalog.spark_catalog.uri";
    String hmsUri = hiveConf.get(METASTOREURIS.varname);

    spark
        .conf()
        .set("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog");
    spark.conf().set("spark.sql.catalog.spark_catalog.type", "hive");

    // HMS uris match
    spark.sessionState().catalogManager().reset();
    spark.conf().set(envHmsUriKey, hmsUri);
    spark.conf().set(catalogHmsUriKey, hmsUri);
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
}
