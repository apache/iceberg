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

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CachingCatalog;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.spark.SparkCatalog;
import org.apache.iceberg.spark.SparkTestBase;
import org.assertj.core.api.Assertions;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class TestSparkCatalogWithCachingBehavior extends SparkTestBase {

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

  @Test
  public void testCatalogWithDisabledCaching() throws IOException {
    spark.conf().set("spark.sql.catalog.mycat", SparkCatalog.class.getName());
    spark.conf().set("spark.sql.catalog.mycat.catalog-impl", NonCacheableHadoopCatalog.class.getName());
    spark.conf().set("spark.sql.catalog.mycat.warehouse", temp.newFolder().toURI().toString());
    SparkCatalog catalog = (SparkCatalog) spark.sessionState().catalogManager().catalog("mycat");
    Assertions.assertThat(catalog).extracting("icebergCatalog").isInstanceOf(NonCacheableHadoopCatalog.class);
  }

  @Test
  public void testDefaultCaching() throws IOException {
    spark.conf().set("spark.sql.catalog.mycat2", SparkCatalog.class.getName());
    spark.conf().set("spark.sql.catalog.mycat2.catalog-impl", HadoopCatalog.class.getName());
    spark.conf().set("spark.sql.catalog.mycat2.warehouse", temp.newFolder().toURI().toString());
    SparkCatalog cat = (SparkCatalog) spark.sessionState().catalogManager().catalog("mycat2");
    // if caching is enabled, then the Catalog will be wrapped in a CachingCatalog
    Assertions.assertThat(cat).extracting("icebergCatalog").isInstanceOf(CachingCatalog.class);
  }

  @Test
  public void testCatalogWithOverriddenCachingBehavior() throws IOException {
    spark.conf().set("spark.sql.catalog.mycat3", SparkCatalog.class.getName());
    spark.conf().set("spark.sql.catalog.mycat3.catalog-impl", NonCacheableHadoopCatalog.class.getName());
    spark.conf().set("spark.sql.catalog.mycat3.warehouse", temp.newFolder().toURI().toString());
    // we override the default caching behavior NonCacheableHadoopCatalog
    spark.conf().set("spark.sql.catalog.mycat3.cache-enabled", "true");
    SparkCatalog catalog = (SparkCatalog) spark.sessionState().catalogManager().catalog("mycat3");
    // if caching is enabled, then the Catalog will be wrapped in a CachingCatalog
    Assertions.assertThat(catalog).extracting("icebergCatalog").isInstanceOf(CachingCatalog.class);
  }

  public static class NonCacheableHadoopCatalog extends HadoopCatalog {
    public NonCacheableHadoopCatalog() {
    }

    public NonCacheableHadoopCatalog(Configuration conf, String warehouseLocation) {
      super(conf, warehouseLocation);
    }

    @Override
    public boolean defaultCachingEnabled() {
      return false;
    }
  }
}
