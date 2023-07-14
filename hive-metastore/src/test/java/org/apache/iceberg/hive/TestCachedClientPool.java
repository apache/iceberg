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
package org.apache.iceberg.hive;

import static org.apache.iceberg.CatalogUtil.ICEBERG_CATALOG_TYPE;
import static org.apache.iceberg.CatalogUtil.ICEBERG_CATALOG_TYPE_HIVE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.security.PrivilegedAction;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.hive.CachedClientPool.Key;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;

public class TestCachedClientPool extends HiveMetastoreTest {

  @Test
  public void testClientPoolCleaner() throws InterruptedException {
    CachedClientPool clientPool = new CachedClientPool(hiveConf, Collections.emptyMap());
    HiveClientPool clientPool1 = clientPool.clientPool();
    assertThat(clientPool1)
        .isSameAs(
            CachedClientPool.clientPoolCache()
                .getIfPresent(CachedClientPool.extractKey(null, hiveConf)));
    TimeUnit.MILLISECONDS.sleep(EVICTION_INTERVAL - TimeUnit.SECONDS.toMillis(2));
    HiveClientPool clientPool2 = clientPool.clientPool();
    assertThat(clientPool2).isSameAs(clientPool1);
    TimeUnit.MILLISECONDS.sleep(EVICTION_INTERVAL + TimeUnit.SECONDS.toMillis(5));
    assertThat(
            CachedClientPool.clientPoolCache()
                .getIfPresent(CachedClientPool.extractKey(null, hiveConf)))
        .isNull();

    // The client has been really closed.
    assertThat(clientPool1.isClosed()).isTrue();
    assertThat(clientPool2.isClosed()).isTrue();
  }

  @Test
  public void testCacheKey() throws Exception {
    UserGroupInformation current = UserGroupInformation.getCurrentUser();
    UserGroupInformation foo1 = UserGroupInformation.createProxyUser("foo", current);
    UserGroupInformation foo2 = UserGroupInformation.createProxyUser("foo", current);
    UserGroupInformation bar = UserGroupInformation.createProxyUser("bar", current);

    Key key1 =
        foo1.doAs(
            (PrivilegedAction<Key>)
                () -> CachedClientPool.extractKey("user_name,conf:key1", hiveConf));
    Key key2 =
        foo2.doAs(
            (PrivilegedAction<Key>)
                () -> CachedClientPool.extractKey("conf:key1,user_name", hiveConf));
    assertThat(key2).as("Key elements order shouldn't matter").isEqualTo(key1);

    key1 = foo1.doAs((PrivilegedAction<Key>) () -> CachedClientPool.extractKey("ugi", hiveConf));
    key2 = bar.doAs((PrivilegedAction<Key>) () -> CachedClientPool.extractKey("ugi", hiveConf));
    assertThat(key2).as("Different users are not supposed to be equivalent").isNotEqualTo(key1);

    key2 = foo2.doAs((PrivilegedAction<Key>) () -> CachedClientPool.extractKey("ugi", hiveConf));
    assertThat(key2)
        .as("Different UGI instances are not supposed to be equivalent")
        .isNotEqualTo(key1);

    key1 = CachedClientPool.extractKey("ugi", hiveConf);
    key2 = CachedClientPool.extractKey("ugi,conf:key1", hiveConf);
    assertThat(key2)
        .as("Keys with different number of elements are not supposed to be equivalent")
        .isNotEqualTo(key1);

    Configuration conf1 = new Configuration(hiveConf);
    Configuration conf2 = new Configuration(hiveConf);

    conf1.set("key1", "val");
    key1 = CachedClientPool.extractKey("conf:key1", conf1);
    key2 = CachedClientPool.extractKey("conf:key1", conf2);
    assertThat(key2)
        .as("Config with different values are not supposed to be equivalent")
        .isNotEqualTo(key1);

    conf2.set("key1", "val");
    conf2.set("key2", "val");
    key2 = CachedClientPool.extractKey("conf:key2", conf2);
    assertThat(key2)
        .as("Config with different keys are not supposed to be equivalent")
        .isNotEqualTo(key1);

    key1 = CachedClientPool.extractKey("conf:key1,ugi", conf1);
    key2 = CachedClientPool.extractKey("ugi,conf:key1", conf2);
    assertThat(key2).as("Config with same key/value should be equivalent").isEqualTo(key1);

    conf1.set("key2", "val");
    key1 = CachedClientPool.extractKey("conf:key2 ,conf:key1", conf1);
    key2 = CachedClientPool.extractKey("conf:key2,conf:key1", conf2);
    assertThat(key2).as("Config with same key/value should be equivalent").isEqualTo(key1);

    assertThatThrownBy(
            () -> CachedClientPool.extractKey("ugi,ugi", hiveConf),
            "Duplicate key elements should result in an error")
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("UGI key element already specified");

    assertThatThrownBy(
            () -> CachedClientPool.extractKey("conf:k1,conf:k2,CONF:k1", hiveConf),
            "Duplicate conf key elements should result in an error")
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("Conf key element k1 already specified");
  }

  @Test
  public void testHmsCatalog() {
    Map<String, String> properties =
        ImmutableMap.of(
            String.valueOf(EVICTION_INTERVAL),
            String.valueOf(Integer.MAX_VALUE),
            ICEBERG_CATALOG_TYPE,
            ICEBERG_CATALOG_TYPE_HIVE);

    Configuration conf1 = new Configuration();
    conf1.set(HiveCatalog.HIVE_CONF_CATALOG, "foo");

    Configuration conf2 = new Configuration();
    conf2.set(HiveCatalog.HIVE_CONF_CATALOG, "foo");

    Configuration conf3 = new Configuration();
    conf3.set(HiveCatalog.HIVE_CONF_CATALOG, "bar");

    HiveCatalog catalog1 = (HiveCatalog) CatalogUtil.buildIcebergCatalog("1", properties, conf1);
    HiveCatalog catalog2 = (HiveCatalog) CatalogUtil.buildIcebergCatalog("2", properties, conf2);
    HiveCatalog catalog3 = (HiveCatalog) CatalogUtil.buildIcebergCatalog("3", properties, conf3);
    HiveCatalog catalog4 =
        (HiveCatalog) CatalogUtil.buildIcebergCatalog("4", properties, new Configuration());

    HiveClientPool pool1 = ((CachedClientPool) catalog1.clientPool()).clientPool();
    HiveClientPool pool2 = ((CachedClientPool) catalog2.clientPool()).clientPool();
    HiveClientPool pool3 = ((CachedClientPool) catalog3.clientPool()).clientPool();
    HiveClientPool pool4 = ((CachedClientPool) catalog4.clientPool()).clientPool();

    assertThat(pool2).isSameAs(pool1);
    assertThat(pool1).isNotSameAs(pool3);
    assertThat(pool2).isNotSameAs(pool3);
    assertThat(pool4).isNotSameAs(pool3);
    assertThat(pool1).isNotSameAs(pool4);
    assertThat(pool2).isNotSameAs(pool4);

    assertThat(pool1.hiveConf().get(HiveCatalog.HIVE_CONF_CATALOG)).isEqualTo("foo");
    assertThat(pool3.hiveConf().get(HiveCatalog.HIVE_CONF_CATALOG)).isEqualTo("bar");
    assertThat(pool4.hiveConf().get(HiveCatalog.HIVE_CONF_CATALOG)).isNull();

    pool1.close();
    pool3.close();
    pool4.close();
  }
}
