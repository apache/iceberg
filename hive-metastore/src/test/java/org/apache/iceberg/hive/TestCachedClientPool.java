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

import java.security.PrivilegedAction;
import java.util.Collections;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.iceberg.AssertHelpers;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.hive.CachedClientPool.Key;
import org.junit.Assert;
import org.junit.Test;

public class TestCachedClientPool extends HiveMetastoreTest {

  @Test
  public void testClientPoolCleaner() throws InterruptedException {
    String metastoreUri = hiveConf.get(HiveConf.ConfVars.METASTOREURIS.varname, "");
    CachedClientPool clientPool = new CachedClientPool(hiveConf, Collections.emptyMap());
    HiveClientPool clientPool1 = clientPool.clientPool();
    Supplier<Object> uri = () -> CachedClientPool.URIElement.of(metastoreUri);
    Assert.assertTrue(
        CachedClientPool.clientPoolCache()
                .getIfPresent(CachedClientPool.toKey(Collections.singletonList(uri)))
            == clientPool1);
    TimeUnit.MILLISECONDS.sleep(EVICTION_INTERVAL - TimeUnit.SECONDS.toMillis(2));
    HiveClientPool clientPool2 = clientPool.clientPool();
    Assert.assertTrue(clientPool1 == clientPool2);
    TimeUnit.MILLISECONDS.sleep(EVICTION_INTERVAL + TimeUnit.SECONDS.toMillis(5));
    Assert.assertNull(
        CachedClientPool.clientPoolCache()
            .getIfPresent(CachedClientPool.toKey(Collections.singletonList(uri))));
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
                () ->
                    CachedClientPool.toKey(
                        CachedClientPool.extractKeySuppliers("Uri, user_name", hiveConf)));
    Key key2 =
        foo2.doAs(
            (PrivilegedAction<Key>)
                () ->
                    CachedClientPool.toKey(
                        CachedClientPool.extractKeySuppliers("user_name,uri", hiveConf)));
    Assert.assertEquals("Key elements order shouldn't matter", key1, key2);

    key1 =
        foo1.doAs(
            (PrivilegedAction<Key>)
                () ->
                    CachedClientPool.toKey(
                        CachedClientPool.extractKeySuppliers("uri,ugi", hiveConf)));
    key2 =
        bar.doAs(
            (PrivilegedAction<Key>)
                () ->
                    CachedClientPool.toKey(
                        CachedClientPool.extractKeySuppliers("uri,ugi", hiveConf)));
    Assert.assertNotEquals("Different users are not supposed to be equivalent", key1, key2);

    key2 =
        foo2.doAs(
            (PrivilegedAction<Key>)
                () ->
                    CachedClientPool.toKey(
                        CachedClientPool.extractKeySuppliers("uri,ugi", hiveConf)));
    Assert.assertNotEquals("Different UGI instances are not supposed to be equivalent", key1, key2);

    key1 = CachedClientPool.toKey(CachedClientPool.extractKeySuppliers("uri", hiveConf));
    key2 = CachedClientPool.toKey(CachedClientPool.extractKeySuppliers("uri,ugi", hiveConf));
    Assert.assertNotEquals(
        "Keys with different number of elements are not supposed to be equivalent", key1, key2);

    Configuration conf1 = new Configuration(hiveConf);
    Configuration conf2 = new Configuration(hiveConf);

    conf1.set("key1", "val");
    key1 = CachedClientPool.toKey(CachedClientPool.extractKeySuppliers("uri,conf:key1", conf1));
    key2 = CachedClientPool.toKey(CachedClientPool.extractKeySuppliers("uri,conf:key1", conf2));
    Assert.assertNotEquals(
        "Config with different values are not supposed to be equivalent", key1, key2);

    conf2.set("key1", "val");
    conf2.set("key2", "val");
    key2 = CachedClientPool.toKey(CachedClientPool.extractKeySuppliers("uri,conf:key2", conf2));
    Assert.assertNotEquals(
        "Config with different keys are not supposed to be equivalent", key1, key2);

    key1 = CachedClientPool.toKey(CachedClientPool.extractKeySuppliers("uri,conf:key1,ugi", conf1));
    key2 = CachedClientPool.toKey(CachedClientPool.extractKeySuppliers("uri,ugi,conf:key1", conf2));
    Assert.assertEquals("Config with same key/value should be equivalent", key1, key2);

    conf1.set("key2", "val");
    key1 =
        CachedClientPool.toKey(
            CachedClientPool.extractKeySuppliers("uri,conf:key2 ,conf:key1", conf1));
    key2 =
        CachedClientPool.toKey(
            CachedClientPool.extractKeySuppliers("conf:key2,conf:key1,uri", conf2));
    Assert.assertEquals("Config with same key/value should be equivalent", key1, key2);

    AssertHelpers.assertThrows(
        "Duplicate key elements should result in an error",
        ValidationException.class,
        "URI key element already specified",
        () -> CachedClientPool.extractKeySuppliers("uri,uri,ugi", hiveConf));

    AssertHelpers.assertThrows(
        "Duplicate conf key elements should result in an error",
        ValidationException.class,
        "Conf key element k1 already specified",
        () -> CachedClientPool.extractKeySuppliers("uri,conf:k1,conf:k2,CONF:k1", hiveConf));
  }
}
