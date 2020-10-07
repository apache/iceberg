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

package org.apache.iceberg.aws.glue.metastore;

import org.apache.hadoop.hive.conf.HiveConf;
import org.junit.Before;
import org.junit.Test;

import static org.apache.iceberg.aws.glue.util.AWSGlueConfig.AWS_GLUE_DB_CACHE_ENABLE;
import static org.apache.iceberg.aws.glue.util.AWSGlueConfig.AWS_GLUE_DB_CACHE_SIZE;
import static org.apache.iceberg.aws.glue.util.AWSGlueConfig.AWS_GLUE_DB_CACHE_TTL_MINS;
import static org.apache.iceberg.aws.glue.util.AWSGlueConfig.AWS_GLUE_ENDPOINT;
import static org.apache.iceberg.aws.glue.util.AWSGlueConfig.AWS_GLUE_TABLE_CACHE_ENABLE;
import static org.apache.iceberg.aws.glue.util.AWSGlueConfig.AWS_GLUE_TABLE_CACHE_SIZE;
import static org.apache.iceberg.aws.glue.util.AWSGlueConfig.AWS_GLUE_TABLE_CACHE_TTL_MINS;
import static org.apache.iceberg.aws.glue.util.AWSGlueConfig.AWS_REGION;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TestAWSGlueMetastoreFactory {

  private AWSGlueMetastoreFactory awsGlueMetastoreFactory;
  private HiveConf hiveConf;

  @Before
  public void before() {
    awsGlueMetastoreFactory = new AWSGlueMetastoreFactory();
    hiveConf = spy(new HiveConf());

    // these configs are needed for AWSGlueClient to get initialized
    System.setProperty(AWS_REGION, "");
    System.setProperty(AWS_GLUE_ENDPOINT, "");
    when(hiveConf.get(AWS_GLUE_ENDPOINT)).thenReturn("endpoint");
    when(hiveConf.get(AWS_REGION)).thenReturn("us-west-1");

    // these configs are needed for AWSGlueMetastoreCacheDecorator to get initialized
    when(hiveConf.getInt(AWS_GLUE_DB_CACHE_SIZE, 0)).thenReturn(1);
    when(hiveConf.getInt(AWS_GLUE_DB_CACHE_TTL_MINS, 0)).thenReturn(1);
    when(hiveConf.getInt(AWS_GLUE_TABLE_CACHE_SIZE, 0)).thenReturn(1);
    when(hiveConf.getInt(AWS_GLUE_TABLE_CACHE_TTL_MINS, 0)).thenReturn(1);
  }

  @Test
  public void testNewMetastoreWhenCacheDisabled() throws Exception {
    when(hiveConf.getBoolean(AWS_GLUE_DB_CACHE_ENABLE, false)).thenReturn(false);
    when(hiveConf.getBoolean(AWS_GLUE_TABLE_CACHE_ENABLE, false)).thenReturn(false);
    assertTrue(DefaultAWSGlueMetastore.class.equals(
        awsGlueMetastoreFactory.newMetastore(hiveConf).getClass()));
    verify(hiveConf, atLeastOnce()).getBoolean(AWS_GLUE_DB_CACHE_ENABLE, false);
    verify(hiveConf, atLeastOnce()).getBoolean(AWS_GLUE_TABLE_CACHE_ENABLE, false);
  }

  @Test
  public void testNewMetastoreWhenTableCacheEnabled() throws Exception {
    when(hiveConf.getBoolean(AWS_GLUE_DB_CACHE_ENABLE, false)).thenReturn(false);
    when(hiveConf.getBoolean(AWS_GLUE_TABLE_CACHE_ENABLE, false)).thenReturn(true);
    assertTrue(AWSGlueMetastoreCacheDecorator.class.equals(
        awsGlueMetastoreFactory.newMetastore(hiveConf).getClass()));
    verify(hiveConf, atLeastOnce()).getBoolean(AWS_GLUE_DB_CACHE_ENABLE, false);
    verify(hiveConf, atLeastOnce()).getBoolean(AWS_GLUE_TABLE_CACHE_ENABLE, false);
  }

  @Test
  public void testNewMetastoreWhenDBCacheEnabled() throws Exception {
    when(hiveConf.getBoolean(AWS_GLUE_DB_CACHE_ENABLE, false)).thenReturn(true);
    when(hiveConf.getBoolean(AWS_GLUE_TABLE_CACHE_ENABLE, false)).thenReturn(true);
    assertTrue(AWSGlueMetastoreCacheDecorator.class.equals(
        awsGlueMetastoreFactory.newMetastore(hiveConf).getClass()));
    verify(hiveConf, atLeastOnce()).getBoolean(AWS_GLUE_DB_CACHE_ENABLE, false);
    verify(hiveConf, atLeastOnce()).getBoolean(AWS_GLUE_TABLE_CACHE_ENABLE, false);
  }

  @Test
  public void testNewMetastoreWhenAllCacheEnabled() throws Exception {
    when(hiveConf.getBoolean(AWS_GLUE_DB_CACHE_ENABLE, false)).thenReturn(true);
    when(hiveConf.getBoolean(AWS_GLUE_TABLE_CACHE_ENABLE, false)).thenReturn(true);
    assertTrue(AWSGlueMetastoreCacheDecorator.class.equals(
        awsGlueMetastoreFactory.newMetastore(hiveConf).getClass()));
    verify(hiveConf, atLeastOnce()).getBoolean(AWS_GLUE_DB_CACHE_ENABLE, false);
    verify(hiveConf, atLeastOnce()).getBoolean(AWS_GLUE_TABLE_CACHE_ENABLE, false);
  }

}
