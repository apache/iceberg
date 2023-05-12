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
package org.apache.iceberg.aws.s3;

import java.util.Map;
import org.apache.iceberg.aws.AwsClientFactory;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestS3FileIOAwsClientFactory {

  @Test
  public void testS3FileIOImplSetToTrue() {
    Map<String, String> properties = Maps.newHashMap();
    properties.put(S3FileIOAwsClientFactory.CLIENT_FACTORY, "true");
    Object factoryImpl = S3FileIOAwsClientFactory.getS3ClientFactoryImpl(properties);
    Assertions.assertThat(factoryImpl instanceof S3FileIOAwsClientFactory)
        .withFailMessage(
            "should instantiate an object of type S3FileIOAwsClientFactory when s3.client-factory-impl is set")
        .isTrue();
    assert factoryImpl instanceof S3FileIOAwsClientFactory;
    Assertions.assertThat(((S3FileIOAwsClientFactory) factoryImpl).s3())
        .withFailMessage("should be able to create s3 client using S3FileIOAwsClientFactory")
        .isNotNull();
  }

  @Test
  public void testS3FileIOImplSetToFalse() {
    Map<String, String> properties = Maps.newHashMap();
    properties.put(S3FileIOAwsClientFactory.CLIENT_FACTORY, "false");
    Object factoryImpl = S3FileIOAwsClientFactory.getS3ClientFactoryImpl(properties);
    Assertions.assertThat(factoryImpl instanceof AwsClientFactory)
        .withFailMessage(
            "should instantiate an object of type AwsClientFactory when s3.client-factory-impl is set to false")
        .isTrue();
    assert factoryImpl instanceof AwsClientFactory;
    Assertions.assertThat(((AwsClientFactory) factoryImpl).s3())
        .withFailMessage("should be able to create s3 client using AwsClientFactory")
        .isNotNull();
  }
}
