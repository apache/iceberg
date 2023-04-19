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
package org.apache.iceberg.aws;

import java.io.Serializable;
import java.util.Map;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.kms.KmsClient;
import software.amazon.awssdk.services.s3.S3Client;

/**
 * Interface to customize AWS clients used by Iceberg. A custom factory must have a no-arg
 * constructor, and use {@link #initialize(Map)} to initialize the factory.
 */
public interface AwsClientFactory extends Serializable {

  /**
   * create a Amazon S3 client
   *
   * @return s3 client
   */
  S3Client s3();

  /**
   * create a AWS Glue client
   *
   * @return glue client
   */
  GlueClient glue();

  /**
   * Create a AWS KMS client
   *
   * @return kms client
   */
  KmsClient kms();

  /**
   * Create a Amazon DynamoDB client
   *
   * @return dynamoDB client
   */
  DynamoDbClient dynamo();

  /**
   * Initialize AWS client factory from catalog properties.
   *
   * @param properties catalog properties
   */
  void initialize(Map<String, String> properties);
}
