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

package org.apache.iceberg.aws.glue;

import java.util.List;
import java.util.UUID;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.aws.AwsClientFactories;
import org.apache.iceberg.aws.AwsClientFactory;
import org.apache.iceberg.aws.AwsIntegTestUtil;
import org.apache.iceberg.aws.AwsProperties;
import org.apache.iceberg.aws.s3.S3FileIO;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.s3.S3Client;

@SuppressWarnings({"VisibilityModifier", "HideUtilityClassConstructor"})
public class GlueTestBase {

  private static final Logger LOG = LoggerFactory.getLogger(GlueTestBase.class);

  // the integration test requires the following env variables
  static final String testBucketName = AwsIntegTestUtil.testBucketName();

  static final String catalogName = "glue";
  static final String testPathPrefix = getRandomName();
  static final List<String> namespaces = Lists.newArrayList();

  // aws clients
  static final AwsClientFactory clientFactory = AwsClientFactories.defaultFactory();
  static final GlueClient glue = clientFactory.glue();
  static final S3Client s3 = clientFactory.s3();

  // iceberg
  static GlueCatalog glueCatalog;
  static GlueCatalog glueCatalogWithSkip;

  static Schema schema = new Schema(Types.NestedField.required(1, "c1", Types.StringType.get(), "c1"));
  static PartitionSpec partitionSpec = PartitionSpec.builderFor(schema).build();

  @BeforeClass
  public static void beforeClass() {
    String testBucketPath = "s3://" + testBucketName + "/" + testPathPrefix;
    S3FileIO fileIO = new S3FileIO(clientFactory::s3);
    glueCatalog = new GlueCatalog();
    glueCatalog.initialize(catalogName, testBucketPath, new AwsProperties(), glue, fileIO);
    AwsProperties properties = new AwsProperties();
    properties.setGlueCatalogSkipArchive(true);
    glueCatalogWithSkip = new GlueCatalog();
    glueCatalogWithSkip.initialize(catalogName, testBucketPath, properties, glue, fileIO);
  }

  @AfterClass
  public static void afterClass() {
    AwsIntegTestUtil.cleanGlueCatalog(glue, namespaces);
    AwsIntegTestUtil.cleanS3Bucket(s3, testBucketName, testPathPrefix);
  }

  public static String getRandomName() {
    return UUID.randomUUID().toString().replace("-", "");
  }

  public static String createNamespace() {
    String namespace = getRandomName();
    namespaces.add(namespace);
    glueCatalog.createNamespace(Namespace.of(namespace));
    return namespace;
  }

  public static String createTable(String namespace) {
    String tableName = getRandomName();
    return createTable(namespace, tableName);
  }

  public static String createTable(String namespace, String tableName) {
    glueCatalog.createTable(TableIdentifier.of(namespace, tableName), schema, partitionSpec);
    return tableName;
  }
}
