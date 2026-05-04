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
package org.apache.iceberg.flink.sink.dynamic;

import static org.assertj.core.api.Assertions.assertThat;

import java.net.URI;
import java.util.Collections;
import java.util.Map;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.data.GenericRowData;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.aws.s3.S3FileIO;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SessionCatalog;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.rest.RESTCatalog;
import org.apache.iceberg.rest.RESTCatalogAdapter;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MinIOContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Configuration;

@Testcontainers
class TestDynamicTableUpdateOperatorWithRESTCatalog {

  private static final String BUCKET = "iceberg-test-bucket";
  private static final String MINIO_IMAGE = "minio/minio:RELEASE.2023-09-04T19-57-37Z";

  @Container
  private static final MinIOContainer MINIO =
      new MinIOContainer(MINIO_IMAGE).withUserName("testuser").withPassword("testpassword");

  private static final Schema SCHEMA =
      new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get()));

  private DynamicTableUpdateOperator operator;
  private RESTCatalog restCatalog;
  private S3Client s3Client;

  @BeforeEach
  void before() throws Exception {
    s3Client =
        S3Client.builder()
            .endpointOverride(URI.create(MINIO.getS3URL()))
            .credentialsProvider(
                StaticCredentialsProvider.create(
                    AwsBasicCredentials.create(MINIO.getUserName(), MINIO.getPassword())))
            .region(Region.US_EAST_1)
            .serviceConfiguration(S3Configuration.builder().pathStyleAccessEnabled(true).build())
            .build();

    s3Client.createBucket(b -> b.bucket(BUCKET));

    Map<String, String> s3Props = Maps.newHashMap();
    s3Props.put("s3.endpoint", MINIO.getS3URL());
    s3Props.put("s3.access-key-id", MINIO.getUserName());
    s3Props.put("s3.secret-access-key", MINIO.getPassword());
    s3Props.put("s3.path-style-access", "true");
    s3Props.put("client.region", "us-east-1");

    InMemoryCatalog backendCatalog = new InMemoryCatalog();
    Map<String, String> backendProps = Maps.newHashMap(s3Props);
    backendProps.put(CatalogProperties.WAREHOUSE_LOCATION, "s3://" + BUCKET + "/warehouse");
    backendProps.put(CatalogProperties.FILE_IO_IMPL, S3FileIO.class.getName());
    backendCatalog.initialize("backend", backendProps);
    backendCatalog.createNamespace(Namespace.of("default"));

    RESTCatalogAdapter adapter = new RESTCatalogAdapter(backendCatalog);
    restCatalog =
        new RESTCatalog(SessionCatalog.SessionContext.createEmpty(), properties -> adapter);
    restCatalog.initialize("test-rest", Maps.newHashMap(backendProps));

    CatalogLoader restCatalogLoader =
        new CatalogLoader() {
          @Override
          public Catalog loadCatalog() {
            return restCatalog;
          }

          @SuppressWarnings({"checkstyle:NoClone", "checkstyle:SuperClone"})
          @Override
          public CatalogLoader clone() {
            return this;
          }
        };

    operator =
        new DynamicTableUpdateOperator(
            restCatalogLoader, TableCreator.DEFAULT, flinkDynamicSinkConfiguration());
  }

  @AfterEach
  void after() throws Exception {
    if (operator != null) {
      // operator.close() closes the catalog (restCatalog) via Closeable check
      operator.close();
    } else if (restCatalog != null) {
      restCatalog.close();
    }

    if (s3Client != null) {
      s3Client.close();
    }
  }

  @Test
  void testOperatorWithS3FileIO() throws Exception {
    operator.open(null);

    DynamicRecordInternal input =
        new DynamicRecordInternal(
            "default.test_table",
            "main",
            SCHEMA,
            GenericRowData.of(1),
            PartitionSpec.unpartitioned(),
            42,
            false,
            Collections.emptySet());

    DynamicRecordInternal output = operator.map(input);
    assertThat(output).isEqualTo(input);
  }

  private static FlinkDynamicSinkConf flinkDynamicSinkConfiguration() {
    Map<String, String> properties = Maps.newHashMap();
    properties.put(FlinkDynamicSinkOptions.CACHE_MAX_SIZE.key(), "10");
    properties.put(FlinkDynamicSinkOptions.CACHE_REFRESH_MS.key(), "1000");
    properties.put(FlinkDynamicSinkOptions.INPUT_SCHEMAS_PER_TABLE_CACHE_MAX_SIZE.key(), "10");
    properties.put(FlinkDynamicSinkOptions.CASE_SENSITIVE.key(), "true");
    properties.put(FlinkDynamicSinkOptions.DROP_UNUSED_COLUMNS.key(), "false");
    return new FlinkDynamicSinkConf(properties, new Configuration());
  }
}
