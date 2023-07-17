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

import static org.mockito.AdditionalAnswers.delegatesTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.adobe.testing.s3mock.junit5.S3MockExtension;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.hadoop.conf.Configurable;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TestHelpers;
import org.apache.iceberg.aws.AwsProperties;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.io.BulkDeletionFailureException;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.FileIOParser;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.jdbc.JdbcCatalog;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Streams;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.SerializableSupplier;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.BucketAlreadyExistsException;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectsRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectsResponse;
import software.amazon.awssdk.services.s3.model.S3Error;

@ExtendWith(S3MockExtension.class)
public class TestS3FileIO {
  @RegisterExtension
  public static final S3MockExtension S3_MOCK = S3MockExtension.builder().silent().build();

  public SerializableSupplier<S3Client> s3 = S3_MOCK::createS3ClientV2;
  private final S3Client s3mock = mock(S3Client.class, delegatesTo(s3.get()));
  private final Random random = new Random(1);
  private final int numBucketsForBatchDeletion = 3;
  private final String batchDeletionBucketPrefix = "batch-delete-";
  private final int batchDeletionSize = 5;
  private S3FileIO s3FileIO;

  private final Map<String, String> properties =
      ImmutableMap.of(
          "s3.write.tags.tagKey1",
          "TagValue1",
          "s3.delete.batch-size",
          Integer.toString(batchDeletionSize));

  @BeforeEach
  public void before() {
    s3FileIO = new S3FileIO(() -> s3mock);
    s3FileIO.initialize(properties);
    createBucket("bucket");
    for (int i = 1; i <= numBucketsForBatchDeletion; i++) {
      createBucket(batchDeletionBucketPrefix + i);
    }
    StaticClientFactory.client = s3mock;
  }

  @AfterEach
  public void after() {
    if (null != s3FileIO) {
      s3FileIO.close();
    }
  }

  @Test
  public void testNewInputFile() throws IOException {
    String location = "s3://bucket/path/to/file.txt";
    byte[] expected = new byte[1024 * 1024];
    random.nextBytes(expected);

    InputFile in = s3FileIO.newInputFile(location);
    Assertions.assertThat(in.exists()).isFalse();

    OutputFile out = s3FileIO.newOutputFile(location);
    try (OutputStream os = out.createOrOverwrite()) {
      IOUtils.write(expected, os);
    }

    Assertions.assertThat(in.exists()).isTrue();
    byte[] actual;

    try (InputStream is = in.newStream()) {
      actual = IOUtils.readFully(is, expected.length);
    }

    Assertions.assertThat(actual).isEqualTo(expected);

    s3FileIO.deleteFile(in);

    Assertions.assertThat(s3FileIO.newInputFile(location).exists()).isFalse();
  }

  @Test
  public void testDeleteFilesMultipleBatches() {
    testBatchDelete(batchDeletionSize * 2);
  }

  @Test
  public void testDeleteFilesLessThanBatchSize() {
    testBatchDelete(batchDeletionSize - 1);
  }

  @Test
  public void testDeleteFilesSingleBatchWithRemainder() {
    testBatchDelete(batchDeletionSize + 1);
  }

  @Test
  public void testDeleteEmptyList() throws IOException {
    String location = "s3://bucket/path/to/file.txt";
    InputFile in = s3FileIO.newInputFile(location);
    Assertions.assertThat(in.exists()).isFalse();
    OutputFile out = s3FileIO.newOutputFile(location);
    try (OutputStream os = out.createOrOverwrite()) {
      IOUtils.write(new byte[1024 * 1024], os);
    }

    s3FileIO.deleteFiles(Lists.newArrayList());

    Assertions.assertThat(s3FileIO.newInputFile(location).exists()).isTrue();
    s3FileIO.deleteFile(in);
    Assertions.assertThat(s3FileIO.newInputFile(location).exists()).isFalse();
  }

  @Test
  public void testDeleteFilesS3ReturnsError() {
    String location = "s3://bucket/path/to/file-to-delete.txt";
    DeleteObjectsResponse deleteObjectsResponse =
        DeleteObjectsResponse.builder()
            .errors(ImmutableList.of(S3Error.builder().key("path/to/file.txt").build()))
            .build();
    doReturn(deleteObjectsResponse).when(s3mock).deleteObjects((DeleteObjectsRequest) any());

    Assertions.assertThatThrownBy(() -> s3FileIO.deleteFiles(Lists.newArrayList(location)))
        .isInstanceOf(BulkDeletionFailureException.class)
        .hasMessage("Failed to delete 1 files");
  }

  private void testBatchDelete(int numObjects) {
    List<String> paths = Lists.newArrayList();
    for (int i = 1; i <= numBucketsForBatchDeletion; i++) {
      String bucketName = batchDeletionBucketPrefix + i;
      for (int j = 1; j <= numObjects; j++) {
        String key = "object-" + j;
        paths.add("s3://" + bucketName + "/" + key);
        s3mock.putObject(
            builder -> builder.bucket(bucketName).key(key).build(), RequestBody.empty());
      }
    }
    s3FileIO.deleteFiles(paths);

    int expectedNumberOfBatchesPerBucket =
        (numObjects / batchDeletionSize) + (numObjects % batchDeletionSize == 0 ? 0 : 1);
    int expectedDeleteRequests = expectedNumberOfBatchesPerBucket * numBucketsForBatchDeletion;
    verify(s3mock, times(expectedDeleteRequests)).deleteObjects((DeleteObjectsRequest) any());
    for (String path : paths) {
      Assertions.assertThat(s3FileIO.newInputFile(path).exists()).isFalse();
    }
  }

  @Test
  public void testSerializeClient() {
    SerializableSupplier<S3Client> pre =
        () ->
            S3Client.builder()
                .httpClientBuilder(UrlConnectionHttpClient.builder())
                .region(Region.US_EAST_1)
                .build();

    byte[] data = SerializationUtils.serialize(pre);
    SerializableSupplier<S3Client> post = SerializationUtils.deserialize(data);

    Assertions.assertThat(post.get().serviceName()).isEqualTo("s3");
  }

  @Test
  public void testPrefixList() {
    String prefix = "s3://bucket/path/to/list";

    List<Integer> scaleSizes = Lists.newArrayList(1, 1000, 2500);

    scaleSizes
        .parallelStream()
        .forEach(
            scale -> {
              String scalePrefix = String.format("%s/%s/", prefix, scale);

              createRandomObjects(scalePrefix, scale);
              Assertions.assertThat(Streams.stream(s3FileIO.listPrefix(scalePrefix)).count())
                  .isEqualTo((long) scale);
            });

    long totalFiles = scaleSizes.stream().mapToLong(Integer::longValue).sum();
    Assertions.assertThat(Streams.stream(s3FileIO.listPrefix(prefix)).count())
        .isEqualTo(totalFiles);
  }

  /**
   * Ignoring because the test is flaky, failing with 500s from S3Mock. Coverage of prefix delete
   * exists through integration tests.
   */
  @Test
  @Disabled
  public void testPrefixDelete() {
    String prefix = "s3://bucket/path/to/delete";
    List<Integer> scaleSizes = Lists.newArrayList(0, 5, 1001);

    scaleSizes.forEach(
        scale -> {
          String scalePrefix = String.format("%s/%s/", prefix, scale);

          createRandomObjects(scalePrefix, scale);
          s3FileIO.deletePrefix(scalePrefix);
          Assertions.assertThat(Streams.stream(s3FileIO.listPrefix(scalePrefix)).count())
              .isEqualTo(0);
        });
  }

  @Test
  public void testReadMissingLocation() {
    String location = "s3://bucket/path/to/data.parquet";
    InputFile in = s3FileIO.newInputFile(location);

    Assertions.assertThatThrownBy(() -> in.newStream().read())
        .isInstanceOf(NotFoundException.class)
        .hasMessage("Location does not exist: " + location);
  }

  @Test
  public void testMissingTableMetadata() {
    Map<String, String> conf = Maps.newHashMap();
    conf.put(
        CatalogProperties.URI,
        "jdbc:sqlite:file::memory:?ic" + UUID.randomUUID().toString().replace("-", ""));
    conf.put(JdbcCatalog.PROPERTY_PREFIX + "username", "user");
    conf.put(JdbcCatalog.PROPERTY_PREFIX + "password", "password");
    conf.put(CatalogProperties.WAREHOUSE_LOCATION, "s3://bucket/warehouse");
    conf.put(CatalogProperties.FILE_IO_IMPL, S3FileIO.class.getName());
    conf.put(AwsProperties.CLIENT_FACTORY, StaticClientFactory.class.getName());

    try (JdbcCatalog catalog = new JdbcCatalog()) {
      catalog.initialize("test_jdbc_catalog", conf);

      Schema schema = new Schema(Types.NestedField.required(1, "id", Types.LongType.get()));
      TableIdentifier ident = TableIdentifier.of("table_name");
      BaseTable table = (BaseTable) catalog.createTable(ident, schema);

      // delete the current metadata
      s3FileIO.deleteFile(table.operations().current().metadataFileLocation());

      long start = System.currentTimeMillis();
      // to test NotFoundException, load the table again. refreshing the existing table doesn't
      // require reading metadata
      Assertions.assertThatThrownBy(() -> catalog.loadTable(ident))
          .isInstanceOf(NotFoundException.class)
          .hasMessageStartingWith("Location does not exist");

      long duration = System.currentTimeMillis() - start;
      Assertions.assertThat(duration < 10_000).as("Should take less than 10 seconds").isTrue();
    }
  }

  @Test
  public void testFileIOJsonSerialization() {
    Object conf;
    if (s3FileIO instanceof Configurable) {
      conf = ((Configurable) s3FileIO).getConf();
    } else {
      conf = null;
    }

    String json = FileIOParser.toJson(s3FileIO);
    try (FileIO deserialized = FileIOParser.fromJson(json, conf)) {
      Assertions.assertThat(deserialized).isInstanceOf(S3FileIO.class);
      Assertions.assertThat(deserialized.properties()).isEqualTo(s3FileIO.properties());
    }
  }

  @Test
  public void testS3FileIOKryoSerialization() throws IOException {
    FileIO testS3FileIO = new S3FileIO();

    // s3 fileIO should be serializable when properties are passed as immutable map
    testS3FileIO.initialize(ImmutableMap.of("k1", "v1"));
    FileIO roundTripSerializedFileIO = TestHelpers.KryoHelpers.roundTripSerialize(testS3FileIO);

    Assertions.assertThat(roundTripSerializedFileIO.properties())
        .isEqualTo(testS3FileIO.properties());
  }

  @Test
  public void testS3FileIOWithEmptyPropsKryoSerialization() throws IOException {
    FileIO testS3FileIO = new S3FileIO();

    // s3 fileIO should be serializable when properties passed as empty immutable map
    testS3FileIO.initialize(ImmutableMap.of());
    FileIO roundTripSerializedFileIO = TestHelpers.KryoHelpers.roundTripSerialize(testS3FileIO);

    Assertions.assertThat(roundTripSerializedFileIO.properties())
        .isEqualTo(testS3FileIO.properties());
  }

  @Test
  public void testS3FileIOJavaSerialization() throws IOException, ClassNotFoundException {
    FileIO testS3FileIO = new S3FileIO();

    // s3 fileIO should be serializable when properties are passed as immutable map
    testS3FileIO.initialize(ImmutableMap.of("k1", "v1"));
    FileIO roundTripSerializedFileIO = TestHelpers.roundTripSerialize(testS3FileIO);

    Assertions.assertThat(roundTripSerializedFileIO.properties())
        .isEqualTo(testS3FileIO.properties());
  }

  private void createRandomObjects(String prefix, int count) {
    S3URI s3URI = new S3URI(prefix);

    random
        .ints(count)
        .parallel()
        .forEach(
            i ->
                s3mock.putObject(
                    builder -> builder.bucket(s3URI.bucket()).key(s3URI.key() + i).build(),
                    RequestBody.empty()));
  }

  private void createBucket(String bucketName) {
    try {
      s3.get().createBucket(CreateBucketRequest.builder().bucket(bucketName).build());
    } catch (BucketAlreadyExistsException e) {
      // do nothing
    }
  }
}
