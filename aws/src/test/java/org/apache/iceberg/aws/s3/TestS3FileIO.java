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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.AdditionalAnswers.delegatesTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.ManifestWriter;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TestHelpers;
import org.apache.iceberg.aws.AwsProperties;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.common.DynMethods;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.io.BulkDeletionFailureException;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.FileIOParser;
import org.apache.iceberg.io.FileInfo;
import org.apache.iceberg.io.IOUtil;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.ResolvingFileIO;
import org.apache.iceberg.jdbc.JdbcCatalog;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Streams;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.SerializableSupplier;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.testcontainers.containers.MinIOContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.BucketAlreadyExistsException;
import software.amazon.awssdk.services.s3.model.BucketAlreadyOwnedByYouException;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectsRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectsResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.S3Error;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.awssdk.services.s3.paginators.ListObjectsV2Iterable;

@Testcontainers
public class TestS3FileIO {
  @Container private final MinIOContainer minio = createMinIOContainer();

  private final SerializableSupplier<S3Client> s3 = () -> MinioUtil.createS3Client(minio);
  private final S3Client s3mock = mock(S3Client.class, delegatesTo(s3.get()));
  private final Random random = new Random(1);
  private final int numBucketsForBatchDeletion = 3;
  private final String batchDeletionBucketPrefix = "batch-delete-";
  private final int batchDeletionSize = 5;
  private S3FileIO s3FileIO;

  private static final String S3_GENERAL_PURPOSE_BUCKET = "bucket";
  private static final String S3_DIRECTORY_BUCKET = "directory-bucket-usw2-az1--x-s3";

  private final Map<String, String> properties =
      ImmutableMap.of(
          "s3.write.tags.tagKey1",
          "TagValue1",
          "s3.delete.batch-size",
          Integer.toString(batchDeletionSize));

  protected MinIOContainer createMinIOContainer() {
    MinIOContainer container = MinioUtil.createContainer();
    container.start();
    return container;
  }

  @BeforeEach
  public void before() {
    s3FileIO = new S3FileIO(() -> s3mock);
    s3FileIO.initialize(properties);
    createBucket(S3_GENERAL_PURPOSE_BUCKET);
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
    assertThat(in.exists()).isFalse();

    OutputFile out = s3FileIO.newOutputFile(location);
    try (OutputStream os = out.createOrOverwrite()) {
      IOUtil.writeFully(os, ByteBuffer.wrap(expected));
    }

    assertThat(in.exists()).isTrue();
    byte[] actual = new byte[1024 * 1024];

    try (InputStream is = in.newStream()) {
      IOUtil.readFully(is, actual, 0, expected.length);
    }

    assertThat(actual).isEqualTo(expected);

    s3FileIO.deleteFile(in);

    assertThat(s3FileIO.newInputFile(location).exists()).isFalse();
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
    assertThat(in.exists()).isFalse();
    OutputFile out = s3FileIO.newOutputFile(location);
    try (OutputStream os = out.createOrOverwrite()) {
      IOUtil.writeFully(os, ByteBuffer.wrap(new byte[1024 * 1024]));
    }

    s3FileIO.deleteFiles(Lists.newArrayList());

    assertThat(s3FileIO.newInputFile(location).exists()).isTrue();
    s3FileIO.deleteFile(in);
    assertThat(s3FileIO.newInputFile(location).exists()).isFalse();
  }

  @Test
  public void testDeleteFilesS3ReturnsError() {
    String location = "s3://bucket/path/to/file-to-delete.txt";
    DeleteObjectsResponse deleteObjectsResponse =
        DeleteObjectsResponse.builder()
            .errors(ImmutableList.of(S3Error.builder().key("path/to/file.txt").build()))
            .build();
    doReturn(deleteObjectsResponse).when(s3mock).deleteObjects((DeleteObjectsRequest) any());

    assertThatThrownBy(() -> s3FileIO.deleteFiles(Lists.newArrayList(location)))
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
      assertThat(s3FileIO.newInputFile(path).exists()).isFalse();
    }
  }

  @Test
  public void testSerializeClient() throws IOException, ClassNotFoundException {
    SerializableSupplier<S3Client> pre =
        () ->
            S3Client.builder()
                .httpClientBuilder(UrlConnectionHttpClient.builder())
                .region(Region.US_EAST_1)
                .build();

    byte[] data = TestHelpers.serialize(pre);
    SerializableSupplier<S3Client> post = TestHelpers.deserialize(data);

    assertThat(post.get().serviceName()).isEqualTo("s3");
  }

  @Test
  public void testPrefixList() {
    String prefix = "s3://bucket/path/to/list";

    List<Integer> scaleSizes = Lists.newArrayList(1, 1000, 2500);

    scaleSizes.parallelStream()
        .forEach(
            scale -> {
              String scalePrefix = String.format("%s/%s/", prefix, scale);

              createRandomObjects(scalePrefix, scale);
              assertThat(Streams.stream(s3FileIO.listPrefix(scalePrefix)).count())
                  .isEqualTo((long) scale);
            });

    long totalFiles = scaleSizes.stream().mapToLong(Integer::longValue).sum();
    assertThat(Streams.stream(s3FileIO.listPrefix(prefix)).count()).isEqualTo(totalFiles);
  }

  /**
   * Tests that we correctly insert the backslash for s3 express buckets. Currently the Adobe S3
   * Mock doesn't cater for express buckets eg. When you call createBucket with s3 express
   * configurations it still just returns a general bucket TODO Update to use S3Mock when it behaves
   * as expected.
   */
  @Test
  public void testPrefixListWithExpressAddSlash() {
    assertPrefixIsAddedCorrectly("path/to/list", properties);

    Map<String, String> newProperties =
        ImmutableMap.of(
            "s3.write.tags.tagKey1",
            "TagValue1",
            "s3.delete.batch-size",
            Integer.toString(batchDeletionSize),
            "s3.directory-bucket.list-prefix-as-directory",
            "true");
    assertPrefixIsAddedCorrectly("path/to/list/", newProperties);
  }

  public void assertPrefixIsAddedCorrectly(String suffix, Map<String, String> props) {
    String prefix = String.format("s3://%s/%s", S3_DIRECTORY_BUCKET, suffix);

    S3Client localMockedClient = mock(S3Client.class);

    List<S3Object> s3Objects =
        Arrays.asList(
            S3Object.builder()
                .key("path/to/list/file1.txt")
                .size(1024L)
                .lastModified(Instant.now())
                .build(),
            S3Object.builder()
                .key("path/to/list/file2.txt")
                .size(2048L)
                .lastModified(Instant.now().minusSeconds(60))
                .build());

    ListObjectsV2Response response = ListObjectsV2Response.builder().contents(s3Objects).build();

    ListObjectsV2Iterable mockedResponse = mock(ListObjectsV2Iterable.class);

    Mockito.when(mockedResponse.stream()).thenReturn(Stream.of(response));

    Mockito.when(
            localMockedClient.listObjectsV2Paginator(
                ListObjectsV2Request.builder()
                    .prefix("path/to/list/")
                    .bucket(S3_DIRECTORY_BUCKET)
                    .build()))
        .thenReturn(mockedResponse);

    // Initialize S3FileIO with the mocked client
    S3FileIO localS3FileIo = new S3FileIO(() -> localMockedClient);
    localS3FileIo.initialize(props);

    // Perform the listing
    List<FileInfo> fileInfoList =
        StreamSupport.stream(
                Spliterators.spliteratorUnknownSize(
                    localS3FileIo.listPrefix(prefix).iterator(), Spliterator.ORDERED),
                false)
            .collect(Collectors.toList());

    // Assert that the returned FileInfo instances match the expected values
    assertEquals(2, fileInfoList.size());
    assertTrue(
        fileInfoList.stream()
            .anyMatch(
                fi ->
                    fi.location().endsWith("file1.txt")
                        && fi.size() == 1024
                        && fi.createdAtMillis() > Instant.now().minusSeconds(120).toEpochMilli()));
    assertTrue(
        fileInfoList.stream()
            .anyMatch(
                fi ->
                    fi.location().endsWith("file2.txt")
                        && fi.size() == 2048
                        && fi.createdAtMillis() < Instant.now().minusSeconds(30).toEpochMilli()));
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
          assertThat(Streams.stream(s3FileIO.listPrefix(scalePrefix)).count()).isEqualTo(0);
        });
  }

  @Test
  public void testReadMissingLocation() {
    String location = "s3://bucket/path/to/data.parquet";
    InputFile in = s3FileIO.newInputFile(location);

    assertThatThrownBy(() -> in.newStream().read())
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
      assertThatThrownBy(() -> catalog.loadTable(ident))
          .isInstanceOf(NotFoundException.class)
          .hasMessageStartingWith("Location does not exist");

      long duration = System.currentTimeMillis() - start;
      assertThat(duration < 10_000).as("Should take less than 10 seconds").isTrue();
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
      assertThat(deserialized).isInstanceOf(S3FileIO.class);
      assertThat(deserialized.properties()).isEqualTo(s3FileIO.properties());
    }
  }

  @Test
  public void testS3FileIOKryoSerialization() throws IOException {
    FileIO testS3FileIO = new S3FileIO();

    // s3 fileIO should be serializable when properties are passed as immutable map
    testS3FileIO.initialize(ImmutableMap.of("k1", "v1"));
    FileIO roundTripSerializedFileIO = TestHelpers.KryoHelpers.roundTripSerialize(testS3FileIO);

    assertThat(roundTripSerializedFileIO.properties()).isEqualTo(testS3FileIO.properties());
  }

  @Test
  public void testS3FileIOWithEmptyPropsKryoSerialization() throws IOException {
    FileIO testS3FileIO = new S3FileIO();

    // s3 fileIO should be serializable when properties passed as empty immutable map
    testS3FileIO.initialize(ImmutableMap.of());
    FileIO roundTripSerializedFileIO = TestHelpers.KryoHelpers.roundTripSerialize(testS3FileIO);

    assertThat(roundTripSerializedFileIO.properties()).isEqualTo(testS3FileIO.properties());
  }

  @Test
  public void testS3FileIOJavaSerialization() throws IOException, ClassNotFoundException {
    FileIO testS3FileIO = new S3FileIO();

    // s3 fileIO should be serializable when properties are passed as immutable map
    testS3FileIO.initialize(ImmutableMap.of("k1", "v1"));
    FileIO roundTripSerializedFileIO = TestHelpers.roundTripSerialize(testS3FileIO);

    assertThat(roundTripSerializedFileIO.properties()).isEqualTo(testS3FileIO.properties());
  }

  @Test
  public void testResolvingFileIOLoad() {
    ResolvingFileIO resolvingFileIO = new ResolvingFileIO();
    resolvingFileIO.setConf(new Configuration());
    resolvingFileIO.initialize(ImmutableMap.of());
    FileIO result =
        DynMethods.builder("io")
            .hiddenImpl(ResolvingFileIO.class, String.class)
            .build(resolvingFileIO)
            .invoke("s3://foo/bar");
    assertThat(result).isInstanceOf(S3FileIO.class);
  }

  @Test
  public void testResolvingFileIOLoadWithoutConf() {
    ResolvingFileIO resolvingFileIO = new ResolvingFileIO();
    resolvingFileIO.initialize(ImmutableMap.of());
    FileIO result =
        DynMethods.builder("io")
            .hiddenImpl(ResolvingFileIO.class, String.class)
            .build(resolvingFileIO)
            .invoke("s3://foo/bar");
    assertThat(result).isInstanceOf(S3FileIO.class);
  }

  @Test
  public void testInputFileWithDataFile() throws IOException {
    String location = "s3://bucket/path/to/data-file.parquet";
    DataFile dataFile =
        DataFiles.builder(PartitionSpec.unpartitioned())
            .withPath(location)
            .withFileSizeInBytes(123L)
            .withFormat(FileFormat.PARQUET)
            .withRecordCount(123L)
            .build();
    OutputStream outputStream = s3FileIO.newOutputFile(location).create();
    byte[] data = "testing".getBytes();
    outputStream.write(data);
    outputStream.close();

    InputFile inputFile = s3FileIO.newInputFile(dataFile);
    reset(s3mock);

    assertThat(inputFile.getLength())
        .as("Data file length should be determined from the file size stats")
        .isEqualTo(123L);
    verify(s3mock, never()).headObject(any(HeadObjectRequest.class));
  }

  @Test
  public void testInputFileWithManifest() throws IOException {
    String dataFileLocation = "s3://bucket/path/to/data-file-2.parquet";
    DataFile dataFile =
        DataFiles.builder(PartitionSpec.unpartitioned())
            .withPath(dataFileLocation)
            .withFileSizeInBytes(123L)
            .withFormat(FileFormat.PARQUET)
            .withRecordCount(123L)
            .build();
    String manifestLocation = "s3://bucket/path/to/manifest.avro";
    OutputFile outputFile = s3FileIO.newOutputFile(manifestLocation);
    ManifestWriter<DataFile> writer =
        ManifestFiles.write(PartitionSpec.unpartitioned(), outputFile);
    writer.add(dataFile);
    writer.close();
    ManifestFile manifest = writer.toManifestFile();
    InputFile inputFile = s3FileIO.newInputFile(manifest);
    reset(s3mock);

    assertThat(inputFile.getLength()).isEqualTo(manifest.length());
    verify(s3mock, never()).headObject(any(HeadObjectRequest.class));
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
    } catch (BucketAlreadyExistsException | BucketAlreadyOwnedByYouException e) {
      // do nothing
    }
  }
}
