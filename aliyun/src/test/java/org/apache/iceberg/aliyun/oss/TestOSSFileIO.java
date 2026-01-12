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
package org.apache.iceberg.aliyun.oss;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.aliyun.oss.OSS;
import com.aliyun.oss.OSSClient;
import com.aliyun.oss.OSSClientBuilder;
import com.aliyun.oss.model.DeleteObjectsRequest;
import com.aliyun.oss.model.DeleteObjectsResult;
import com.aliyun.oss.model.ListObjectsV2Request;
import com.aliyun.oss.model.ListObjectsV2Result;
import com.aliyun.oss.model.OSSObjectSummary;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.aliyun.AliyunProperties;
import org.apache.iceberg.common.DynMethods;
import org.apache.iceberg.io.BulkDeletionFailureException;
import org.apache.iceberg.io.DelegateFileIO;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.FileInfo;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.ResolvingFileIO;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.io.ByteStreams;
import org.apache.iceberg.util.SerializableSupplier;
import org.apache.iceberg.util.SerializationUtil;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestOSSFileIO extends AliyunOSSTestBase {
  private static final String OSS_IMPL_CLASS = OSSFileIO.class.getName();
  private final Configuration conf = new Configuration();
  private final Random random = ThreadLocalRandom.current();

  private OSSFileIO fileIO;

  @BeforeEach
  public void beforeFile() {
    fileIO =
        new OSSFileIO(
            ossClient(),
            new AliyunProperties(ImmutableMap.of(AliyunProperties.OSS_DELETE_BATCH_SIZE, "2")));
  }

  @AfterEach
  public void afterFile() {
    if (fileIO != null) {
      fileIO.close();
    }
  }

  @Test
  public void testOutputFile() throws IOException {
    String location = randomLocation();
    int dataSize = 1024 * 10;
    byte[] data = randomData(dataSize);

    OutputFile out = fileIO().newOutputFile(location);
    writeOSSData(out, data);

    OSSURI uri = new OSSURI(location);
    assertThat(ossClient().get().doesObjectExist(uri.bucket(), uri.key()))
        .as("OSS file should exist")
        .isTrue();
    assertThat(out.location()).as("Should have expected location").isEqualTo(location);
    assertThat(ossDataLength(uri)).as("Should have expected length").isEqualTo(dataSize);
    assertThat(ossDataContent(uri, dataSize)).as("Should have expected content").isEqualTo(data);
  }

  @Test
  public void testInputFile() throws IOException {
    String location = randomLocation();
    InputFile in = fileIO().newInputFile(location);
    assertThat(in.exists()).as("OSS file should not exist").isFalse();

    int dataSize = 1024 * 10;
    byte[] data = randomData(dataSize);
    OutputFile out = fileIO().newOutputFile(location);
    writeOSSData(out, data);

    assertThat(in.exists()).as("OSS file should exist").isTrue();
    assertThat(in.location()).as("Should have expected location").isEqualTo(location);
    assertThat(in.getLength()).as("Should have expected length").isEqualTo(dataSize);
    assertThat(inFileContent(in, dataSize)).as("Should have expected content").isEqualTo(data);
  }

  @Test
  public void testDeleteFile() throws IOException {
    String location = randomLocation();
    int dataSize = 1024 * 10;
    byte[] data = randomData(dataSize);
    OutputFile out = fileIO().newOutputFile(location);
    writeOSSData(out, data);

    InputFile in = fileIO().newInputFile(location);
    assertThat(in.exists()).as("OSS file should exist").isTrue();

    fileIO().deleteFile(in);
    assertThat(fileIO().newInputFile(location).exists()).as("OSS file should not exist").isFalse();
  }

  @Test
  public void testDeleteFiles() throws IOException {
    List<String> locations = Lists.newArrayList();
    List<byte[]> dataList = Lists.newArrayList();
    int numFiles = 5;
    for (int i = 0; i < numFiles; i++) {
      String location = randomLocation();
      locations.add(location);
      byte[] data = randomData(1024);
      dataList.add(data);
      OutputFile out = fileIO().newOutputFile(location);
      writeOSSData(out, data);
      InputFile inputFile = fileIO().newInputFile(location);
      assertThat(inFileContent(inputFile, 1024))
          .as("OSS file %s Should have expected content", location)
          .isEqualTo(data);
    }
    // Delete files in batches
    fileIO().deleteFiles(locations);
    // Verify all files are deleted
    for (int i = 0; i < numFiles; i++) {
      assertThat(fileIO().newInputFile(locations.get(i)).exists())
          .as("OSS file %d should not exist after bulk delete", i)
          .isFalse();
    }
  }

  @Test
  public void testDeleteFilesAcrossBuckets() throws IOException {
    List<String> buckets = Lists.newArrayList();
    setUpMultipleBuckets(buckets, 3);
    // 5 files in each bucket, delete batch size is 2
    int filesInEachBucket = 5;
    List<String> locations = Lists.newArrayList();
    List<byte[]> dataList = Lists.newArrayList();
    for (String bucket : buckets) {
      for (int j = 0; j < filesInEachBucket; j++) {
        String location = randomLocationInBucket(bucket);
        locations.add(location);
        byte[] data = randomData(1024);
        dataList.add(data);

        OutputFile out = fileIO().newOutputFile(location);
        writeOSSData(out, data);
        InputFile inputFile = fileIO().newInputFile(location);
        assertThat(inFileContent(inputFile, 1024))
            .as("OSS file %s Should have expected content", location)
            .isEqualTo(data);
      }
    }
    fileIO().deleteFiles(locations);
    for (int i = 0; i < locations.size(); i++) {
      assertThat(fileIO().newInputFile(locations.get(i)).exists())
          .as("OSS file %d should not exist after bulk delete", i)
          .isFalse();
    }
    tearDownMultipleBuckets(buckets);
  }

  @Test
  public void testDeletePrefix() throws IOException {
    String testId = UUID.randomUUID().toString();
    String prefix = location("delete-prefix-test-" + testId + "/");
    List<String> locations = Lists.newArrayList();
    int numFiles = 3;
    for (int i = 0; i < numFiles; i++) {
      String location = prefix + "file-" + i + ".dat";
      locations.add(location);
      byte[] data = randomData(1024);
      OutputFile out = fileIO().newOutputFile(location);
      writeOSSData(out, data);
      assertThat(fileIO().newInputFile(location).exists())
          .as("OSS file %d should exist", i)
          .isTrue();
    }
    // Delete all files under the prefix
    fileIO().deletePrefix(prefix);
    // Verify all files are deleted
    for (int i = 0; i < numFiles; i++) {
      assertThat(fileIO().newInputFile(locations.get(i)).exists())
          .as("OSS file %d should not exist after prefix delete", i)
          .isFalse();
    }
  }

  /**
   * Special key to trigger mock server error
   *
   * @see org.apache.iceberg.aliyun.oss.mock.AliyunOSSMock
   */
  static final String SPECIAL_KEY_TRIGGER_MOCK_SERVER_ERROR = "mock-internal-error-key";

  /**
   * Test bulk deletion partial failure scenario.
   *
   * <p>Attempts to delete 2 keys in the same bucket, one valid key and one special key
   * SPECIAL_KEY_TRIGGER_MOCK_SERVER_ERROR which triggers the mock server to fail.
   *
   * <p>expecting {@link BulkDeletionFailureException} to be thrown, and has a message indicates one
   * file failed.
   */
  @Test
  public void testDeleteFilesWithPartialFailure() throws IOException {
    // Create one valid test file
    String validLocation = randomLocation();
    byte[] data = randomData(1024);
    OutputFile out = fileIO().newOutputFile(validLocation);
    writeOSSData(out, data);
    String mockInternalErrorKey = location(SPECIAL_KEY_TRIGGER_MOCK_SERVER_ERROR);
    List<String> locationsToDelete = Lists.newArrayList(validLocation, mockInternalErrorKey);
    assertThatThrownBy(() -> fileIO().deleteFiles(locationsToDelete))
        .isInstanceOf(BulkDeletionFailureException.class)
        .hasMessage("Failed to delete 1 files");
  }

  /**
   * Test bulk deletion failure when attempting to delete files across 2 buckets.
   *
   * <p>The first bucket has a SPECIAL_KEY_TRIGGER_MOCK_SERVER_ERROR key. The second is an invalid
   * bucket, causing the deleteObjects request to fail.
   *
   * <p>expecting {@link BulkDeletionFailureException} to be thrown, and has a message indicates 2
   * files failed.
   */
  @Test
  public void testDeleteFilesWithRequestFailure() throws IOException {
    // Create one valid test file
    String validLocation = randomLocation();
    byte[] data = randomData(1024);
    OutputFile out = fileIO().newOutputFile(validLocation);
    writeOSSData(out, data);
    // special key to fail one file deletion
    String mockInternalErrorKey = location(SPECIAL_KEY_TRIGGER_MOCK_SERVER_ERROR);
    // invalid bucket to fail one request of OSS.deleteObjects
    String invalidKeyToDelete = "oss://invalid-bucket/invalid-key-to-delete";
    List<String> locationsToDelete =
        Lists.newArrayList(mockInternalErrorKey, invalidKeyToDelete, validLocation);
    assertThatThrownBy(() -> fileIO().deleteFiles(locationsToDelete))
        .isInstanceOf(BulkDeletionFailureException.class)
        .hasMessage("Failed to delete 2 files");
  }

  /**
   * Test deleteObjects batching behavior with mock verification.
   *
   * <p>This test verifies that {@link OSSFileIO#deleteFiles(Iterable)} correctly batches delete
   * operations according to the configured {@link AliyunProperties#OSS_DELETE_BATCH_SIZE}. It uses
   * a mock OSS client to count the number of {@code deleteObjects} calls.
   *
   * <p>Test setup:
   *
   * <ul>
   *   <li>Creates 7 files in a single bucket
   *   <li>Configures batch size to 3
   *   <li>Expected: 3 batches (3 + 3 + 1 files) = 3 total {@code deleteObjects} calls
   * </ul>
   */
  @Test
  public void testDeleteObjectsBatches() {
    OSS mockOssClient = mock(OSS.class);
    DeleteObjectsResult mockResult = mock(DeleteObjectsResult.class);
    // Mock deleteObjects to return successful result with all keys reported as deleted
    when(mockOssClient.deleteObjects(any(DeleteObjectsRequest.class)))
        .thenAnswer(
            invocation -> {
              DeleteObjectsRequest request = invocation.getArgument(0);
              List<String> deletedKeys = request.getKeys();
              when(mockResult.getDeletedObjects()).thenReturn(deletedKeys);
              return mockResult;
            });
    // Create OSSFileIO with mock client and batch size of 3
    AliyunProperties properties =
        new AliyunProperties(ImmutableMap.of(AliyunProperties.OSS_DELETE_BATCH_SIZE, "3"));
    OSSFileIO mockFileIO = new OSSFileIO(() -> mockOssClient, properties);
    // delete 7 files
    List<String> locations = Lists.newArrayList();
    for (int i = 0; i < 7; i++) {
      String location = location(String.valueOf(i));
      locations.add(location);
    }
    mockFileIO.deleteFiles(locations);
    // Verify deleteObjects was called 3 times
    verify(mockOssClient, times(3)).deleteObjects(any(DeleteObjectsRequest.class));
  }

  /**
   * Test listPrefix pagination behavior with mock verification.
   *
   * <p>This test verifies that {@link OSSFileIO#listPrefix(String)} correctly handles paginated
   * responses from the OSS listObjectsV2 API. It uses a mock OSS client to simulate multiple pages
   * of results and verifies the number of API calls.
   *
   * <p>Test setup:
   *
   * <ul>
   *   <li>Creates a mock that returns 3 pages of results (2 objects per page)
   *   <li>Expected: 3 calls to {@code listObjectsV2}
   * </ul>
   */
  @Test
  public void testListPrefixPagination() {
    OSS mockOssClient = mock(OSS.class);
    ListObjectsV2Result page1 = mock(ListObjectsV2Result.class);
    ListObjectsV2Result page2 = mock(ListObjectsV2Result.class);
    ListObjectsV2Result page3 = mock(ListObjectsV2Result.class);
    // Page 1: 2 objects, has more pages
    OSSObjectSummary obj1 = mock(OSSObjectSummary.class);
    OSSObjectSummary obj2 = mock(OSSObjectSummary.class);
    when(obj1.getKey()).thenReturn("file1.txt");
    when(obj1.getSize()).thenReturn(100L);
    when(obj1.getLastModified()).thenReturn(new java.util.Date());
    when(obj2.getKey()).thenReturn("file2.txt");
    when(obj2.getSize()).thenReturn(200L);
    when(obj2.getLastModified()).thenReturn(new java.util.Date());

    when(page1.getObjectSummaries()).thenReturn(Lists.newArrayList(obj1, obj2));
    when(page1.getNextContinuationToken()).thenReturn("token1");
    when(page1.isTruncated()).thenReturn(true);

    // Page 2: 2 objects, has more pages
    OSSObjectSummary obj3 = mock(OSSObjectSummary.class);
    OSSObjectSummary obj4 = mock(OSSObjectSummary.class);
    when(obj3.getKey()).thenReturn("file3.txt");
    when(obj3.getSize()).thenReturn(300L);
    when(obj3.getLastModified()).thenReturn(new java.util.Date());
    when(obj4.getKey()).thenReturn("file4.txt");
    when(obj4.getSize()).thenReturn(400L);
    when(obj4.getLastModified()).thenReturn(new java.util.Date());

    when(page2.getObjectSummaries()).thenReturn(Lists.newArrayList(obj3, obj4));
    when(page2.getNextContinuationToken()).thenReturn("token2");
    when(page2.isTruncated()).thenReturn(true);

    // Page 3: 1 object, no more pages
    OSSObjectSummary obj5 = mock(OSSObjectSummary.class);
    when(obj5.getKey()).thenReturn("file5.txt");
    when(obj5.getSize()).thenReturn(500L);
    when(obj5.getLastModified()).thenReturn(new java.util.Date());

    when(page3.getObjectSummaries()).thenReturn(Lists.newArrayList(obj5));
    when(page3.getNextContinuationToken()).thenReturn(null);
    when(page3.isTruncated()).thenReturn(false);

    // Mock listObjectsV2 calls - first call returns page1, second call (with token) returns page2,
    // third call returns page3
    when(mockOssClient.listObjectsV2(any(ListObjectsV2Request.class)))
        .thenAnswer(
            invocation -> {
              ListObjectsV2Request request = invocation.getArgument(0);
              if (request.getContinuationToken() == null) {
                return page1;
              } else if ("token1".equals(request.getContinuationToken())) {
                return page2;
              } else if ("token2".equals(request.getContinuationToken())) {
                return page3;
              }
              return page3;
            });
    OSSFileIO mockFileIO = new OSSFileIO(() -> mockOssClient);
    Iterable<FileInfo> result = mockFileIO.listPrefix("oss://test-bucket/prefix/");
    List<FileInfo> allFiles = Lists.newArrayList(result);
    assertThat(allFiles).hasSize(5);
    // Verify listObjectsV2 was called 3 times
    verify(mockOssClient, times(3)).listObjectsV2(any(ListObjectsV2Request.class));
  }

  @Test
  public void testListPrefix() throws IOException {
    String testId = UUID.randomUUID().toString();
    String prefix = location("list-prefix-test-" + testId + "/");
    // Test empty prefix
    assertThat(fileIO().listPrefix(prefix)).isEmpty();

    // Create 5 test files
    List<String> locations =
        IntStream.range(0, 5)
            .mapToObj(i -> prefix + "file-" + i + ".dat")
            .collect(Collectors.toList());
    for (String location : locations) {
      try (OSSFileIO ossFileIO = new OSSFileIO(ossClient())) {
        OutputFile out = ossFileIO.newOutputFile(location);
        writeOSSData(out, randomData(1024));
      }
    }

    // Test list prefix: 5 total, page size 2
    List<FileInfo> fileInfos = Lists.newArrayList(fileIO().listPrefix(prefix));
    assertThat(fileInfos).hasSize(5);

    for (FileInfo fileInfo : fileInfos) {
      assertThat(fileInfo.location()).startsWith(prefix);
      assertThat(fileInfo.size()).isEqualTo(1024);
      assertThat(fileInfo.createdAtMillis()).isGreaterThan(0);
    }

    // Test with more specific prefix: 1 total, page size 2
    String specificPrefix = prefix + "file-1";
    List<FileInfo> specificFileInfos = Lists.newArrayList(fileIO().listPrefix(specificPrefix));
    assertThat(specificFileInfos).hasSize(1);
    assertThat(specificFileInfos.get(0).location()).startsWith(specificPrefix);
  }

  @Test
  public void testLoadFileIO() {
    FileIO file = CatalogUtil.loadFileIO(OSS_IMPL_CLASS, ImmutableMap.of(), conf);
    assertThat(file).as("Should be OSSFileIO").isInstanceOf(OSSFileIO.class);

    byte[] data = SerializationUtil.serializeToBytes(file);
    FileIO expectedFileIO = SerializationUtil.deserializeFromBytes(data);
    assertThat(expectedFileIO)
        .as("The deserialized FileIO should be OSSFileIO")
        .isInstanceOf(OSSFileIO.class);
  }

  @Test
  public void testLoadFileIOWithBatchSize() throws Exception {
    // Test loading FileIO with custom delete batch size configuration
    int customBatchSize = 500;
    Map<String, String> properties =
        ImmutableMap.of(AliyunProperties.OSS_DELETE_BATCH_SIZE, String.valueOf(customBatchSize));
    FileIO file = CatalogUtil.loadFileIO(OSS_IMPL_CLASS, properties, conf);
    assertThat(file).as("Should be OSSFileIO").isInstanceOf(OSSFileIO.class);

    OSSFileIO ossFileIO = (OSSFileIO) file;
    java.lang.reflect.Field aliyunPropertiesField =
        OSSFileIO.class.getDeclaredField("aliyunProperties");
    aliyunPropertiesField.setAccessible(true);
    AliyunProperties aliyunProperties = (AliyunProperties) aliyunPropertiesField.get(ossFileIO);
    assertThat(aliyunProperties.ossDeleteBatchSize())
        .as("Batch size should be configured to custom value")
        .isEqualTo(customBatchSize);

    // Verify serialization/deserialization works with configured properties
    byte[] data = SerializationUtil.serializeToBytes(file);
    FileIO deserializedFileIO = SerializationUtil.deserializeFromBytes(data);
    assertThat(deserializedFileIO)
        .as("The deserialized FileIO should be OSSFileIO")
        .isInstanceOf(OSSFileIO.class);
    aliyunProperties = (AliyunProperties) aliyunPropertiesField.get(deserializedFileIO);
    assertThat(aliyunProperties.ossDeleteBatchSize())
        .as("Deserialized FileIO Batch size should be configured to custom value")
        .isEqualTo(customBatchSize);
  }

  @Test
  public void testResolvingFileIOLoad() {
    ResolvingFileIO resolvingFileIO = new ResolvingFileIO();
    resolvingFileIO.setConf(conf);
    resolvingFileIO.initialize(ImmutableMap.of());
    FileIO result =
        DynMethods.builder("io")
            .hiddenImpl(ResolvingFileIO.class, String.class)
            .build(resolvingFileIO)
            .invoke("oss://foo/bar");
    assertThat(result).isInstanceOf(OSSFileIO.class);
    assertThat(result).isInstanceOf(DelegateFileIO.class);
  }

  @Test
  public void serializeClient() throws URISyntaxException {
    String endpoint = "iceberg-test-oss.aliyun.com";
    String accessKeyId = UUID.randomUUID().toString();
    String accessSecret = UUID.randomUUID().toString();
    SerializableSupplier<OSS> pre =
        () -> new OSSClientBuilder().build(endpoint, accessKeyId, accessSecret);

    byte[] data = SerializationUtil.serializeToBytes(pre);
    SerializableSupplier<OSS> post = SerializationUtil.deserializeFromBytes(data);

    OSS client = post.get();
    assertThat(client).as("Should be instance of oss client").isInstanceOf(OSSClient.class);

    OSSClient oss = (OSSClient) client;
    assertThat(oss.getEndpoint())
        .as("Should have expected endpoint")
        .isEqualTo(new URI("http://" + endpoint));

    assertThat(oss.getCredentialsProvider().getCredentials().getAccessKeyId())
        .as("Should have expected access key")
        .isEqualTo(accessKeyId);
    assertThat(oss.getCredentialsProvider().getCredentials().getSecretAccessKey())
        .as("Should have expected secret key")
        .isEqualTo(accessSecret);
    assertThat(oss.getCredentialsProvider().getCredentials().getSecurityToken())
        .as("Should have no security token")
        .isNull();
  }

  private OSSFileIO fileIO() {
    return fileIO;
  }

  private String randomLocation() {
    return location(String.format("%s.dat", UUID.randomUUID()));
  }

  private String randomLocationInBucket(String bucket) {
    return location(bucket, String.format("%s.dat", UUID.randomUUID()));
  }

  private byte[] randomData(int size) {
    byte[] data = new byte[size];
    random.nextBytes(data);
    return data;
  }

  private long ossDataLength(OSSURI uri) {
    return ossClient()
        .get()
        .getObject(uri.bucket(), uri.key())
        .getObjectMetadata()
        .getContentLength();
  }

  private byte[] ossDataContent(OSSURI uri, int dataSize) throws IOException {
    try (InputStream is = ossClient().get().getObject(uri.bucket(), uri.key()).getObjectContent()) {
      byte[] actual = new byte[dataSize];
      ByteStreams.readFully(is, actual);
      return actual;
    }
  }

  private void writeOSSData(OutputFile out, byte[] data) throws IOException {
    try (OutputStream os = out.create();
        InputStream is = new ByteArrayInputStream(data)) {
      ByteStreams.copy(is, os);
    }
  }

  private byte[] inFileContent(InputFile in, int dataSize) throws IOException {
    try (InputStream is = in.newStream()) {
      byte[] actual = new byte[dataSize];
      ByteStreams.readFully(is, actual);
      return actual;
    }
  }

  private void setUpMultipleBuckets(List<String> bucketsResult, int bucketNum) {
    for (int i = 0; i < bucketNum; i++) {
      String bucketName = "delegate-oss-file-io-test-" + UUID.randomUUID();
      bucketsResult.add(bucketName);
      setUpBucket(bucketName);
    }
  }

  private void tearDownMultipleBuckets(List<String> bucketsResult) {
    for (String bucket : bucketsResult) {
      tearDownBucket(bucket);
    }
  }
}
