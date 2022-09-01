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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.AdditionalAnswers.delegatesTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.adobe.testing.s3mock.junit4.S3MockRule;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.hadoop.conf.Configurable;
import org.apache.iceberg.AssertHelpers;
import org.apache.iceberg.TestHelpers;
import org.apache.iceberg.io.BulkDeletionFailureException;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.FileIOParser;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Streams;
import org.apache.iceberg.util.SerializableSupplier;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectsRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectsResponse;
import software.amazon.awssdk.services.s3.model.S3Error;

@RunWith(MockitoJUnitRunner.class)
public class TestS3FileIO {
  @ClassRule public static final S3MockRule S3_MOCK_RULE = S3MockRule.builder().silent().build();
  public SerializableSupplier<S3Client> s3 = S3_MOCK_RULE::createS3ClientV2;
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

  @Before
  public void before() {
    s3FileIO = new S3FileIO(() -> s3mock);
    s3FileIO.initialize(properties);
    s3.get().createBucket(CreateBucketRequest.builder().bucket("bucket").build());
    for (int i = 1; i <= numBucketsForBatchDeletion; i++) {
      s3.get()
          .createBucket(
              CreateBucketRequest.builder().bucket(batchDeletionBucketPrefix + i).build());
    }
  }

  @Test
  public void testNewInputFile() throws IOException {
    String location = "s3://bucket/path/to/file.txt";
    byte[] expected = new byte[1024 * 1024];
    random.nextBytes(expected);

    InputFile in = s3FileIO.newInputFile(location);
    assertFalse(in.exists());

    OutputFile out = s3FileIO.newOutputFile(location);
    try (OutputStream os = out.createOrOverwrite()) {
      IOUtils.write(expected, os);
    }

    assertTrue(in.exists());
    byte[] actual;

    try (InputStream is = in.newStream()) {
      actual = IOUtils.readFully(is, expected.length);
    }

    assertArrayEquals(expected, actual);

    s3FileIO.deleteFile(in);

    assertFalse(s3FileIO.newInputFile(location).exists());
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
    assertFalse(in.exists());
    OutputFile out = s3FileIO.newOutputFile(location);
    try (OutputStream os = out.createOrOverwrite()) {
      IOUtils.write(new byte[1024 * 1024], os);
    }

    s3FileIO.deleteFiles(Lists.newArrayList());

    Assert.assertTrue(s3FileIO.newInputFile(location).exists());
    s3FileIO.deleteFile(in);
    assertFalse(s3FileIO.newInputFile(location).exists());
  }

  @Test
  public void testDeleteFilesS3ReturnsError() {
    String location = "s3://bucket/path/to/file-to-delete.txt";
    DeleteObjectsResponse deleteObjectsResponse =
        DeleteObjectsResponse.builder()
            .errors(ImmutableList.of(S3Error.builder().key("path/to/file.txt").build()))
            .build();
    doReturn(deleteObjectsResponse).when(s3mock).deleteObjects((DeleteObjectsRequest) any());

    AssertHelpers.assertThrows(
        "A failure during S3 DeleteObjects call should result in FileIODeleteException",
        BulkDeletionFailureException.class,
        "Failed to delete 1 file",
        () -> s3FileIO.deleteFiles(Lists.newArrayList(location)));
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
      Assert.assertFalse(s3FileIO.newInputFile(path).exists());
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

    assertEquals("s3", post.get().serviceName());
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
              assertEquals((long) scale, Streams.stream(s3FileIO.listPrefix(scalePrefix)).count());
            });

    long totalFiles = scaleSizes.stream().mapToLong(Integer::longValue).sum();
    Assertions.assertEquals(totalFiles, Streams.stream(s3FileIO.listPrefix(prefix)).count());
  }

  /**
   * Ignoring because the test is flaky, failing with 500s from S3Mock. Coverage of prefix delete
   * exists through integration tests.
   */
  @Test
  @Ignore
  public void testPrefixDelete() {
    String prefix = "s3://bucket/path/to/delete";
    List<Integer> scaleSizes = Lists.newArrayList(0, 5, 1001);

    scaleSizes.forEach(
        scale -> {
          String scalePrefix = String.format("%s/%s/", prefix, scale);

          createRandomObjects(scalePrefix, scale);
          s3FileIO.deletePrefix(scalePrefix);
          assertEquals(0L, Streams.stream(s3FileIO.listPrefix(scalePrefix)).count());
        });
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
      Assert.assertTrue(deserialized instanceof S3FileIO);
      Assert.assertEquals(s3FileIO.properties(), deserialized.properties());
    }
  }

  @Test
  public void testS3FileIOKryoSerialization() throws IOException {
    FileIO testS3FileIO = new S3FileIO();

    // s3 fileIO should be serializable when properties are passed as immutable map
    testS3FileIO.initialize(ImmutableMap.of("k1", "v1"));
    FileIO roundTripSerializedFileIO = TestHelpers.KryoHelpers.roundTripSerialize(testS3FileIO);

    Assert.assertEquals(testS3FileIO.properties(), roundTripSerializedFileIO.properties());
  }

  @Test
  public void testS3FileIOJavaSerialization() throws IOException, ClassNotFoundException {
    FileIO testS3FileIO = new S3FileIO();

    // s3 fileIO should be serializable when properties are passed as immutable map
    testS3FileIO.initialize(ImmutableMap.of("k1", "v1"));
    FileIO roundTripSerializedFileIO = TestHelpers.roundTripSerialize(testS3FileIO);

    Assert.assertEquals(testS3FileIO.properties(), roundTripSerializedFileIO.properties());
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
}
