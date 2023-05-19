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
package org.apache.iceberg.aliyun.oss.mock;

import com.aliyun.oss.OSSErrorCode;
import com.aliyun.oss.model.Bucket;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.apache.directory.api.util.Hex;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.io.ByteStreams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;

@Component
public class AliyunOSSMockLocalStore {
  private static final Logger LOG = LoggerFactory.getLogger(AliyunOSSMockLocalStore.class);

  private static final String DATA_FILE = ".DATA";
  private static final String META_FILE = ".META";

  private final File root;

  private final ObjectMapper objectMapper = new ObjectMapper();

  public AliyunOSSMockLocalStore(
      @Value("${" + AliyunOSSMockApp.PROP_ROOT_DIR + ":}") String rootDir) {
    Preconditions.checkNotNull(rootDir, "Root directory cannot be null");
    this.root = new File(rootDir);

    root.deleteOnExit();
    root.mkdirs();

    LOG.info("Root directory of local OSS store is {}", root);
  }

  static String md5sum(String filepath) throws IOException {
    try (InputStream is = new FileInputStream(filepath)) {
      return md5sum(is);
    }
  }

  static String md5sum(InputStream is) throws IOException {
    MessageDigest md;
    try {
      md = MessageDigest.getInstance("MD5");
      md.reset();
    } catch (NoSuchAlgorithmException e) {
      throw new RuntimeException(e);
    }
    byte[] bytes = new byte[1024];
    int numBytes;

    while ((numBytes = is.read(bytes)) != -1) {
      md.update(bytes, 0, numBytes);
    }
    return new String(Hex.encodeHex(md.digest())).toUpperCase(Locale.ROOT);
  }

  private static void inputStreamToFile(InputStream inputStream, File targetFile)
      throws IOException {
    try (OutputStream outputStream = new FileOutputStream(targetFile)) {
      ByteStreams.copy(inputStream, outputStream);
    }
  }

  void createBucket(String bucketName) throws IOException {
    File newBucket = new File(root, bucketName);
    FileUtils.forceMkdir(newBucket);
  }

  Bucket getBucket(String bucketName) {
    List<Bucket> buckets =
        findBucketsByFilter(
            file -> Files.isDirectory(file) && file.getFileName().endsWith(bucketName));

    return buckets.size() > 0 ? buckets.get(0) : null;
  }

  void deleteBucket(String bucketName) throws IOException {
    Bucket bucket = getBucket(bucketName);
    Preconditions.checkNotNull(bucket, "Bucket %s shouldn't be null.", bucketName);

    File dir = new File(root, bucket.getName());
    if (Files.walk(dir.toPath()).anyMatch(p -> p.toFile().isFile())) {
      throw new AliyunOSSMockLocalController.OssException(
          409, OSSErrorCode.BUCKET_NOT_EMPTY, "The bucket you tried to delete is not empty. ");
    }

    FileUtils.deleteDirectory(dir);
  }

  ObjectMetadata putObject(
      String bucketName,
      String fileName,
      InputStream dataStream,
      String contentType,
      String contentEncoding,
      Map<String, String> userMetaData)
      throws IOException {
    File bucketDir = new File(root, bucketName);
    assert bucketDir.exists() || bucketDir.mkdirs();

    File dataFile = new File(bucketDir, fileName + DATA_FILE);
    File metaFile = new File(bucketDir, fileName + META_FILE);
    if (!dataFile.exists()) {
      dataFile.getParentFile().mkdirs();
      dataFile.createNewFile();
    }

    inputStreamToFile(dataStream, dataFile);

    ObjectMetadata metadata = new ObjectMetadata();
    metadata.setContentLength(dataFile.length());
    metadata.setContentMD5(md5sum(dataFile.getAbsolutePath()));
    metadata.setContentType(
        contentType != null ? contentType : MediaType.APPLICATION_OCTET_STREAM_VALUE);
    metadata.setContentEncoding(contentEncoding);
    metadata.setDataFile(dataFile.getAbsolutePath());
    metadata.setMetaFile(metaFile.getAbsolutePath());

    BasicFileAttributes attributes =
        Files.readAttributes(dataFile.toPath(), BasicFileAttributes.class);
    metadata.setLastModificationDate(attributes.lastModifiedTime().toMillis());

    metadata.setUserMetaData(userMetaData);

    objectMapper.writeValue(metaFile, metadata);

    return metadata;
  }

  void deleteObject(String bucketName, String filename) {
    File bucketDir = new File(root, bucketName);
    assert bucketDir.exists();

    File dataFile = new File(bucketDir, filename + DATA_FILE);
    File metaFile = new File(bucketDir, filename + META_FILE);
    assert !dataFile.exists() || dataFile.delete();
    assert !metaFile.exists() || metaFile.delete();
  }

  ObjectMetadata getObjectMetadata(String bucketName, String filename) throws IOException {
    File bucketDir = new File(root, bucketName);
    assert bucketDir.exists();

    File dataFile = new File(bucketDir, filename + DATA_FILE);
    if (!dataFile.exists()) {
      return null;
    }

    File metaFile = new File(bucketDir, filename + META_FILE);
    return objectMapper.readValue(metaFile, ObjectMetadata.class);
  }

  private List<Bucket> findBucketsByFilter(final DirectoryStream.Filter<Path> filter) {
    List<Bucket> buckets = Lists.newArrayList();

    try (DirectoryStream<Path> stream = Files.newDirectoryStream(root.toPath(), filter)) {
      for (final Path path : stream) {
        buckets.add(new Bucket(path.getFileName().toString()));
      }
    } catch (final IOException e) {
      LOG.error("Could not iterate over Bucket-Folders", e);
    }

    return buckets;
  }
}
