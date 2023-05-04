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

import org.apache.iceberg.encryption.NativeFileCryptoParameters;
import org.apache.iceberg.encryption.NativelyEncryptedFile;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.SeekableInputStream;
import org.apache.iceberg.metrics.MetricsContext;
import software.amazon.awssdk.services.s3.S3Client;

public class S3InputFile extends BaseS3File implements InputFile, NativelyEncryptedFile {
  private NativeFileCryptoParameters nativeDecryptionParameters;
  private Long length;

  public static S3InputFile fromLocation(
      String location,
      S3Client client,
      S3FileIOProperties s3FileIOProperties,
      MetricsContext metrics) {
    return new S3InputFile(
        client,
        new S3URI(location, s3FileIOProperties.bucketToAccessPointMapping()),
        null,
        s3FileIOProperties,
        metrics);
  }

  public static S3InputFile fromLocation(
      String location,
      long length,
      S3Client client,
      S3FileIOProperties s3FileIOProperties,
      MetricsContext metrics) {
    return new S3InputFile(
        client,
        new S3URI(location, s3FileIOProperties.bucketToAccessPointMapping()),
        length > 0 ? length : null,
        s3FileIOProperties,
        metrics);
  }

  S3InputFile(
      S3Client client,
      S3URI uri,
      Long length,
      S3FileIOProperties s3FileIOProperties,
      MetricsContext metrics) {
    super(client, uri, s3FileIOProperties, metrics);
    this.length = length;
  }

  /**
   * Note: this may be stale if file was deleted since metadata is cached for size/existence checks.
   *
   * @return content length
   */
  @Override
  public long getLength() {
    if (length == null) {
      this.length = getObjectMetadata().contentLength();
    }

    return length;
  }

  @Override
  public SeekableInputStream newStream() {
    return new S3InputStream(client(), uri(), s3FileIOProperties(), metrics());
  }

  @Override
  public NativeFileCryptoParameters nativeCryptoParameters() {
    return nativeDecryptionParameters;
  }

  @Override
  public void setNativeCryptoParameters(NativeFileCryptoParameters nativeCryptoParameters) {
    this.nativeDecryptionParameters = nativeCryptoParameters;
  }
}
