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
import org.apache.iceberg.aws.AwsClientUtil;
import org.apache.iceberg.aws.AwsProperties;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.util.SerializableSupplier;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.Delete;
import software.amazon.awssdk.services.s3.model.DeleteObjectsRequest;
import software.amazon.awssdk.services.s3.model.ObjectIdentifier;

/**
 * FileIO implementation backed by S3.
 * Locations used must follow the conventions for S3 URIs (e.g. s3://bucket/path...).
 * See {@link S3URI#VALID_SCHEMES} for the list of supported S3 URI schemes.
 */
public class S3FileIO implements FileIO {
  private final SerializableSupplier<S3Client> s3;
  private AwsProperties awsProperties;
  private transient S3Client client;

  public S3FileIO() {
    this(AwsClientUtil::defaultS3Client);
  }

  public S3FileIO(SerializableSupplier<S3Client> s3) {
    this(s3, new AwsProperties());
  }

  public S3FileIO(SerializableSupplier<S3Client> s3, AwsProperties awsProperties) {
    this.s3 = s3;
    this.awsProperties = awsProperties;
  }

  @Override
  public InputFile newInputFile(String path) {
    return new S3InputFile(client(), new S3URI(path), awsProperties);
  }

  @Override
  public OutputFile newOutputFile(String path) {
    return new S3OutputFile(client(), new S3URI(path), awsProperties);
  }

  @Override
  public void deleteFile(String path) {
    S3URI location = new S3URI(path);
    ObjectIdentifier objectIdentifier = ObjectIdentifier.builder().key(location.key()).build();
    Delete delete = Delete.builder().objects(objectIdentifier).build();
    DeleteObjectsRequest deleteRequest =
        DeleteObjectsRequest.builder().bucket(location.bucket()).delete(delete).build();

    client().deleteObjects(deleteRequest);
  }

  private S3Client client() {
    if (client == null) {
      client = s3.get();
    }
    return client;
  }

  @Override
  public void initialize(Map<String, String> properties) {
    this.awsProperties = new AwsProperties(properties);
  }
}
