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

import com.amazonaws.services.s3.AmazonS3URI;
import java.io.IOException;
import java.io.Serializable;
import org.apache.http.HttpStatus;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.PositionOutputStream;
import org.apache.iceberg.io.SeekableInputStream;
import org.apache.iceberg.util.SerializableSupplier;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.Delete;
import software.amazon.awssdk.services.s3.model.DeleteObjectsRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.ObjectIdentifier;
import software.amazon.awssdk.services.s3.model.S3Exception;

/**
 * FileIO implementation backed by S3.  Locations used must follow the conventions for URIs (e.g. s3://bucket/path/..).
 */
public class S3FileIO implements FileIO, Serializable {
  private final SerializableSupplier<S3Client> s3;
  private transient S3Client client;

  public S3FileIO(SerializableSupplier<S3Client> s3) {
    this.s3 = s3;
  }

  @Override
  public InputFile newInputFile(String path) {
    return new S3InputFile(new AmazonS3URI(path));
  }

  @Override
  public OutputFile newOutputFile(String path) {
    return new S3OutputFile(new AmazonS3URI(path));
  }

  @Override
  public void deleteFile(String path) {
    AmazonS3URI location = new AmazonS3URI(path);
    ObjectIdentifier objectIdentifier = ObjectIdentifier.builder().key(location.getKey()).build();
    Delete delete = Delete.builder().objects(objectIdentifier).build();
    DeleteObjectsRequest deleteRequest =
        DeleteObjectsRequest.builder().bucket(location.getBucket()).delete(delete).build();

    client().deleteObjects(deleteRequest);
  }

  private S3Client client() {
    if (client == null) {
      client = s3.get();
    }
    return client;
  }

  public class S3InputFile implements InputFile {
    private final AmazonS3URI location;
    private HeadObjectResponse metadata;

    public S3InputFile(AmazonS3URI location) {
      this.location = location;
    }

    /**
     * Note: this may be stale if file was deleted since metadata is cached for size/existence checks.
     *
     * @return content length
     */
    @Override
    public long getLength() {
      return getObjectMetadata().contentLength();
    }

    @Override
    public SeekableInputStream newStream() {
      return new S3InputStream(client(), location);
    }

    @Override
    public String location() {
      return location.toString();
    }

    /**
     * Note: this may be stale if file was deleted since metadata is cached for size/existence checks.
     *
     * @return flag
     */
    @Override
    public boolean exists() {
      try {
        return getObjectMetadata() != null;
      } catch (S3Exception e) {
        if (e.statusCode() == HttpStatus.SC_NOT_FOUND) {
          return false;
        } else {
          throw e; // return null if 404 Not Found, otherwise rethrow
        }
      }
    }

    @Override
    public String toString() {
      return location.toString();
    }

    private HeadObjectResponse getObjectMetadata() throws S3Exception {
      if (metadata == null) {
        metadata = client().headObject(HeadObjectRequest.builder()
            .bucket(location.getBucket())
            .key(location.getKey())
            .build());
      }

      return metadata;
    }
  }

  public class S3OutputFile implements OutputFile {
    private final AmazonS3URI location;

    public S3OutputFile(AmazonS3URI location) {
      this.location = location;
    }

    @Override
    public PositionOutputStream create() {
      throw new UnsupportedOperationException();
    }

    @Override
    public PositionOutputStream createOrOverwrite() {
      try {
        return new S3OutputStream(client(), location);
      } catch (IOException e) {
        throw new RuntimeException("Failed to create temporary file for staging.", e);
      }
    }

    @Override
    public String location() {
      return location.toString();
    }

    @Override
    public InputFile toInputFile() {
      return new S3InputFile(location);
    }
  }
}
