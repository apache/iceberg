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

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3URI;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.ObjectMetadata;
import java.io.IOException;
import java.io.Serializable;
import java.util.Objects;
import org.apache.http.HttpStatus;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.PositionOutputStream;
import org.apache.iceberg.io.SeekableInputStream;
import org.apache.iceberg.util.SerializableSupplier;

/**
 * FileIO implementation backed by S3.  Locations used must follow the conventions
 * for AmazonS3URIs (e.g. s3://bucket/path/..).
 */
public class S3FileIO implements FileIO, Serializable {
  private final SerializableSupplier<AmazonS3> s3;

  public S3FileIO(SerializableSupplier<AmazonS3> s3) {
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
    s3.get().deleteObject(location.getBucket(), location.getKey());
  }

  public class S3InputFile implements InputFile {
    private final AmazonS3URI location;
    private ObjectMetadata metadata;

    public S3InputFile(AmazonS3URI location) {
      this.location = location;
    }

    /**
     * Note: this may be stale if file was deleted since metadata is cached
     * for size/existence checks.
     *
     * @return content length
     */
    @Override
    public long getLength() {
      return Objects.requireNonNull(getObjectMetadata()).getContentLength();
    }

    @Override
    public SeekableInputStream newStream() {
      return new S3InputStream(s3.get(), location);
    }

    @Override
    public String location() {
      return location.toString();
    }

    /**
     * Note: this may be stale if file was deleted since metadata is cached
     * for size/existence checks.
     *
     * @return flag
     */
    @Override
    public boolean exists() {
      return getObjectMetadata() != null;
    }

    @Override
    public String toString() {
      return location.toString();
    }

    private ObjectMetadata getObjectMetadata() {
      if (metadata == null) {
        try {
          metadata = s3.get().getObjectMetadata(location.getBucket(), location.getKey());
        } catch (AmazonS3Exception e) {
          if (e.getStatusCode() != HttpStatus.SC_NOT_FOUND) {
            throw e; // return null if 404 Not Found, otherwise rethrow
          }
        }
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
        return new S3OutputStream(s3.get(), location);
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
