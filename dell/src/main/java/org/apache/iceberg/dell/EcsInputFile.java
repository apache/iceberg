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

package org.apache.iceberg.dell;

import com.emc.object.s3.S3Client;
import com.emc.object.s3.S3Exception;
import com.emc.object.s3.S3ObjectMetadata;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.SeekableInputStream;

class EcsInputFile extends BaseEcsFile implements InputFile {

  EcsInputFile(S3Client client, String location) {
    super(client, location);
  }

  /**
   * Eager-get object length
   *
   * @return Length if object exists
   */
  @Override
  public long getLength() {
    try {
      S3ObjectMetadata metadata = client().getObjectMetadata(uri().bucket(), uri().name());
      return metadata.getContentLength();
    } catch (S3Exception e) {
      if (e.getHttpCode() == 404) {
        return 0;
      } else {
        throw e;
      }
    }
  }

  @Override
  public SeekableInputStream newStream() {
    return new EcsSeekableInputStream(client(), uri());
  }
}
