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
package org.apache.iceberg.gcp.gcs;

import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.StorageException;
import java.io.IOException;
import org.apache.iceberg.exceptions.NotFoundException;

final class GCSExceptionUtil {
  private GCSExceptionUtil() {}

  static void throwNotFoundIfNotPresent(IOException ioException, BlobId blobId) {
    if (ioException.getCause() instanceof StorageException storageException
        && storageException.getCode() == 404) {
      throw new NotFoundException(ioException, "Location does not exist: %s", blobId.toGsUtilUri());
    }
  }
}
