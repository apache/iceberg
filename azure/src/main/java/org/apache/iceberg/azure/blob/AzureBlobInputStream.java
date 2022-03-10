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

package org.apache.iceberg.azure.blob;

import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.specialized.BlobInputStream;
import java.io.IOException;
import org.apache.iceberg.io.SeekableInputStream;

public class AzureBlobInputStream extends SeekableInputStream {

  private final BlobClient blobClient;
  private final BlobInputStream inputStream;

  private long pos;

  public AzureBlobInputStream(BlobClient blobClient) {
    this.blobClient = blobClient;
    this.inputStream = blobClient.openInputStream();
    this.pos = 0L;
  }

  @Override
  public long getPos() throws IOException {
    return pos;
  }

  @Override
  public void seek(long newPos) throws IOException {
    if (newPos == pos) {
      return;
    } else if (newPos > pos) {
      final long bytesToSkip = newPos - pos;
      inputStream.skip(bytesToSkip);
    }
  }

  @Override
  public int read() throws IOException {
    return 0;
  }
}
