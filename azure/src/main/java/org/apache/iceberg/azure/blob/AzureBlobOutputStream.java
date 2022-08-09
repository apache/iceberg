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
import com.azure.storage.blob.models.ParallelTransferOptions;
import com.azure.storage.blob.options.BlockBlobOutputStreamOptions;
import com.azure.storage.blob.specialized.BlobOutputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;
import org.apache.iceberg.azure.AzureProperties;
import org.apache.iceberg.io.PositionOutputStream;
import org.apache.iceberg.relocated.com.google.common.base.Joiner;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AzureBlobOutputStream extends PositionOutputStream {

  private static final Logger LOG = LoggerFactory.getLogger(AzureBlobOutputStream.class);
  private static final Joiner TRACE_JOINER = Joiner.on("\n\t");

  private final StackTraceElement[] createStack;
  private final AzureURI azureURI;
  private final AzureProperties azureProperties;
  private final BlobClient blobClient;

  private OutputStream stream;
  private long pos = 0;
  private boolean closed = false;

  public AzureBlobOutputStream(AzureURI azureURI, AzureProperties azureProperties, BlobClient blobClient) {
    this.createStack = Thread.currentThread().getStackTrace();
    this.azureURI = azureURI;
    this.azureProperties = azureProperties;
    this.blobClient = blobClient;
    openStream();
  }

  @Override
  public long getPos() {
    return pos;
  }

  @Override
  public void write(int b) throws IOException {
    Preconditions.checkState(!closed, "Cannot write: stream already closed");
    stream.write(b);
    pos++;
  }

  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    Preconditions.checkState(!closed, "Cannot write: stream already closed");
    stream.write(b, off, len);
    pos += len;
  }

  @Override
  public void flush() throws IOException {
    Preconditions.checkState(!closed, "Cannot flush: stream already closed");
    super.flush();
    stream.flush();
  }

  @Override
  public void close() throws IOException {
    if (closed) {
      return;
    }

    this.flush();

    super.close();
    stream.close();
    closed = true;
  }

  @SuppressWarnings("checkstyle:NoFinalizer")
  @Override
  protected void finalize() throws Throwable {
    super.finalize();
    if (!closed) {
      close(); // releasing resources is more important than printing the warning
      String trace = TRACE_JOINER.join(Arrays.copyOfRange(createStack, 1, createStack.length));
      LOG.warn("Unclosed output stream created by:\n\t{}", trace);
    }
  }

  private void openStream() {
    ParallelTransferOptions options = new ParallelTransferOptions()
        .setBlockSizeLong(azureProperties.writeBlockSize(azureURI.storageAccount()))
        .setMaxConcurrency(azureProperties.maxWriteConcurrency(azureURI.storageAccount()))
        .setMaxSingleUploadSizeLong(azureProperties.maxSingleUploadSize(azureURI.storageAccount()));
    BlobOutputStream blobOutputStream = blobClient.getBlockBlobClient()
        .getBlobOutputStream(new BlockBlobOutputStreamOptions().setParallelTransferOptions(options));
    this.stream = new BufferedOutputStream(blobOutputStream);
  }
}
