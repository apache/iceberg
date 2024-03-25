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
package org.apache.hadoop.fs.s3a;

import java.io.IOException;
import java.io.OutputStream;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/** mock class for testing hadoop s3a writer */
public class S3ABlockOutputStream extends OutputStream {
  public ExecutorService mockCloseService;
  public Future<?> mockUploadOnClose;

  public S3ABlockOutputStream() {
    mockCloseService = Executors.newSingleThreadExecutor();
  }

  @Override
  public void write(int b) throws IOException {
    throw new IOException("mocked class, do not use");
  }

  @Override
  public void close() throws IOException {
    try {
      mockUploadOnClose =
          mockCloseService.submit(
              () -> {
                try {
                  Thread.sleep(30 * 1000);
                } catch (InterruptedException e) {
                  // ignore
                }
              });
      mockUploadOnClose.get();
    } catch (CancellationException | InterruptedException e) {
      // mock interrupt in S3ABlockOutputStream#putObject
      Thread.currentThread().interrupt();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    super.close();
  }

  public void interruptClose() {
    mockUploadOnClose.cancel(true);
  }
}
