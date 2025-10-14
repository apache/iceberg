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

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Arrays;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.http.AbortableInputStream;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;

@ExtendWith(MockitoExtension.class)
public final class TestS3InputStream {

  @Mock private S3Client s3Client;
  private AbortableInputStream inputStream;

  private S3InputStream s3InputStream;

  private static final int CONTENT_LENGTH = 1024;

  @BeforeEach
  void before() {
    byte[] writeValue = new byte[CONTENT_LENGTH];
    Arrays.fill(writeValue, (byte) 1);

    inputStream =
        spy(AbortableInputStream.create(new ByteArrayInputStream(new byte[CONTENT_LENGTH])));
    when(s3Client.getObject(any(GetObjectRequest.class), any(ResponseTransformer.class)))
        .thenReturn(inputStream);
    s3InputStream = new S3InputStream(s3Client, mock(), CONTENT_LENGTH);
  }

  @Test
  void testReadFullyClosesTheStream() throws IOException {
    s3InputStream.readFully(0, new byte[0]);

    verify(inputStream).close();
  }

  @Test
  void testReadTailClosesTheStream() throws IOException {
    s3InputStream.readTail(new byte[0], 0, 0);

    verify(inputStream).close();
  }

  @Test
  void testAbortIsCalledAfterPartialRead() throws IOException {
    byte[] buff = new byte[500];
    s3InputStream.read(buff);

    // close after reading partial object, should call abort
    s3InputStream.close();
    verify(inputStream).abort();
  }

  @Test
  void testAbortIsCalledAfterFullRead() throws IOException {
    byte[] buff = new byte[CONTENT_LENGTH];
    s3InputStream.read(buff);

    // If we're at EoF, this should not call abort.
    s3InputStream.close();
    verify(inputStream, never()).abort();
  }
}
