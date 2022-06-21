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

package org.apache.iceberg.encryption;


import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Random;
import org.apache.iceberg.Files;
import org.apache.iceberg.io.PositionOutputStream;
import org.apache.iceberg.io.SeekableInputStream;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class TestGcmStreams {

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

  @Test
  public void testRandomWriteRead() throws IOException {
    Random random = new Random();
    int testFileSize = 1000000;
    byte[] testFileContents = new byte[testFileSize];
    random.nextBytes(testFileContents);
    int[] aesKeyLengthArray = {16, 24, 32};
    for (int keyLength : aesKeyLengthArray) {
      byte[] key = new byte[keyLength];
      random.nextBytes(key);
      File testFile = temp.newFile();

      AesGcmOutputFile encryptedFile = new AesGcmOutputFile(Files.localOutput(testFile), key);
      PositionOutputStream encryptedStream = encryptedFile.createOrOverwrite();

      int maxChunkLen = testFileSize / 5;
      int offset = 0;
      int left = testFileSize;

      while (left > 0) {
        int chunkLen = random.nextInt(maxChunkLen);
        if (chunkLen > left) {
          chunkLen = left;
        }
        encryptedStream.write(testFileContents, offset, chunkLen);
        offset += chunkLen;
        Assert.assertEquals("Position", offset, encryptedStream.getPos());
        left -= chunkLen;

        if (left == 0) {
          break;
        }
      }
      encryptedStream.close();

      AesGcmInputFile decryptedFile = new AesGcmInputFile(Files.localInput(testFile), key);
      SeekableInputStream decryptedStream = decryptedFile.newStream();

      Assert.assertEquals("File size", testFileSize, decryptedFile.getLength());

      byte[] chunk = new byte[testFileSize];

      for (int n = 0; n < 1000; n++) {
        int chunkLen = random.nextInt(testFileSize);
        int pos = random.nextInt(testFileSize);
        left = testFileSize - pos;
        if (left < chunkLen) {
          chunkLen = left;
        }

        decryptedStream.seek(pos);
        int len = decryptedStream.read(chunk, 0, chunkLen);

        Assert.assertEquals("Read length", len, chunkLen);

        ByteBuffer bb1 = ByteBuffer.wrap(chunk, 0, chunkLen);
        ByteBuffer bb2 = ByteBuffer.wrap(testFileContents, pos, chunkLen);

        Assert.assertEquals("Read contents", bb1, bb2);
      }

      decryptedStream.close();
    }
  }
}
