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
package org.apache.iceberg.flink.sink;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import org.apache.flink.annotation.Internal;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.iceberg.ContentFileAvroEncoder;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.relocated.com.google.common.io.ByteStreams;

@Internal
public class WriteResultSerializer implements SimpleVersionedSerializer<WriteResult> {
  private static final int VERSION = 1;

  @Override
  public int getVersion() {
    return VERSION;
  }

  @Override
  public byte[] serialize(WriteResult writeResult) throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    DataOutputStream view = new DataOutputStream(out);

    writeByteArray(ContentFileAvroEncoder.encode(writeResult.dataFiles()), view);
    writeByteArray(ContentFileAvroEncoder.encode(writeResult.deleteFiles()), view);

    view.writeInt(writeResult.referencedDataFiles().length);
    for (CharSequence referencedDataFile : writeResult.referencedDataFiles()) {
      view.writeUTF(referencedDataFile.toString());
    }

    writeByteArray(ContentFileAvroEncoder.encode(writeResult.rewrittenDeleteFiles()), view);

    return out.toByteArray();
  }

  @Override
  public WriteResult deserialize(int version, byte[] serialized) throws IOException {
    if (version != 1) {
      throw new IOException("Unrecognized version or corrupt state: " + version);
    }
    DataInputStream view = new DataInputStream(new ByteArrayInputStream(serialized));

    DataFile[] dataFiles = ContentFileAvroEncoder.decodeDataFiles(readByteArray(view));
    DeleteFile[] deleteFiles = ContentFileAvroEncoder.decodeDeleteFiles(readByteArray(view));

    CharSequence[] referencedDataFiles = new CharSequence[view.readInt()];
    for (int i = 0; i < referencedDataFiles.length; i++) {
      referencedDataFiles[i] = view.readUTF();
    }

    DeleteFile[] rewrittenDeleteFiles =
        ContentFileAvroEncoder.decodeDeleteFiles(readByteArray(view));

    return WriteResult.builder()
        .addDataFiles(dataFiles)
        .addDeleteFiles(deleteFiles)
        .addReferencedDataFiles(referencedDataFiles)
        .addRewrittenDeleteFiles(rewrittenDeleteFiles)
        .build();
  }

  private static void writeByteArray(byte[] buffer, DataOutputStream view) throws IOException {
    view.writeInt(buffer.length);
    view.write(buffer);
  }

  private static byte[] readByteArray(DataInputStream inputStream) throws IOException {
    byte[] buffer = new byte[inputStream.readInt()];
    ByteStreams.readFully(inputStream, buffer);
    return buffer;
  }
}
