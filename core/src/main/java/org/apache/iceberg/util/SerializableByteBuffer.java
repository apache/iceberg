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
package org.apache.iceberg.util;

import java.io.EOFException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;

public class SerializableByteBuffer implements Serializable {

  private transient ByteBuffer wrapped;

  private SerializableByteBuffer(ByteBuffer wrapped) {
    this.wrapped = wrapped;
  }

  public static SerializableByteBuffer wrap(ByteBuffer buffer) {
    return new SerializableByteBuffer(buffer);
  }

  public ByteBuffer buffer() {
    return wrapped;
  }

  private void readObject(ObjectInputStream in) throws IOException {
    int length = in.readInt();
    wrapped = ByteBuffer.allocate(length);
    int amountRead = 0;
    ReadableByteChannel channel = Channels.newChannel(in);
    while (amountRead < length) {
      int ret = channel.read(wrapped);
      if (ret == -1) {
        throw new EOFException("End of file before fully reading buffer");
      }
      amountRead += ret;
    }
    wrapped.rewind(); // Allow us to read it later
  }

  private void writeObject(ObjectOutputStream out) throws IOException {
    out.writeInt(wrapped.limit());
    if (Channels.newChannel(out).write(wrapped) != wrapped.limit()) {
      throw new IOException("Could not fully write buffer to output stream");
    }
    wrapped.rewind(); // Allow us to write it again later
  }
}
