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
package org.apache.iceberg.inmemory;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.UUID;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.NotFoundException;
import org.junit.jupiter.api.Test;

public class TestInMemoryFileIO {

  @Test
  public void testBasicEndToEnd() throws IOException {
    InMemoryFileIO fileIO = new InMemoryFileIO();
    String location = randomLocation();
    assertThat(fileIO.fileExists(location)).isFalse();

    OutputStream outputStream = fileIO.newOutputFile(location).create();
    byte[] data = "hello world".getBytes();
    outputStream.write(data);
    outputStream.close();
    assertThat(fileIO.fileExists(location)).isTrue();

    InputStream inputStream = fileIO.newInputFile(location).newStream();
    byte[] buf = new byte[data.length];
    inputStream.read(buf);
    inputStream.close();
    assertThat(new String(buf)).isEqualTo("hello world");

    fileIO.deleteFile(location);
    assertThat(fileIO.fileExists(location)).isFalse();
  }

  @Test
  public void testNewInputFileNotFound() {
    InMemoryFileIO fileIO = new InMemoryFileIO();
    assertThatExceptionOfType(NotFoundException.class)
        .isThrownBy(() -> fileIO.newInputFile("s3://nonexistent/file"))
        .withMessageContaining("No in-memory file found");
  }

  @Test
  public void testDeleteFileNotFound() {
    InMemoryFileIO fileIO = new InMemoryFileIO();
    assertThatExceptionOfType(NotFoundException.class)
        .isThrownBy(() -> fileIO.deleteFile("s3://nonexistent/file"))
        .withMessageContaining("No in-memory file found");
  }

  @Test
  public void testCreateNoOverwrite() {
    String location = randomLocation();
    InMemoryFileIO fileIO = new InMemoryFileIO();
    fileIO.addFile(location, "hello world".getBytes());
    assertThatExceptionOfType(AlreadyExistsException.class)
        .isThrownBy(() -> fileIO.newOutputFile(location).create())
        .withMessage("Already exists");
  }

  @Test
  public void testOverwriteBeforeAndAfterClose() throws IOException {
    String location = randomLocation();
    byte[] oldData = "old data".getBytes();
    byte[] newData = "new data".getBytes();

    InMemoryFileIO fileIO = new InMemoryFileIO();
    OutputStream outputStream = fileIO.newOutputFile(location).create();
    outputStream.write(oldData);

    // Even though we've called create() and started writing data, this file won't yet exist
    // in the parentFileIO before we've closed it.
    assertThat(fileIO.fileExists(location)).isFalse();

    // File appears after closing it.
    outputStream.close();
    assertThat(fileIO.fileExists(location)).isTrue();

    // Start a new OutputFile and write new data but don't close() it yet.
    outputStream = fileIO.newOutputFile(location).createOrOverwrite();
    outputStream.write(newData);

    // We'll still read old data.
    InputStream inputStream = fileIO.newInputFile(location).newStream();
    byte[] buf = new byte[oldData.length];
    inputStream.read(buf);
    inputStream.close();
    assertThat(new String(buf)).isEqualTo("old data");

    // Finally, close the new output stream; data should be overwritten with new data now.
    outputStream.close();
    inputStream = fileIO.newInputFile(location).newStream();
    buf = new byte[newData.length];
    inputStream.read(buf);
    inputStream.close();
    assertThat(new String(buf)).isEqualTo("new data");
  }

  @Test
  public void testFilesAreSharedAcrossMultipleInstances() {
    String location = randomLocation();
    InMemoryFileIO fileIO = new InMemoryFileIO();
    fileIO.addFile(location, "hello world".getBytes());

    InMemoryFileIO fileIO2 = new InMemoryFileIO();
    assertThat(fileIO2.fileExists(location))
        .isTrue()
        .as("Files should be shared across all InMemoryFileIO instances");
  }

  private String randomLocation() {
    return "s3://foo/" + UUID.randomUUID();
  }
}
