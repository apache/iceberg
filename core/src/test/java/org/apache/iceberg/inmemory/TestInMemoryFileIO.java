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

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.UUID;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

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

  @Test
  public void properties() {
    InMemoryFileIO io = new InMemoryFileIO();
    Map<String, String> properties = ImmutableMap.of("key1", "value1", "key2", "value2");
    io.initialize(properties);
    assertThat(io.properties()).isEqualTo(properties);
  }

  @Test
  public void tryRenameMovesInMemoryEntry() {
    String from = randomLocation();
    String to = randomLocation();
    InMemoryFileIO fileIO = new InMemoryFileIO();
    fileIO.addFile(from, "payload".getBytes());

    assertThat(InMemoryFileIO.tryRename(from, to)).isTrue();
    assertThat(fileIO.fileExists(from)).isFalse();
    assertThat(fileIO.fileExists(to)).isTrue();
  }

  @Test
  public void tryRenameReturnsFalseWhenSourceMissing() {
    assertThat(InMemoryFileIO.tryRename("s3://nope/" + UUID.randomUUID(), "s3://nope/other"))
        .isFalse();
  }

  @Test
  public void diskFallbackReadsLocalFileWhenNotInMemory(@TempDir Path tempDir) throws IOException {
    File diskFile = tempDir.resolve("payload.bin").toFile();
    Files.write(diskFile.toPath(), "from-disk".getBytes());

    InMemoryFileIO fileIO = new InMemoryFileIO();
    fileIO.initialize(ImmutableMap.of(InMemoryFileIO.DISK_FALLBACK, "true"));

    InputFile input = fileIO.newInputFile("file:" + diskFile.getAbsolutePath());
    try (InputStream stream = input.newStream()) {
      byte[] buf = new byte[(int) input.getLength()];
      assertThat(stream.read(buf)).isEqualTo(buf.length);
      assertThat(new String(buf)).isEqualTo("from-disk");
    }
  }

  @Test
  public void diskFallbackDoesNotShadowInMemoryReads(@TempDir Path tempDir) throws IOException {
    File diskFile = tempDir.resolve("shared.bin").toFile();
    Files.write(diskFile.toPath(), "from-disk".getBytes());
    String location = "file:" + diskFile.getAbsolutePath();

    InMemoryFileIO fileIO = new InMemoryFileIO();
    fileIO.initialize(ImmutableMap.of(InMemoryFileIO.DISK_FALLBACK, "true"));
    fileIO.addFile(location, "from-memory".getBytes());

    InputFile input = fileIO.newInputFile(location);
    try (InputStream stream = input.newStream()) {
      byte[] buf = new byte[(int) input.getLength()];
      assertThat(stream.read(buf)).isEqualTo(buf.length);
      assertThat(new String(buf)).isEqualTo("from-memory");
    }
  }

  @Test
  public void diskFallbackPreservesRequestedLocation(@TempDir Path tempDir) throws IOException {
    File diskFile = tempDir.resolve("preserve.bin").toFile();
    Files.write(diskFile.toPath(), "bytes".getBytes());
    String requestedLocation = "file:" + diskFile.getAbsolutePath();

    InMemoryFileIO fileIO = new InMemoryFileIO();
    fileIO.initialize(ImmutableMap.of(InMemoryFileIO.DISK_FALLBACK, "true"));

    // Iceberg components key InputFile maps off the requested location string. The disk-backed
    // delegate would otherwise drop the file: scheme and break those lookups.
    InputFile input = fileIO.newInputFile(requestedLocation);
    assertThat(input.location()).isEqualTo(requestedLocation);
    assertThat(input.toString()).isEqualTo(requestedLocation);
  }

  @Test
  public void diskFallbackOffStillThrowsForUnknownLocation() {
    InMemoryFileIO fileIO = new InMemoryFileIO();
    assertThatExceptionOfType(NotFoundException.class)
        .isThrownBy(() -> fileIO.newInputFile("file:/var/empty/" + UUID.randomUUID()))
        .withMessageContaining("No in-memory file found");
  }

  @Test
  public void diskFallbackDeleteRemovesLocalFile(@TempDir Path tempDir) throws IOException {
    File diskFile = tempDir.resolve("delete-me.bin").toFile();
    Files.write(diskFile.toPath(), "bytes".getBytes());

    InMemoryFileIO fileIO = new InMemoryFileIO();
    fileIO.initialize(ImmutableMap.of(InMemoryFileIO.DISK_FALLBACK, "true"));
    fileIO.deleteFile("file:" + diskFile.getAbsolutePath());

    assertThat(diskFile).doesNotExist();
  }

  private String randomLocation() {
    return "s3://foo/" + UUID.randomUUID();
  }
}
