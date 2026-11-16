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
package org.apache.iceberg.io.canonicaluri;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.encryption.NativeEncryptionInputFile;
import org.apache.iceberg.encryption.NativeEncryptionKeyMetadata;
import org.apache.iceberg.encryption.NativeEncryptionOutputFile;
import org.apache.iceberg.io.DelegateFileIO;
import org.apache.iceberg.io.DelegatingInputStream;
import org.apache.iceberg.io.DelegatingOutputStream;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.FileInfo;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.PositionOutputStream;
import org.apache.iceberg.io.RangeReadable;
import org.apache.iceberg.io.SeekableInputStream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class TestCanonicalUriFileIO {
  private static final String CANONICAL_PREFIX = "s3://warehouse/";
  private static final String PHYSICAL_PREFIX = "test://backend/warehouse/";

  @BeforeEach
  void resetDelegate() {
    RecordingFileIO.reset();
  }

  @Test
  void loadsThroughIoImplAndPreservesWrapperIdentity() throws IOException {
    FileIO loaded = CatalogUtil.loadFileIO(CanonicalUriFileIO.class.getName(), properties(), null);
    assertThat(loaded).isInstanceOf(CanonicalUriFileIO.class);

    CanonicalUriFileIO io = (CanonicalUriFileIO) loaded;
    String canonical = CANONICAL_PREFIX + "table/data/a.parquet";
    String physical = PHYSICAL_PREFIX + "table/data/a.parquet";

    OutputFile output = io.newOutputFile(canonical);
    assertThat(output.location()).isEqualTo(canonical);
    try (PositionOutputStream stream = output.create()) {
      assertThat(stream).isNotInstanceOf(DelegatingOutputStream.class);
      stream.write("canonical-bytes".getBytes(UTF_8));
    }

    assertThat(RecordingFileIO.lastOutputLocation).isEqualTo(physical);
    assertThat(output.toInputFile().location()).isEqualTo(canonical);

    InputFile input = io.newInputFile(canonical, 15L);
    assertThat(input.location()).isEqualTo(canonical);
    assertThat(input.getLength()).isEqualTo(15L);
    assertThat(RecordingFileIO.lastInputLocation).isEqualTo(physical);
    assertThat(RecordingFileIO.lastKnownLength.get()).isEqualTo(15L);
    try (SeekableInputStream stream = input.newStream()) {
      assertThat(stream).isInstanceOf(RangeReadable.class);
      assertThat(stream).isNotInstanceOf(DelegatingInputStream.class);
      assertThat(readAll(stream)).isEqualTo("canonical-bytes");
    }

    assertThat(io.metrics().readCount()).isEqualTo(1L);
    assertThat(io.metrics().writeCount()).isEqualTo(1L);
    assertThat(io.metrics().translationCount()).isGreaterThanOrEqualTo(2L);
  }

  @Test
  void reverseMapsPrefixListingAndKeepsBulkOperations() throws IOException {
    CanonicalUriFileIO io = newFileIO();
    write(io, CANONICAL_PREFIX + "table/a", "a");
    write(io, CANONICAL_PREFIX + "table/b", "b");

    List<FileInfo> listed = new ArrayList<>();
    io.listPrefix(CANONICAL_PREFIX + "table/").forEach(listed::add);
    assertThat(listed)
        .extracting(FileInfo::location)
        .containsExactlyInAnyOrder(CANONICAL_PREFIX + "table/a", CANONICAL_PREFIX + "table/b");

    io.deleteFiles(Arrays.asList(CANONICAL_PREFIX + "table/a", CANONICAL_PREFIX + "table/b"));
    assertThat(RecordingFileIO.bulkDeleteCalls.get()).isEqualTo(1);
    assertThat(RecordingFileIO.data).isEmpty();

    write(io, CANONICAL_PREFIX + "table/c", "c");
    write(io, CANONICAL_PREFIX + "table/d", "d");
    io.deletePrefix(CANONICAL_PREFIX + "table/");
    assertThat(RecordingFileIO.deletePrefixCalls.get()).isEqualTo(1);
    assertThat(RecordingFileIO.data).isEmpty();
  }

  @Test
  void rejectsPhysicalWriteAndDeleteAliases() {
    CanonicalUriFileIO io = newFileIO();

    assertThatThrownBy(() -> io.newOutputFile(PHYSICAL_PREFIX + "table/a"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("physical alias");
    assertThatThrownBy(() -> io.deleteFile(PHYSICAL_PREFIX + "table/a"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("physical alias");
  }

  @Test
  void survivesJavaSerializationWithoutSerializingDelegate() throws Exception {
    CanonicalUriFileIO io = newFileIO();
    write(io, CANONICAL_PREFIX + "table/a", "serialized");
    int delegatesBefore = RecordingFileIO.instances.get();

    CanonicalUriFileIO restored = roundTrip(io);
    assertThat(read(restored, CANONICAL_PREFIX + "table/a")).isEqualTo("serialized");
    assertThat(RecordingFileIO.instances.get()).isEqualTo(delegatesBefore + 1);
    assertThat(restored.properties()).containsEntry(PathAliasProperties.ENABLED, "true");
  }

  @Test
  void killSwitchPassesLocationsThroughUnchanged() {
    Map<String, String> properties = properties();
    properties.put(PathAliasProperties.ENABLED, "false");
    CanonicalUriFileIO io = new CanonicalUriFileIO();
    io.initialize(properties);

    io.newInputFile("test://unchanged/a");
    assertThat(RecordingFileIO.lastInputLocation).isEqualTo("test://unchanged/a");
  }

  @Test
  void failsClosedWhenDelegateDoesNotSupportRequiredOperations() {
    Map<String, String> properties = properties();
    properties.put(PathAliasProperties.DELEGATE_IMPL, BasicFileIO.class.getName());
    CanonicalUriFileIO io = new CanonicalUriFileIO();
    io.initialize(properties);

    assertThatThrownBy(() -> io.newInputFile(CANONICAL_PREFIX + "a"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("does not implement DelegateFileIO");
  }

  @Test
  void rejectsReinitializationToKeepMappingsImmutable() {
    CanonicalUriFileIO io = newFileIO();

    assertThatThrownBy(() -> io.initialize(properties()))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("already initialized");
  }

  @Test
  void preservesNativeEncryptionCapabilitiesAndCanonicalIdentity() {
    String canonical = CANONICAL_PREFIX + "table/encrypted.parquet";
    String physical = PHYSICAL_PREFIX + "table/encrypted.parquet";
    PathAliasMetrics metrics = new PathAliasMetrics(true);
    NativeEncryptionKeyMetadata keyMetadata = new TestNativeKeyMetadata();

    InputFile input =
        CanonicalInputFile.wrap(
            canonical, new NativeMemoryInputFile(physical, keyMetadata), metrics);
    assertThat(input).isInstanceOf(NativeEncryptionInputFile.class);
    NativeEncryptionInputFile nativeInput = (NativeEncryptionInputFile) input;
    assertThat(nativeInput.location()).isEqualTo(canonical);
    assertThat(nativeInput.encryptedInputFile().location()).isEqualTo(canonical);
    assertThat(nativeInput.keyMetadata()).isSameAs(keyMetadata);

    OutputFile output =
        CanonicalOutputFile.wrap(
            canonical, new NativeMemoryOutputFile(physical, keyMetadata), metrics);
    assertThat(output).isInstanceOf(NativeEncryptionOutputFile.class);
    NativeEncryptionOutputFile nativeOutput = (NativeEncryptionOutputFile) output;
    assertThat(nativeOutput.location()).isEqualTo(canonical);
    assertThat(nativeOutput.encryptingOutputFile().location()).isEqualTo(canonical);
    assertThat(nativeOutput.plainOutputFile().location()).isEqualTo(canonical);
    assertThat(nativeOutput.keyMetadata()).isSameAs(keyMetadata);
  }

  private static CanonicalUriFileIO newFileIO() {
    CanonicalUriFileIO io = new CanonicalUriFileIO();
    io.initialize(properties());
    return io;
  }

  private static Map<String, String> properties() {
    Map<String, String> properties = new ConcurrentHashMap<>();
    properties.put(PathAliasProperties.ENABLED, "true");
    properties.put(PathAliasProperties.DELEGATE_IMPL, RecordingFileIO.class.getName());
    properties.put(PathAliasProperties.UNMATCHED, "passthrough");
    properties.put("pathalias.rule.0.id", "warehouse");
    properties.put("pathalias.rule.0.canonical", CANONICAL_PREFIX);
    properties.put("pathalias.rule.0.physical", PHYSICAL_PREFIX);
    return properties;
  }

  private static void write(CanonicalUriFileIO io, String location, String value)
      throws IOException {
    try (PositionOutputStream stream = io.newOutputFile(location).createOrOverwrite()) {
      stream.write(value.getBytes(UTF_8));
    }
  }

  private static String read(CanonicalUriFileIO io, String location) throws IOException {
    try (SeekableInputStream stream = io.newInputFile(location).newStream()) {
      return readAll(stream);
    }
  }

  private static String readAll(SeekableInputStream stream) throws IOException {
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    byte[] buffer = new byte[32];
    int read;
    while ((read = stream.read(buffer)) >= 0) {
      bytes.write(buffer, 0, read);
    }

    return new String(bytes.toByteArray(), UTF_8);
  }

  private static CanonicalUriFileIO roundTrip(CanonicalUriFileIO io) throws Exception {
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    try (ObjectOutputStream output = new ObjectOutputStream(bytes)) {
      output.writeObject(io);
    }

    try (ObjectInputStream input =
        new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
      return (CanonicalUriFileIO) input.readObject();
    }
  }

  public static class RecordingFileIO implements DelegateFileIO {
    private static final Map<String, byte[]> data = new ConcurrentHashMap<>();
    private static final AtomicInteger instances = new AtomicInteger();
    private static final AtomicInteger bulkDeleteCalls = new AtomicInteger();
    private static final AtomicInteger deletePrefixCalls = new AtomicInteger();
    private static final AtomicLong lastKnownLength = new AtomicLong(-1L);
    private static volatile String lastInputLocation;
    private static volatile String lastOutputLocation;

    public RecordingFileIO() {
      instances.incrementAndGet();
    }

    static void reset() {
      data.clear();
      instances.set(0);
      bulkDeleteCalls.set(0);
      deletePrefixCalls.set(0);
      lastKnownLength.set(-1L);
      lastInputLocation = null;
      lastOutputLocation = null;
    }

    @Override
    public InputFile newInputFile(String path) {
      lastInputLocation = path;
      return new MemoryInputFile(path);
    }

    @Override
    public InputFile newInputFile(String path, long length) {
      lastKnownLength.set(length);
      return newInputFile(path);
    }

    @Override
    public OutputFile newOutputFile(String path) {
      lastOutputLocation = path;
      return new MemoryOutputFile(path);
    }

    @Override
    public void deleteFile(String path) {
      data.remove(path);
    }

    @Override
    public void deleteFiles(Iterable<String> pathsToDelete) {
      bulkDeleteCalls.incrementAndGet();
      pathsToDelete.forEach(data::remove);
    }

    @Override
    public Iterable<FileInfo> listPrefix(String prefix) {
      List<FileInfo> files = new ArrayList<>();
      data.forEach(
          (location, contents) -> {
            if (location.startsWith(prefix)) {
              files.add(new FileInfo(location, contents.length, 0L));
            }
          });
      return files;
    }

    @Override
    public void deletePrefix(String prefix) {
      deletePrefixCalls.incrementAndGet();
      data.keySet().removeIf(location -> location.startsWith(prefix));
    }
  }

  public static class BasicFileIO implements FileIO {
    @Override
    public InputFile newInputFile(String path) {
      return new MemoryInputFile(path);
    }

    @Override
    public OutputFile newOutputFile(String path) {
      return new MemoryOutputFile(path);
    }

    @Override
    public void deleteFile(String path) {}
  }

  private static class MemoryInputFile implements InputFile {
    private final String location;

    private MemoryInputFile(String location) {
      this.location = location;
    }

    @Override
    public long getLength() {
      return bytes().length;
    }

    @Override
    public SeekableInputStream newStream() {
      return new MemoryInputStream(bytes());
    }

    @Override
    public String location() {
      return location;
    }

    @Override
    public boolean exists() {
      return RecordingFileIO.data.containsKey(location);
    }

    private byte[] bytes() {
      return RecordingFileIO.data.getOrDefault(location, new byte[0]);
    }
  }

  private static class MemoryOutputFile implements OutputFile {
    private final String location;

    private MemoryOutputFile(String location) {
      this.location = location;
    }

    @Override
    public PositionOutputStream create() {
      if (RecordingFileIO.data.containsKey(location)) {
        throw new IllegalStateException("File already exists: " + location);
      }

      return createOrOverwrite();
    }

    @Override
    public PositionOutputStream createOrOverwrite() {
      return new MemoryOutputStream(location);
    }

    @Override
    public String location() {
      return location;
    }

    @Override
    public InputFile toInputFile() {
      return new MemoryInputFile(location);
    }
  }

  private static class NativeMemoryInputFile extends MemoryInputFile
      implements NativeEncryptionInputFile {
    private final NativeEncryptionKeyMetadata keyMetadata;

    private NativeMemoryInputFile(String location, NativeEncryptionKeyMetadata keyMetadata) {
      super(location);
      this.keyMetadata = keyMetadata;
    }

    @Override
    public InputFile encryptedInputFile() {
      return new MemoryInputFile(location());
    }

    @Override
    public NativeEncryptionKeyMetadata keyMetadata() {
      return keyMetadata;
    }
  }

  private static class NativeMemoryOutputFile extends MemoryOutputFile
      implements NativeEncryptionOutputFile {
    private final NativeEncryptionKeyMetadata keyMetadata;

    private NativeMemoryOutputFile(String location, NativeEncryptionKeyMetadata keyMetadata) {
      super(location);
      this.keyMetadata = keyMetadata;
    }

    @Override
    public OutputFile encryptingOutputFile() {
      return new MemoryOutputFile(location());
    }

    @Override
    public OutputFile plainOutputFile() {
      return new MemoryOutputFile(location());
    }

    @Override
    public NativeEncryptionKeyMetadata keyMetadata() {
      return keyMetadata;
    }
  }

  private static class TestNativeKeyMetadata implements NativeEncryptionKeyMetadata {
    @Override
    public ByteBuffer encryptionKey() {
      return ByteBuffer.wrap(new byte[] {1});
    }

    @Override
    public ByteBuffer aadPrefix() {
      return ByteBuffer.wrap(new byte[] {2});
    }

    @Override
    public ByteBuffer buffer() {
      return ByteBuffer.wrap(new byte[] {3});
    }

    @Override
    public NativeEncryptionKeyMetadata copy() {
      return this;
    }
  }

  private static class MemoryInputStream extends SeekableInputStream
      implements DelegatingInputStream, RangeReadable {
    private final byte[] data;
    private int position;

    private MemoryInputStream(byte[] data) {
      this.data = data;
    }

    @Override
    public long getPos() {
      return position;
    }

    @Override
    public void seek(long newPos) {
      this.position = Math.toIntExact(newPos);
    }

    @Override
    public int read() {
      return position < data.length ? data[position++] & 0xff : -1;
    }

    @Override
    public int read(byte[] buffer, int off, int len) {
      if (position >= data.length) {
        return -1;
      }

      int bytesToRead = Math.min(len, data.length - position);
      System.arraycopy(data, position, buffer, off, bytesToRead);
      position += bytesToRead;
      return bytesToRead;
    }

    @Override
    public void readFully(long readPosition, byte[] buffer, int offset, int length) {
      System.arraycopy(data, Math.toIntExact(readPosition), buffer, offset, length);
    }

    @Override
    public int readTail(byte[] buffer, int offset, int length) {
      int bytesToRead = Math.min(length, data.length);
      System.arraycopy(data, data.length - bytesToRead, buffer, offset, bytesToRead);
      return bytesToRead;
    }

    @Override
    public ByteArrayInputStream getDelegate() {
      return new ByteArrayInputStream(data);
    }
  }

  private static class MemoryOutputStream extends PositionOutputStream
      implements DelegatingOutputStream {
    private final String location;
    private final ByteArrayOutputStream bytes = new ByteArrayOutputStream();

    private MemoryOutputStream(String location) {
      this.location = location;
    }

    @Override
    public long getPos() {
      return bytes.size();
    }

    @Override
    public void write(int value) {
      bytes.write(value);
    }

    @Override
    public void write(byte[] buffer, int off, int len) {
      bytes.write(buffer, off, len);
    }

    @Override
    public void close() {
      RecordingFileIO.data.put(location, bytes.toByteArray());
    }

    @Override
    public ByteArrayOutputStream getDelegate() {
      return bytes;
    }
  }
}
