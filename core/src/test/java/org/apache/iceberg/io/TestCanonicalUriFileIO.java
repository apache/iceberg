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
package org.apache.iceberg.io;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

public class TestCanonicalUriFileIO {

  @Test
  public void testCanonicalLocationContract() {
    String canonical = "storage://warehouse/db/table/data.parquet";
    String physical = "s3://warehouse/db/table/data.parquet";

    org.apache.iceberg.inmemory.InMemoryFileIO delegate =
        new org.apache.iceberg.inmemory.InMemoryFileIO();

    CanonicalUriFileIO io =
        new CanonicalUriFileIO(delegate, "storage://warehouse/", "s3://warehouse/");

    OutputFile output = io.newOutputFile(canonical);

    assertThat(output.location()).isEqualTo(canonical);

    try (PositionOutputStream stream = output.create()) {
      stream.write(42);
    } catch (java.io.IOException e) {
      throw new java.io.UncheckedIOException(e);
    }

    assertThat(output.toInputFile().location()).isEqualTo(canonical);
    assertThat(io.newInputFile(canonical).location()).isEqualTo(canonical);

    assertThat(delegate.fileExists(physical)).isTrue();
    assertThat(delegate.fileExists(canonical)).isFalse();

    try (SeekableInputStream stream = io.newInputFile(canonical).newStream()) {
      assertThat(stream.read()).isEqualTo(42);
    } catch (java.io.IOException e) {
      throw new java.io.UncheckedIOException(e);
    }

    io.deleteFile(canonical);
    assertThat(delegate.fileExists(physical)).isFalse();
  }

  @Test
  public void testUnmappedPhysicalLocation() {
    org.apache.iceberg.inmemory.InMemoryFileIO delegate =
        new org.apache.iceberg.inmemory.InMemoryFileIO();

    CanonicalUriFileIO io =
        new CanonicalUriFileIO(delegate, "storage://warehouse/", "s3://warehouse/");

    String physical = "s3://warehouse/existing.parquet";
    delegate.addFile(physical, new byte[] {42});

    assertThat(io.newInputFile(physical).location()).isEqualTo(physical);
    assertThat(io.newInputFile(physical).exists()).isTrue();
  }

  @Test
  public void testInvalidUriMappingConfiguration() {
    org.apache.iceberg.inmemory.InMemoryFileIO delegate =
        new org.apache.iceberg.inmemory.InMemoryFileIO();

    assertThatThrownBy(() -> new CanonicalUriFileIO(delegate, "", "s3://warehouse/"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Canonical URI prefix must not be null or empty");

    assertThatThrownBy(() -> new CanonicalUriFileIO(delegate, "storage://warehouse/", ""))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Physical URI prefix must not be null or empty");

    assertThatThrownBy(() -> new CanonicalUriFileIO(delegate, null, "s3://warehouse/"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Canonical URI prefix must not be null or empty");

    assertThatThrownBy(() -> new CanonicalUriFileIO(delegate, "storage://warehouse/", null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Physical URI prefix must not be null or empty");
  }

  @Test
  public void testCanonicalPrefixBoundary() {
    org.apache.iceberg.inmemory.InMemoryFileIO delegate =
        new org.apache.iceberg.inmemory.InMemoryFileIO();

    CanonicalUriFileIO io =
        new CanonicalUriFileIO(delegate, "storage://warehouse/", "s3://warehouse/");

    String canonical = "storage://warehouse/data.parquet";
    String similar = "storage://warehouse-other/data.parquet";

    try (PositionOutputStream stream = io.newOutputFile(canonical).create()) {
      stream.write(42);
    } catch (java.io.IOException e) {
      throw new java.io.UncheckedIOException(e);
    }

    assertThat(delegate.fileExists("s3://warehouse/data.parquet")).isTrue();

    try (PositionOutputStream stream = io.newOutputFile(similar).create()) {
      stream.write(43);
    } catch (java.io.IOException e) {
      throw new java.io.UncheckedIOException(e);
    }

    assertThat(delegate.fileExists(similar)).isTrue();
    assertThat(delegate.fileExists("s3://warehouse-other/data.parquet")).isFalse();
  }

  @Test
  public void testUriPrefixesRequireTrailingSlash() {
    org.apache.iceberg.inmemory.InMemoryFileIO delegate =
        new org.apache.iceberg.inmemory.InMemoryFileIO();

    assertThatThrownBy(
            () -> new CanonicalUriFileIO(delegate, "storage://warehouse", "s3://warehouse/"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("must end with");

    assertThatThrownBy(
            () -> new CanonicalUriFileIO(delegate, "storage://warehouse/", "s3://warehouse"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("must end with");
  }

  @Test
  public void testBulkAndPrefixOperations() {
    PrefixTestFileIO delegate = new PrefixTestFileIO();

    CanonicalUriFileIO io =
        new CanonicalUriFileIO(delegate, "storage://warehouse/", "s3://warehouse/");

    String first = "storage://warehouse/table/first.parquet";
    String second = "storage://warehouse/table/second.parquet";
    String third = "storage://warehouse/table/third.parquet";
    String physicalFirst = "s3://warehouse/table/first.parquet";
    String physicalSecond = "s3://warehouse/table/second.parquet";
    String physicalThird = "s3://warehouse/table/third.parquet";

    delegate.addFile(physicalFirst, new byte[] {1});
    delegate.addFile(physicalSecond, new byte[] {2});
    delegate.addFile(physicalThird, new byte[] {3});

    assertThat(io.listPrefix("storage://warehouse/table/"))
        .extracting(FileInfo::location)
        .containsExactlyInAnyOrder(first, second, third);

    io.deleteFiles(java.util.Arrays.asList(first, second));

    assertThat(delegate.fileExists(physicalFirst)).isFalse();
    assertThat(delegate.fileExists(physicalSecond)).isFalse();
    assertThat(delegate.fileExists(physicalThird)).isTrue();

    io.deletePrefix("storage://warehouse/table/");

    assertThat(delegate.fileExists(physicalThird)).isFalse();
  }

  private static class PrefixTestFileIO extends org.apache.iceberg.inmemory.InMemoryFileIO
      implements SupportsPrefixOperations {

    private final java.util.List<String> paths = new java.util.ArrayList<>();

    @Override
    public void addFile(String location, byte[] contents) {
      super.addFile(location, contents);
      paths.add(location);
    }

    @Override
    public Iterable<FileInfo> listPrefix(String prefix) {
      java.util.List<FileInfo> files = new java.util.ArrayList<>();
      for (String path : paths) {
        if (path.startsWith(prefix) && fileExists(path)) {
          files.add(new FileInfo(path, newInputFile(path).getLength(), 0L));
        }
      }
      return files;
    }

    @Override
    public void deletePrefix(String prefix) {
      for (String path : paths) {
        if (path.startsWith(prefix) && fileExists(path)) {
          deleteFile(path);
        }
      }
    }
  }

  @Test
  public void testSerializationRoundTrip() throws Exception {
    CanonicalUriFileIO io =
        new CanonicalUriFileIO(
            new org.apache.iceberg.inmemory.InMemoryFileIO(),
            "storage://warehouse/",
            "s3://warehouse/");

    java.io.ByteArrayOutputStream bytes = new java.io.ByteArrayOutputStream();

    try (java.io.ObjectOutputStream output = new java.io.ObjectOutputStream(bytes)) {
      output.writeObject(io);
    }

    CanonicalUriFileIO restored;

    try (java.io.ObjectInputStream input =
        new java.io.ObjectInputStream(new java.io.ByteArrayInputStream(bytes.toByteArray()))) {
      restored = (CanonicalUriFileIO) input.readObject();
    }

    assertThat(restored.newOutputFile("storage://warehouse/test.parquet").location())
        .isEqualTo("storage://warehouse/test.parquet");
  }
}
