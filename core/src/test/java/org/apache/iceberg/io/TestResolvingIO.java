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
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
//import java.nio.file.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.TestHelpers;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.inmemory.InMemoryFileIO;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class TestResolvingIO {

  @TempDir private Path temp;

  @TempDir private File tempDir;

  @Test
  public void testResolvingFileIOKryoSerialization() throws IOException {
    FileIO testResolvingFileIO = new ResolvingFileIO();

    // resolving fileIO should be serializable when properties are passed as immutable map
    testResolvingFileIO.initialize(ImmutableMap.of("k1", "v1"));
    FileIO roundTripSerializedFileIO =
        TestHelpers.KryoHelpers.roundTripSerialize(testResolvingFileIO);
    assertThat(roundTripSerializedFileIO.properties()).isEqualTo(testResolvingFileIO.properties());
  }

  @Test
  public void testResolvingFileIOWithHadoopFileIOKryoSerialization() throws IOException {
    ResolvingFileIO resolvingFileIO = new ResolvingFileIO();
    Configuration conf = new Configuration();
    resolvingFileIO.setConf(conf);
    resolvingFileIO.initialize(ImmutableMap.of("k1", "v1"));

    assertThat(resolvingFileIO.ioClass(temp.toString())).isEqualTo(HadoopFileIO.class);
    assertThat(resolvingFileIO.newInputFile(temp.toString())).isNotNull();

    ResolvingFileIO roundTripSerializedFileIO =
        TestHelpers.KryoHelpers.roundTripSerialize(resolvingFileIO);
    roundTripSerializedFileIO.setConf(conf);
    assertThat(roundTripSerializedFileIO.properties()).isEqualTo(resolvingFileIO.properties());

    assertThat(roundTripSerializedFileIO.ioClass(temp.toString())).isEqualTo(HadoopFileIO.class);
    assertThat(roundTripSerializedFileIO.newInputFile(temp.toString())).isNotNull();
  }

  @Test
  public void testResolvingFileIOJavaSerialization() throws IOException, ClassNotFoundException {
    FileIO testResolvingFileIO = new ResolvingFileIO();

    // resolving fileIO should be serializable when properties are passed as immutable map
    testResolvingFileIO.initialize(ImmutableMap.of("k1", "v1"));
    FileIO roundTripSerializedFileIO = TestHelpers.roundTripSerialize(testResolvingFileIO);
    assertThat(roundTripSerializedFileIO.properties()).isEqualTo(testResolvingFileIO.properties());
  }

  @Test
  public void testResolvingFileIOWithHadoopFileIOJavaSerialization()
      throws IOException, ClassNotFoundException {
    ResolvingFileIO resolvingFileIO = new ResolvingFileIO();
    Configuration conf = new Configuration();
    resolvingFileIO.setConf(conf);
    resolvingFileIO.initialize(ImmutableMap.of("k1", "v1"));

    assertThat(resolvingFileIO.ioClass(temp.toString())).isEqualTo(HadoopFileIO.class);
    assertThat(resolvingFileIO.newInputFile(temp.toString())).isNotNull();

    ResolvingFileIO roundTripSerializedFileIO = TestHelpers.roundTripSerialize(resolvingFileIO);
    roundTripSerializedFileIO.setConf(conf);
    assertThat(roundTripSerializedFileIO.properties()).isEqualTo(resolvingFileIO.properties());

    assertThat(roundTripSerializedFileIO.ioClass(temp.toString())).isEqualTo(HadoopFileIO.class);
    assertThat(roundTripSerializedFileIO.newInputFile(temp.toString())).isNotNull();
  }

  @Test
  public void resolveFileIOBulkDeletion() throws IOException {
    // configure resolving fileIO
    ResolvingFileIO resolvingFileIO = new ResolvingFileIO();
    Configuration hadoopConf = new Configuration();
    resolvingFileIO.setConf(hadoopConf);
    resolvingFileIO.initialize(
        ImmutableMap.of("io-impl", "org.apache.iceberg.hadoop.HadoopFileIO"));
    ResolvingFileIO spy = spy(resolvingFileIO);
    // configure delegation IO
    FileSystem fs = FileSystem.getLocal(hadoopConf);
    Path parent = new Path(tempDir.toURI());
    HadoopFileIO delegate = new HadoopFileIO(hadoopConf);
    when(spy.io(anyString())).thenReturn(delegate);
    // write
    List<Path> randomFilePaths =
        IntStream.range(1, 10)
            .mapToObj(i -> new Path(parent, "random-" + i + "-" + UUID.randomUUID()))
            .collect(Collectors.toList());
    for (Path randomFilePath : randomFilePaths) {
      fs.createNewFile(randomFilePath);
      assertThat(delegate.newInputFile(randomFilePath.toUri().toString()).exists()).isTrue();
    }
    // bulk deletion
    List<String> randomFilePathString =
        randomFilePaths.stream().map(p -> p.toUri().toString()).collect(Collectors.toList());
    spy.deleteFiles(randomFilePathString);

    for (String path : randomFilePathString) {
      assertThat(delegate.newInputFile(path).exists()).isFalse();
    }
  }

  @Test
  public void resolveFileIONonBulkDeletion() {
    // configure resolving fileIO
    ResolvingFileIO resolvingFileIO = new ResolvingFileIO();
    Configuration hadoopConf = new Configuration();
    resolvingFileIO.setConf(hadoopConf);
    resolvingFileIO.initialize(
        ImmutableMap.of("io-impl", "org.apache.iceberg.inmemory.InMemoryFileIO"));
    ResolvingFileIO spy = spy(resolvingFileIO);
    // configure delegation IO
    String parentPath = "inmemory://foo.db/bar";
    InMemoryFileIO delegation = new InMemoryFileIO();
    when(spy.io(anyString())).thenReturn(delegation);
    // write
    byte[] someData = "some data".getBytes();

    List<String> randomFilePaths =
        IntStream.range(1, 10)
            .mapToObj(i -> parentPath + "-" + i + "-" + UUID.randomUUID())
            .collect(Collectors.toList());
    for (String randomFilePath : randomFilePaths) {
      delegation.addFile(randomFilePath, someData);
      assertThat(delegation.fileExists(randomFilePath)).isTrue();
    }
    // non-bulk deletion
    spy.deleteFiles(randomFilePaths);

    for (String path : randomFilePaths) {
      assertThat(delegation.fileExists(path)).isFalse();
    }
  }
}
