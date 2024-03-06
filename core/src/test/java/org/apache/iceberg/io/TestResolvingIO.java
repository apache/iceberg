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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.withSettings;

import java.io.IOException;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.TestHelpers;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class TestResolvingIO {

  @TempDir private java.nio.file.Path temp;

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
    ResolvingFileIO resolvingFileIO = spy(new ResolvingFileIO());
    Configuration hadoopConf = new Configuration();
    FileSystem fs = FileSystem.getLocal(hadoopConf);
    Path parent = new Path(temp.toUri());
    // configure delegation IO
    HadoopFileIO delegation = new HadoopFileIO(hadoopConf);
    doReturn(delegation).when(resolvingFileIO).io(anyString());
    // write
    List<Path> randomFilePaths =
        IntStream.range(1, 10)
            .mapToObj(i -> new Path(parent, "random-" + i + "-" + UUID.randomUUID()))
            .collect(Collectors.toList());
    for (Path randomFilePath : randomFilePaths) {
      fs.createNewFile(randomFilePath);
      assertThat(delegation.newInputFile(randomFilePath.toUri().toString()).exists()).isTrue();
    }
    // bulk deletion
    List<String> randomFilePathString =
        randomFilePaths.stream().map(p -> p.toUri().toString()).collect(Collectors.toList());
    resolvingFileIO.deleteFiles(randomFilePathString);

    for (String path : randomFilePathString) {
      assertThat(delegation.newInputFile(path).exists()).isFalse();
    }
  }

  @Test
  public void delegateFileIOWithPrefixBasedSupport() throws IOException {
    ResolvingFileIO resolvingFileIO = spy(new ResolvingFileIO());
    Configuration hadoopConf = new Configuration();
    FileSystem fs = FileSystem.getLocal(hadoopConf);
    Path parent = new Path(temp.toUri());
    HadoopFileIO delegate = new HadoopFileIO(hadoopConf);
    doReturn(delegate).when(resolvingFileIO).io(anyString());

    List<Path> paths =
        IntStream.range(1, 10)
            .mapToObj(i -> new Path(parent, "random-" + i + "-" + UUID.randomUUID()))
            .collect(Collectors.toList());
    for (Path path : paths) {
      fs.createNewFile(path);
      assertThat(delegate.newInputFile(path.toString()).exists()).isTrue();
    }

    paths.stream()
        .map(Path::toString)
        .forEach(
            path -> {
              // HadoopFileIO can only list prefixes that match the full path
              assertThat(resolvingFileIO.listPrefix(path)).hasSize(1);
              resolvingFileIO.deletePrefix(path);
              assertThat(delegate.newInputFile(path).exists()).isFalse();
            });
  }

  @Test
  public void delegateFileIOWithAndWithoutMixins() {
    ResolvingFileIO resolvingFileIO = spy(new ResolvingFileIO());
    resolvingFileIO.setConf(new Configuration());
    resolvingFileIO.initialize(ImmutableMap.of());

    String fileIONoMixins = mock(FileIO.class).getClass().getName();
    doReturn(fileIONoMixins).when(resolvingFileIO).implFromLocation(any());
    assertThatThrownBy(() -> resolvingFileIO.newInputFile("/file"))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageStartingWith(
            "FileIO does not implement DelegateFileIO: org.apache.iceberg.io.FileIO");

    String fileIOWithMixins =
        mock(FileIO.class, withSettings().extraInterfaces(DelegateFileIO.class))
            .getClass()
            .getName();
    doReturn(fileIOWithMixins).when(resolvingFileIO).implFromLocation(any());
    // being null is ok here as long as the code doesn't throw an exception
    assertThat(resolvingFileIO.newInputFile("/file")).isNull();
  }

  @Test
  public void customFileIOScheme() {
    ResolvingFileIO resolvingFileIO = new ResolvingFileIO();
    resolvingFileIO.setConf(new Configuration());
    resolvingFileIO.initialize(
        ImmutableMap.of(
            "resolving-io.schemes.custom", "org.apache.iceberg.io.TestCustomResolvingFileIO"));

    // testing custom scheme
    assertThat(resolvingFileIO.ioClass("custom://foo")).isEqualTo(TestCustomResolvingFileIO.class);
  }
}
