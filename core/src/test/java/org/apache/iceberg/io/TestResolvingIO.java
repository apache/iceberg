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

import java.io.IOException;
import java.nio.file.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.TestHelpers;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class TestResolvingIO {

  @TempDir private Path temp;

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
}
