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
import static org.assertj.core.api.AssertionsForClassTypes.assertThatCode;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.withSettings;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.TestHelpers;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;

public class TestResolvingIO {

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
  public void testResolvingFileIOJavaSerialization() throws IOException, ClassNotFoundException {
    FileIO testResolvingFileIO = new ResolvingFileIO();

    // resolving fileIO should be serializable when properties are passed as immutable map
    testResolvingFileIO.initialize(ImmutableMap.of("k1", "v1"));
    FileIO roundTripSerializedFileIO = TestHelpers.roundTripSerialize(testResolvingFileIO);
    assertThat(roundTripSerializedFileIO.properties()).isEqualTo(testResolvingFileIO.properties());
  }

  @Test
  public void testEnsureInterfaceImplementation() {
    ResolvingFileIO testResolvingFileIO = spy(new ResolvingFileIO());
    testResolvingFileIO.setConf(new Configuration());
    testResolvingFileIO.initialize(ImmutableMap.of());

    String fileIONoMixins = mock(FileIO.class).getClass().getName();
    doReturn(fileIONoMixins).when(testResolvingFileIO).implFromLocation(any());
    assertThatThrownBy(() -> testResolvingFileIO.newInputFile("/file"))
        .isInstanceOf(IllegalStateException.class);

    String fileIOWithMixins =
        mock(
                FileIO.class,
                withSettings()
                    .extraInterfaces(SupportsPrefixOperations.class, SupportsBulkOperations.class))
            .getClass()
            .getName();
    doReturn(fileIOWithMixins).when(testResolvingFileIO).implFromLocation(any());
    assertThatCode(() -> testResolvingFileIO.newInputFile("/file")).doesNotThrowAnyException();
  }
}
