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
package org.apache.iceberg.orc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.Files;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.junit.jupiter.api.Test;

public class TestORCFileIOProxies {
  @Test
  public void testInputFileSystem() throws IOException {
    File inputFile = File.createTempFile("read", ".orc");
    inputFile.deleteOnExit();

    InputFile localFile = Files.localInput(inputFile);
    FileIOFSUtil.InputFileSystem ifs = new FileIOFSUtil.InputFileSystem(localFile);
    InputStream is = ifs.open(new Path(localFile.location()));
    assertThat(is).isNotNull();

    // Cannot use the filesystem for any other operation
    assertThatThrownBy(() -> ifs.getFileStatus(new Path(localFile.location())))
        .isInstanceOf(UnsupportedOperationException.class);

    // Cannot use the filesystem for any other path
    assertThatThrownBy(() -> ifs.open(new Path("/tmp/dummy")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageStartingWith("Input /tmp/dummy does not equal expected");
  }

  @Test
  public void testOutputFileSystem() throws IOException {
    File localFile = File.createTempFile("write", ".orc");
    localFile.deleteOnExit();

    OutputFile outputFile = Files.localOutput(localFile);
    FileSystem ofs = new FileIOFSUtil.OutputFileSystem(outputFile);
    try (OutputStream os = ofs.create(new Path(outputFile.location()))) {
      os.write('O');
      os.write('R');
      os.write('C');
    }
    // No other operation is supported
    assertThatThrownBy(() -> ofs.open(new Path(outputFile.location())))
        .isInstanceOf(UnsupportedOperationException.class);
    // No other path is supported
    assertThatThrownBy(() -> ofs.create(new Path("/tmp/dummy")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageStartingWith("Input /tmp/dummy does not equal expected");

    FileSystem ifs = new FileIOFSUtil.InputFileSystem(outputFile.toInputFile());
    try (InputStream is = ifs.open(new Path(outputFile.location()))) {
      assertThat(is.read()).isEqualTo('O');
      assertThat(is.read()).isEqualTo('R');
      assertThat(is.read()).isEqualTo('C');
      assertThat(is.read()).isEqualTo(-1);
    }
  }
}
