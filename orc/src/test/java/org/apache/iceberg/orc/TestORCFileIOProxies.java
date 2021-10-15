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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.Files;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.junit.Test;

public class TestORCFileIOProxies {
  @Test
  public void testInputFileSystem() throws IOException {
    File inputFile = File.createTempFile("read", ".orc");
    inputFile.deleteOnExit();

    InputFile localFile = Files.localInput(inputFile);
    ORC.InputFileSystem ifs = new ORC.InputFileSystem(localFile);
    InputStream is = ifs.open(new Path(localFile.location()));
    assertNotNull(is);

    // Cannot use the filesystem for any other operation
    assertThrows(
        UnsupportedOperationException.class,
        () -> ifs.getFileStatus(new Path(localFile.location())));

    // Cannot use the filesystem for any other path
    assertThrows(IllegalArgumentException.class, () -> ifs.open(new Path("/tmp/dummy")));
  }

  @Test
  public void testOutputFileSystem() throws IOException {
    File localFile = File.createTempFile("write", ".orc");
    localFile.deleteOnExit();

    OutputFile outputFile = Files.localOutput(localFile);
    FileSystem ofs = new ORC.OutputFileSystem(outputFile);
    try (OutputStream os = ofs.create(new Path(outputFile.location()))) {
      os.write('O');
      os.write('R');
      os.write('C');
    }
    // No other operation is supported
    assertThrows(
        UnsupportedOperationException.class, () -> ofs.open(new Path(outputFile.location())));
    // No other path is supported
    assertThrows(IllegalArgumentException.class, () -> ofs.create(new Path("/tmp/dummy")));

    FileSystem ifs = new ORC.InputFileSystem(outputFile.toInputFile());
    try (InputStream is = ifs.open(new Path(outputFile.location()))) {
      assertEquals('O', is.read());
      assertEquals('R', is.read());
      assertEquals('C', is.read());
      assertEquals(-1, is.read());
    }
  }
}
