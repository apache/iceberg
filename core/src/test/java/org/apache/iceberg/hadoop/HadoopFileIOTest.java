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
package org.apache.iceberg.hadoop;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Random;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Streams;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class HadoopFileIOTest {
  private final Random random = new Random(1);

  private FileSystem fs;
  private HadoopFileIO hadoopFileIO;

  @TempDir static File tempDir;

  @BeforeEach
  public void before() throws Exception {
    Configuration conf = new Configuration();
    fs = FileSystem.getLocal(conf);

    hadoopFileIO = new HadoopFileIO(conf);
  }

  @Test
  public void testListPrefix() {
    Path parent = new Path(tempDir.toURI());

    List<Integer> scaleSizes = Lists.newArrayList(1, 1000, 2500);

    scaleSizes
        .parallelStream()
        .forEach(
            scale -> {
              Path scalePath = new Path(parent, Integer.toString(scale));

              createRandomFiles(scalePath, scale);
              assertEquals(
                  (long) scale,
                  Streams.stream(hadoopFileIO.listPrefix(scalePath.toUri().toString())).count());
            });

    long totalFiles = scaleSizes.stream().mapToLong(Integer::longValue).sum();
    assertEquals(
        totalFiles, Streams.stream(hadoopFileIO.listPrefix(parent.toUri().toString())).count());
  }

  @Test
  public void testDeletePrefix() {
    Path parent = new Path(tempDir.toURI());

    List<Integer> scaleSizes = Lists.newArrayList(1, 1000, 2500);

    scaleSizes
        .parallelStream()
        .forEach(
            scale -> {
              Path scalePath = new Path(parent, Integer.toString(scale));

              createRandomFiles(scalePath, scale);
              hadoopFileIO.deletePrefix(scalePath.toUri().toString());

              // Hadoop filesystem will throw if the path does not exist
              assertThrows(
                  UncheckedIOException.class,
                  () -> hadoopFileIO.listPrefix(scalePath.toUri().toString()).iterator());
            });

    hadoopFileIO.deletePrefix(parent.toUri().toString());
    // Hadoop filesystem will throw if the path does not exist
    assertThrows(
        UncheckedIOException.class,
        () -> hadoopFileIO.listPrefix(parent.toUri().toString()).iterator());
  }

  private void createRandomFiles(Path parent, int count) {
    random
        .ints(count)
        .parallel()
        .forEach(
            i -> {
              try {
                fs.createNewFile(new Path(parent, "file-" + i));
              } catch (IOException e) {
                throw new UncheckedIOException(e);
              }
            });
  }
}
