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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.ByteArrayInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.junit.jupiter.api.Test;

public class TestHadoopTableOperations {
  @Test
  public void testVersionHintRetryBeforeMetadataScan() {
    Configuration conf = new Configuration();
    conf.setInt(ConfigProperties.VERSION_HINT_NUM_RETRIES, 2);
    conf.setLong(ConfigProperties.VERSION_HINT_RETRY_MIN_WAIT_MS, 0L);

    TestingFileSystem fs = new TestingFileSystem();
    fs.addDirectory("metadata");
    fs.addFile("v3.metadata.json", "");
    fs.addVersionHintAvailableAfterAttempt(3, "7");

    HadoopTableOperations ops = newTableOps(conf, fs);

    assertThat(ops.findVersion()).isEqualTo(7);
    assertThat(fs.versionHintOpenAttempts()).isEqualTo(3);
    assertThat(fs.metadataRootExistsCalls()).isEqualTo(1);
    assertThat(fs.listStatusCalls()).isZero();
  }

  @Test
  public void testVersionHintRetriesCanBeDisabled() {
    Configuration conf = new Configuration();
    conf.setInt(ConfigProperties.VERSION_HINT_NUM_RETRIES, 0);

    TestingFileSystem fs = new TestingFileSystem();
    fs.addDirectory("metadata");
    fs.addFile("v1.metadata.json", "");
    fs.addFile("v3.metadata.json", "");

    HadoopTableOperations ops = newTableOps(conf, fs);

    assertThat(ops.findVersion()).isEqualTo(3);
    assertThat(fs.versionHintOpenAttempts()).isEqualTo(1);
    assertThat(fs.metadataRootExistsCalls()).isEqualTo(1);
    assertThat(fs.listStatusCalls()).isEqualTo(1);
  }

  @Test
  public void testConfirmedMetadataRootIsNotCheckedAgainBeforeFallbackScan() {
    Configuration conf = new Configuration();
    conf.setInt(ConfigProperties.VERSION_HINT_NUM_RETRIES, 2);
    conf.setLong(ConfigProperties.VERSION_HINT_RETRY_MIN_WAIT_MS, 0L);

    TestingFileSystem fs = new TestingFileSystem();
    fs.addDirectory("metadata");
    fs.addFile("v3.metadata.json", "");
    fs.failMetadataRootExistsAfterCall(1);

    HadoopTableOperations ops = newTableOps(conf, fs);

    assertThat(ops.findVersion()).isEqualTo(3);
    assertThat(fs.versionHintOpenAttempts()).isEqualTo(3);
    assertThat(fs.metadataRootExistsCalls()).isEqualTo(1);
    assertThat(fs.listStatusCalls()).isEqualTo(1);
  }

  @Test
  public void testCorruptVersionHintDoesNotRetry() {
    Configuration conf = new Configuration();
    conf.setInt(ConfigProperties.VERSION_HINT_NUM_RETRIES, 2);

    TestingFileSystem fs = new TestingFileSystem();
    fs.addDirectory("metadata");
    fs.addFile("v3.metadata.json", "");
    fs.addVersionHintAvailableAfterAttempt(1, "not-a-version");

    HadoopTableOperations ops = newTableOps(conf, fs);

    assertThat(ops.findVersion()).isEqualTo(3);
    assertThat(fs.versionHintOpenAttempts()).isEqualTo(1);
    assertThat(fs.metadataRootExistsCalls()).isEqualTo(1);
    assertThat(fs.listStatusCalls()).isEqualTo(1);
  }

  @Test
  public void testCorruptVersionHintFallsBackWhenThreadAlreadyInterrupted() {
    Configuration conf = new Configuration();
    conf.setInt(ConfigProperties.VERSION_HINT_NUM_RETRIES, 2);

    TestingFileSystem fs = new TestingFileSystem();
    fs.addDirectory("metadata");
    fs.addFile("v3.metadata.json", "");
    fs.addVersionHintAvailableAfterAttempt(1, "not-a-version");

    HadoopTableOperations ops = newTableOps(conf, fs);

    try {
      Thread.currentThread().interrupt();
      assertThat(ops.findVersion()).isEqualTo(3);
      assertThat(Thread.currentThread().isInterrupted()).isTrue();
      assertThat(fs.versionHintOpenAttempts()).isEqualTo(1);
      assertThat(fs.metadataRootExistsCalls()).isEqualTo(1);
      assertThat(fs.listStatusCalls()).isEqualTo(1);
    } finally {
      Thread.interrupted();
    }
  }

  @Test
  public void testVersionHintRetryStopsAtTotalTimeout() {
    Configuration conf = new Configuration();
    conf.setInt(ConfigProperties.VERSION_HINT_NUM_RETRIES, 2);
    conf.setLong(ConfigProperties.VERSION_HINT_RETRY_MIN_WAIT_MS, 10L);
    conf.setLong(ConfigProperties.VERSION_HINT_RETRY_TOTAL_TIMEOUT_MS, 1L);

    TestingFileSystem fs = new TestingFileSystem();
    fs.addDirectory("metadata");
    fs.addFile("v3.metadata.json", "");
    fs.addVersionHintAvailableAfterAttempt(3, "7");

    HadoopTableOperations ops = newTableOps(conf, fs);

    assertThat(ops.findVersion()).isEqualTo(3);
    assertThat(fs.versionHintOpenAttempts()).isEqualTo(2);
    assertThat(fs.metadataRootExistsCalls()).isEqualTo(1);
    assertThat(fs.listStatusCalls()).isEqualTo(1);
  }

  @Test
  public void testMissingMetadataRootDoesNotRetryVersionHint() {
    TestingFileSystem fs = new TestingFileSystem();
    HadoopTableOperations ops = newTableOps(new Configuration(), fs);

    assertThat(ops.findVersion()).isZero();
    assertThat(fs.versionHintOpenAttempts()).isEqualTo(1);
    assertThat(fs.metadataRootExistsCalls()).isEqualTo(1);
    assertThat(fs.listStatusCalls()).isZero();
  }

  @Test
  public void testInterruptedVersionHintRetryThrows() {
    Configuration conf = new Configuration();
    conf.setInt(ConfigProperties.VERSION_HINT_NUM_RETRIES, 2);
    conf.setLong(ConfigProperties.VERSION_HINT_RETRY_MIN_WAIT_MS, 100L);

    TestingFileSystem fs = new TestingFileSystem();
    fs.addDirectory("metadata");
    HadoopTableOperations ops = newTableOps(conf, fs);

    try {
      Thread.currentThread().interrupt();
      assertThatThrownBy(ops::findVersion)
          .isInstanceOf(RuntimeException.class)
          .hasMessageContaining("InterruptedException")
          .hasCauseInstanceOf(InterruptedException.class);
      assertThat(Thread.currentThread().isInterrupted()).isTrue();
      assertThat(fs.versionHintOpenAttempts()).isEqualTo(1);
      assertThat(fs.metadataRootExistsCalls()).isEqualTo(1);
      assertThat(fs.listStatusCalls()).isZero();
    } finally {
      Thread.interrupted();
    }
  }

  private static HadoopTableOperations newTableOps(Configuration conf, FileSystem fs) {
    return new HadoopTableOperations(new Path("test:/table"), null, conf, null) {
      @Override
      protected FileSystem getFileSystem(Path path, Configuration hadoopConf) {
        return fs;
      }
    };
  }

  private static class TestingFileSystem extends FileSystem {
    private final Map<String, byte[]> files = Maps.newLinkedHashMap();
    private int listStatusCalls = 0;
    private int metadataRootExistsCalls = 0;
    private int failMetadataRootExistsAfterCall = Integer.MAX_VALUE;
    private int versionHintOpenAttempts = 0;
    private int versionHintAvailableAfterAttempt = Integer.MAX_VALUE;

    private void addDirectory(String name) {
      files.put(name, null);
    }

    private void addFile(String name, String content) {
      files.put(name, content.getBytes(StandardCharsets.UTF_8));
    }

    private void addVersionHintAvailableAfterAttempt(int openAttempt, String content) {
      this.versionHintAvailableAfterAttempt = openAttempt;
      addFile(Util.VERSION_HINT_FILENAME, content);
    }

    private void failMetadataRootExistsAfterCall(int callCount) {
      this.failMetadataRootExistsAfterCall = callCount;
    }

    private int listStatusCalls() {
      return listStatusCalls;
    }

    private int versionHintOpenAttempts() {
      return versionHintOpenAttempts;
    }

    private int metadataRootExistsCalls() {
      return metadataRootExistsCalls;
    }

    @Override
    public URI getUri() {
      return URI.create("test:/");
    }

    @Override
    public FSDataInputStream open(Path f) throws IOException {
      return open(f, 4096);
    }

    @Override
    public FSDataInputStream open(Path f, int bufferSize) throws IOException {
      if (Util.VERSION_HINT_FILENAME.equals(f.getName())) {
        versionHintOpenAttempts += 1;
        if (versionHintOpenAttempts < versionHintAvailableAfterAttempt) {
          throw new FileNotFoundException(f.toString());
        }
      }

      byte[] content = files.get(f.getName());
      if (content == null) {
        throw new FileNotFoundException(f.toString());
      }

      return new FSDataInputStream(new ByteArrayFSInputStream(content));
    }

    @Override
    public FSDataOutputStream create(
        Path f,
        FsPermission permission,
        boolean overwrite,
        int bufferSize,
        short replication,
        long blockSize,
        Progressable progress) {
      throw new UnsupportedOperationException("create is not used");
    }

    @Override
    public FSDataOutputStream append(Path f, int bufferSize, Progressable progress) {
      throw new UnsupportedOperationException("append is not used");
    }

    @Override
    public boolean rename(Path src, Path dst) {
      throw new UnsupportedOperationException("rename is not used");
    }

    @Override
    public boolean delete(Path f, boolean recursive) {
      throw new UnsupportedOperationException("delete is not used");
    }

    @Override
    public FileStatus[] listStatus(Path f, PathFilter filter) {
      listStatusCalls += 1;
      return files.keySet().stream()
          .filter(name -> name.startsWith("v"))
          .map(TestingFileSystem::fileStatus)
          .filter(status -> filter.accept(status.getPath()))
          .toArray(FileStatus[]::new);
    }

    @Override
    public FileStatus[] listStatus(Path f) {
      return listStatus(f, path -> true);
    }

    @Override
    public void setWorkingDirectory(Path newDir) {}

    @Override
    public Path getWorkingDirectory() {
      return new Path("test:/");
    }

    @Override
    public boolean mkdirs(Path f, FsPermission permission) {
      throw new UnsupportedOperationException("mkdirs is not used");
    }

    @Override
    public boolean exists(Path f) throws IOException {
      if ("metadata".equals(f.getName())) {
        metadataRootExistsCalls += 1;
        if (metadataRootExistsCalls > failMetadataRootExistsAfterCall) {
          throw new IOException("Injected metadata root exists failure");
        }
      }

      return files.containsKey(f.getName());
    }

    @Override
    public FileStatus getFileStatus(Path f) throws IOException {
      if (!exists(f)) {
        throw new FileNotFoundException(f.toString());
      }

      return fileStatus(f.getName());
    }

    private static FileStatus fileStatus(String name) {
      return new FileStatus(0L, false, 1, 0L, 0L, new Path("test:/table/metadata/" + name));
    }
  }

  private static class ByteArrayFSInputStream extends FSInputStream {
    private final ByteArrayInputStream delegate;

    private ByteArrayFSInputStream(byte[] bytes) {
      this.delegate = new ByteArrayInputStream(bytes);
    }

    @Override
    public void seek(long pos) {
      delegate.reset();
      delegate.skip(pos);
    }

    @Override
    public long getPos() {
      return 0;
    }

    @Override
    public boolean seekToNewSource(long targetPos) {
      return false;
    }

    @Override
    public int read() {
      return delegate.read();
    }

    @Override
    public int read(byte[] b, int off, int len) {
      return delegate.read(b, off, len);
    }
  }
}
