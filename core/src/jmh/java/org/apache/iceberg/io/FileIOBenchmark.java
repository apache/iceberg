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

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.iceberg.hadoop.HadoopConfigurable;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

/**
 * A benchmark that evaluates the raw read/write performance of FileIO implementations.
 *
 * <p>To run this benchmark with HadoopFileIO on local disk (default):
 *
 * <pre>{@code
 * ./gradlew :iceberg-core:jmh -PjmhIncludeRegex=FileIOBenchmark
 * }</pre>
 *
 * <p>To run with OSSFileIO:
 *
 * <pre>{@code
 * ./gradlew :iceberg-core:jmh \
 *   -Dbenchmark.fileIOClass=org.apache.iceberg.aliyun.oss.OSSFileIO \
 *   -Dbenchmark.base.path=oss://bucket/benchmark-tmp/ \
 *   -Doss.endpoint=https://oss-cn-hangzhou.aliyuncs.com \
 *   -Dclient.access-key-id=xxx \
 *   -Dclient.access-key-secret=xxx \
 *   -PjmhIncludeRegex=FileIOBenchmark
 * }</pre>
 *
 * <p>To run with S3FileIO:
 *
 * <pre>{@code
 * ./gradlew :iceberg-core:jmh \
 *   -Dbenchmark.fileIOClass=org.apache.iceberg.aws.s3.S3FileIO \
 *   -Dbenchmark.base.path=s3://bucket/benchmark-tmp/ \
 *   -Ds3.endpoint=https://s3.amazonaws.com \
 *   -Ds3.access-key-id=xxx \
 *   -Ds3.secret-access-key=xxx \
 *   -PjmhIncludeRegex=FileIOBenchmark
 * }</pre>
 */
@Fork(1)
@Warmup(iterations = 3)
@Measurement(iterations = 5)
@BenchmarkMode({Mode.AverageTime, Mode.Throughput})
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Thread)
public class FileIOBenchmark {

  private static final String PROPERTIES_FILE = "benchmark-fileio.properties";

  private static final String[] JVM_PROPERTY_PREFIXES = {
    "java.", "sun.", "jdk.", "os.", "user.", "file.", "line.", "path.", "awt."
  };

  @Param("org.apache.iceberg.hadoop.HadoopFileIO")
  private String fileIOClass;

  @Param({"1", "64", "1024", "16384", "131072"})
  private int fileSizeKB;

  @Param({"4", "64", "256", "1024"})
  private int bufferSizeKB;

  private FileIO fileIO;
  private String runDir;
  private List<String> createdFiles;
  private byte[] writeBuffer;
  private byte[] readBuffer;
  private String readFilePath;
  private AtomicLong writeCounter;
  private Random random;

  @Setup(Level.Trial)
  public void before() {
    // allow system properties to override @Param values (e.g. -Dbenchmark.fileIOClass=...)
    String fileIOOverride = System.getProperty("benchmark.fileIOClass");
    if (fileIOOverride != null && !fileIOOverride.isEmpty()) {
      fileIOClass = fileIOOverride;
    }

    Map<String, String> properties = loadProperties();

    String basePath = properties.remove("benchmark.base.path");
    if (basePath == null || basePath.isEmpty()) {
      // default to local temp directory for HadoopFileIO
      try {
        basePath = Files.createTempDirectory("fileio-benchmark-").toAbsolutePath().toString();
      } catch (IOException e) {
        throw new UncheckedIOException("Failed to create temp directory", e);
      }
    }
    // remove trailing slash
    if (basePath.endsWith("/")) {
      basePath = basePath.substring(0, basePath.length() - 1);
    }

    if (HadoopFileIO.class.getName().equals(fileIOClass)) {
      Configuration conf = new Configuration();
      conf.set("fs.file.impl", LocalFileSystem.class.getName());
      conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
      fileIO = new HadoopFileIO(conf);
    } else {
      try {
        fileIO =
            Class.forName(fileIOClass)
                .asSubclass(FileIO.class)
                .getDeclaredConstructor()
                .newInstance();
      } catch (ReflectiveOperationException e) {
        throw new RuntimeException("Failed to create FileIO instance: " + fileIOClass, e);
      }

      if (fileIO instanceof HadoopConfigurable) {
        ((HadoopConfigurable) fileIO).setConf(new Configuration());
      }

      fileIO.initialize(properties);
    }

    runDir = basePath + "/bench-" + UUID.randomUUID();
    createdFiles = Lists.newArrayList();
    writeCounter = new AtomicLong(0);
    random = new Random(42);

    // pre-allocate buffers
    int bufSize = bufferSizeKB * 1024;
    writeBuffer = new byte[bufSize];
    random.nextBytes(writeBuffer);
    readBuffer = new byte[bufSize];

    // prepare test file for read benchmarks
    readFilePath = runDir + "/read-test-file";
    writeTestFile(readFilePath, fileSizeKB * 1024L);
    createdFiles.add(readFilePath);
  }

  @TearDown(Level.Trial)
  public void after() {
    if (fileIO == null) {
      return;
    }

    try {
      // Try batch delete first if supported (e.g. native S3).
      // Falls through to single-file cleanup on failure or if unsupported.
      boolean batchDeleted = false;
      if (fileIO instanceof SupportsPrefixOperations) {
        try {
          ((SupportsPrefixOperations) fileIO).deletePrefix(runDir);
          batchDeleted = true;
        } catch (Exception e) {
          // Batch delete may fail on S3-compatible stores (e.g. Alibaba Cloud OSS),
          // fall through to single-file deletion below.
        }
      }

      if (!batchDeleted) {
        // Single-file deletion: used when batch delete is unavailable or failed.
        for (String path : createdFiles) {
          try {
            fileIO.deleteFile(path);
          } catch (Exception ignored) {
            // best-effort cleanup
          }
        }
      }
    } finally {
      fileIO.close();
    }
  }

  @Benchmark
  @Threads(1)
  public void sequentialWrite() {
    String path = runDir + "/write-seq-" + writeCounter.getAndIncrement();
    OutputFile outputFile = fileIO.newOutputFile(path);
    try (PositionOutputStream out = outputFile.create()) {
      long remaining = fileSizeKB * 1024L;
      while (remaining > 0) {
        int toWrite = (int) Math.min(writeBuffer.length, remaining);
        out.write(writeBuffer, 0, toWrite);
        remaining -= toWrite;
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    createdFiles.add(path);
  }

  @Benchmark
  @Threads(1)
  public long sequentialRead() {
    long totalBytes = 0;
    InputFile inputFile = fileIO.newInputFile(readFilePath);
    try (SeekableInputStream in = inputFile.newStream()) {
      int bytesRead;
      while ((bytesRead = in.read(readBuffer)) != -1) {
        totalBytes += bytesRead;
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    return totalBytes;
  }

  @Benchmark
  @Threads(1)
  public long randomRead() {
    long totalBytes = 0;
    long fileSize = fileSizeKB * 1024L;
    int bufSize = bufferSizeKB * 1024;
    int numReads = (int) Math.min(fileSize / Math.max(bufSize, 1), 100);
    if (numReads <= 0) {
      numReads = 1;
    }

    InputFile inputFile = fileIO.newInputFile(readFilePath);
    try (SeekableInputStream in = inputFile.newStream()) {
      for (int i = 0; i < numReads; i++) {
        long maxOffset = Math.max(fileSize - bufSize, 0);
        long offset = maxOffset > 0 ? (long) (random.nextDouble() * maxOffset) : 0;
        in.seek(offset);
        int totalRead = 0;
        while (totalRead < bufSize) {
          int read = in.read(readBuffer, totalRead, bufSize - totalRead);
          if (read == -1) {
            break;
          }
          totalRead += read;
        }
        totalBytes += totalRead;
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    return totalBytes;
  }

  @Benchmark
  @Threads(1)
  public long rangeRead() {
    long totalBytes = 0;
    long fileSize = fileSizeKB * 1024L;
    int bufSize = bufferSizeKB * 1024;
    int numReads = (int) Math.min(fileSize / Math.max(bufSize, 1), 100);
    if (numReads <= 0) {
      numReads = 1;
    }

    InputFile inputFile = fileIO.newInputFile(readFilePath);
    try (SeekableInputStream in = inputFile.newStream()) {
      if (!(in instanceof RangeReadable)) {
        throw new UnsupportedOperationException(
            "rangeRead benchmark requires a RangeReadable stream, got: " + in.getClass().getName());
      }
      RangeReadable rangeReadable = (RangeReadable) in;
      for (int i = 0; i < numReads; i++) {
        long maxOffset = Math.max(fileSize - bufSize, 0);
        long offset = maxOffset > 0 ? (long) (random.nextDouble() * maxOffset) : 0;
        rangeReadable.readFully(offset, readBuffer, 0, bufSize);
        totalBytes += bufSize;
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    return totalBytes;
  }

  private void writeTestFile(String path, long size) {
    OutputFile outputFile = fileIO.newOutputFile(path);
    try (PositionOutputStream out = outputFile.create()) {
      long remaining = size;
      while (remaining > 0) {
        int toWrite = (int) Math.min(writeBuffer.length, remaining);
        out.write(writeBuffer, 0, toWrite);
        remaining -= toWrite;
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private Map<String, String> loadProperties() {
    Map<String, String> result = Maps.newHashMap();

    // 1. Try loading from properties file
    Properties fileProps = new Properties();
    // try classpath first
    try (InputStream is =
        FileIOBenchmark.class.getClassLoader().getResourceAsStream(PROPERTIES_FILE)) {
      if (is != null) {
        fileProps.load(is);
      }
    } catch (IOException e) {
      // ignore
    }
    // try current directory if not found on classpath
    if (fileProps.isEmpty() && Files.exists(Paths.get(PROPERTIES_FILE))) {
      try (FileInputStream fis = new FileInputStream(PROPERTIES_FILE)) {
        fileProps.load(fis);
      } catch (IOException e) {
        // ignore
      }
    }
    for (String key : fileProps.stringPropertyNames()) {
      result.put(key, fileProps.getProperty(key));
    }

    // 2. Overlay all non-JVM-internal system properties (forwarded by jmh.gradle)
    for (Map.Entry<Object, Object> entry : System.getProperties().entrySet()) {
      String key = entry.getKey().toString();
      boolean isJvmInternal = false;
      for (String prefix : JVM_PROPERTY_PREFIXES) {
        if (key.startsWith(prefix)) {
          isJvmInternal = true;
          break;
        }
      }
      if (!isJvmInternal) {
        result.put(key, entry.getValue().toString());
      }
    }

    return result;
  }
}
