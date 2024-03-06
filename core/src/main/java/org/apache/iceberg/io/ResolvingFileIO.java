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

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.hadoop.HadoopConfigurable;
import org.apache.iceberg.hadoop.SerializableConfiguration;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.base.Joiner;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Iterators;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.util.SerializableMap;
import org.apache.iceberg.util.SerializableSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * FileIO implementation that uses location scheme to choose the correct FileIO implementation.
 * Delegate FileIO implementations must implement the {@link DelegateFileIO} mixin interface,
 * otherwise initialization will fail.
 */
public class ResolvingFileIO implements HadoopConfigurable, DelegateFileIO {
  private static final Logger LOG = LoggerFactory.getLogger(ResolvingFileIO.class);
  private static final int BATCH_SIZE = 100_000;
  private static final String FALLBACK_IMPL = "org.apache.iceberg.hadoop.HadoopFileIO";
  private static final String S3_FILE_IO_IMPL = "org.apache.iceberg.aws.s3.S3FileIO";
  private static final String GCS_FILE_IO_IMPL = "org.apache.iceberg.gcp.gcs.GCSFileIO";
  private static final String ADLS_FILE_IO_IMPL = "org.apache.iceberg.azure.adlsv2.ADLSFileIO";
  private static final Map<String, String> DEFAULT_SCHEME_TO_FILE_IO =
      ImmutableMap.of(
          "s3", S3_FILE_IO_IMPL,
          "s3a", S3_FILE_IO_IMPL,
          "s3n", S3_FILE_IO_IMPL,
          "gs", GCS_FILE_IO_IMPL,
          "abfs", ADLS_FILE_IO_IMPL,
          "abfss", ADLS_FILE_IO_IMPL);
  private static final String SCHEME_PROPERTY_PREFIX = "resolving-io.schemes.";

  private final Map<String, DelegateFileIO> ioInstances = Maps.newConcurrentMap();
  private final AtomicBoolean isClosed = new AtomicBoolean(false);
  private final transient StackTraceElement[] createStack;
  private SerializableMap<String, String> properties;
  private SerializableSupplier<Configuration> hadoopConf;
  private final Map<String, String> schemeToFileIO = Maps.newHashMap();

  /**
   * No-arg constructor to load the FileIO dynamically.
   *
   * <p>All fields are initialized by calling {@link ResolvingFileIO#initialize(Map)} later.
   */
  public ResolvingFileIO() {
    createStack = Thread.currentThread().getStackTrace();
  }

  @Override
  public InputFile newInputFile(String location) {
    return io(location).newInputFile(location);
  }

  @Override
  public InputFile newInputFile(String location, long length) {
    return io(location).newInputFile(location, length);
  }

  @Override
  public OutputFile newOutputFile(String location) {
    return io(location).newOutputFile(location);
  }

  @Override
  public void deleteFile(String location) {
    io(location).deleteFile(location);
  }

  @Override
  public void deleteFiles(Iterable<String> pathsToDelete) throws BulkDeletionFailureException {
    Iterators.partition(pathsToDelete.iterator(), BATCH_SIZE)
        .forEachRemaining(
            partitioned -> {
              Map<DelegateFileIO, List<String>> pathByFileIO =
                  partitioned.stream().collect(Collectors.groupingBy(this::io));
              for (Map.Entry<DelegateFileIO, List<String>> entries : pathByFileIO.entrySet()) {
                DelegateFileIO io = entries.getKey();
                List<String> filePaths = entries.getValue();
                io.deleteFiles(filePaths);
              }
            });
  }

  @Override
  public Map<String, String> properties() {
    return properties.immutableMap();
  }

  @Override
  public void initialize(Map<String, String> newProperties) {
    close(); // close and discard any existing FileIO instances
    this.schemeToFileIO.putAll(DEFAULT_SCHEME_TO_FILE_IO);
    this.properties = SerializableMap.copyOf(newProperties);
    isClosed.set(false);
    // load custom IO scheme handlers (starting with resolving-io.schemes. prefix)
    for (String key : this.properties.keySet()) {
      if (key.startsWith(SCHEME_PROPERTY_PREFIX)) {
        this.schemeToFileIO.put(
            key.substring(SCHEME_PROPERTY_PREFIX.length()), this.properties.get(key));
      }
    }
  }

  @Override
  public void close() {
    if (isClosed.compareAndSet(false, true)) {
      schemeToFileIO.clear();

      List<DelegateFileIO> instances = Lists.newArrayList();

      instances.addAll(ioInstances.values());
      ioInstances.clear();

      for (DelegateFileIO io : instances) {
        io.close();
      }
    }
  }

  @Override
  public void serializeConfWith(
      Function<Configuration, SerializableSupplier<Configuration>> confSerializer) {
    this.hadoopConf = confSerializer.apply(hadoopConf.get());
  }

  @Override
  public void setConf(Configuration conf) {
    this.hadoopConf = new SerializableConfiguration(conf)::get;
  }

  @Override
  public Configuration getConf() {
    return hadoopConf.get();
  }

  @VisibleForTesting
  DelegateFileIO io(String location) {
    String impl = implFromLocation(location);
    DelegateFileIO io = ioInstances.get(impl);
    if (io != null) {
      if (io instanceof HadoopConfigurable && ((HadoopConfigurable) io).getConf() == null) {
        synchronized (io) {
          if (((HadoopConfigurable) io).getConf() == null) {
            // re-apply the config in case it's null after Kryo serialization
            ((HadoopConfigurable) io).setConf(hadoopConf.get());
          }
        }
      }

      return io;
    }

    return ioInstances.computeIfAbsent(
        impl,
        key -> {
          Configuration conf = hadoopConf.get();
          FileIO fileIO;

          try {
            Map<String, String> props = Maps.newHashMap(properties);
            // ResolvingFileIO is keeping track of the creation stacktrace, so no need to do the
            // same in S3FileIO.
            props.put("init-creation-stacktrace", "false");
            fileIO = CatalogUtil.loadFileIO(key, props, conf);
          } catch (IllegalArgumentException e) {
            if (key.equals(FALLBACK_IMPL)) {
              // no implementation to fall back to, throw the exception
              throw e;
            } else {
              // couldn't load the normal class, fall back to HadoopFileIO
              LOG.warn(
                  "Failed to load FileIO implementation: {}, falling back to {}",
                  key,
                  FALLBACK_IMPL,
                  e);
              try {
                fileIO = CatalogUtil.loadFileIO(FALLBACK_IMPL, properties, conf);
              } catch (IllegalArgumentException suppressed) {
                LOG.warn(
                    "Failed to load FileIO implementation: {} (fallback)",
                    FALLBACK_IMPL,
                    suppressed);
                // both attempts failed, throw the original exception with the later exception
                // suppressed
                e.addSuppressed(suppressed);
                throw e;
              }
            }
          }

          Preconditions.checkState(
              fileIO instanceof DelegateFileIO,
              "FileIO does not implement DelegateFileIO: " + fileIO.getClass().getName());

          return (DelegateFileIO) fileIO;
        });
  }

  @VisibleForTesting
  String implFromLocation(String location) {
    return schemeToFileIO.getOrDefault(scheme(location), FALLBACK_IMPL);
  }

  public Class<?> ioClass(String location) {
    String fileIOClassName = implFromLocation(location);
    try {
      return Class.forName(fileIOClassName);
    } catch (ClassNotFoundException e) {
      throw new ValidationException("Class %s not found : %s", fileIOClassName, e.getMessage());
    }
  }

  private static String scheme(String location) {
    int colonPos = location.indexOf(":");
    if (colonPos > 0) {
      return location.substring(0, colonPos);
    }

    return null;
  }

  @SuppressWarnings("checkstyle:NoFinalizer")
  @Override
  protected void finalize() throws Throwable {
    super.finalize();
    if (!isClosed.get()) {
      close();

      if (null != createStack) {
        String trace =
            Joiner.on("\n\t").join(Arrays.copyOfRange(createStack, 1, createStack.length));
        LOG.warn("Unclosed ResolvingFileIO instance created by:\n\t{}", trace);
      }
    }
  }

  @Override
  public Iterable<FileInfo> listPrefix(String prefix) {
    return io(prefix).listPrefix(prefix);
  }

  @Override
  public void deletePrefix(String prefix) {
    io(prefix).deletePrefix(prefix);
  }
}
