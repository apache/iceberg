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
package org.apache.iceberg.io.canonicaluri;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.function.Function;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.hadoop.HadoopConfigurable;
import org.apache.iceberg.hadoop.SerializableConfiguration;
import org.apache.iceberg.io.BulkDeletionFailureException;
import org.apache.iceberg.io.DelegateFileIO;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.FileInfo;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.util.SerializableMap;
import org.apache.iceberg.util.SerializableSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A FileIO that preserves canonical Iceberg locations while accessing translated storage URIs.
 *
 * <p>Rules use deterministic longest-prefix matching. Translation is syntactic and does not issue
 * storage or catalog requests.
 */
public final class CanonicalUriFileIO implements DelegateFileIO, HadoopConfigurable {
  private static final Logger LOG = LoggerFactory.getLogger(CanonicalUriFileIO.class);

  private SerializableMap<String, String> properties =
      SerializableMap.copyOf(Collections.emptyMap());
  private SerializableSupplier<Configuration> hadoopConf;
  private PathAliasConfig config;
  private PathAliasMetrics metrics = new PathAliasMetrics(false);
  private transient volatile DelegateFileIO delegate;
  private volatile boolean closed;

  /** Constructor used for dynamic FileIO loading. */
  public CanonicalUriFileIO() {}

  @Override
  public InputFile newInputFile(String location) {
    metrics.read();
    MappingResult mapped = translate(location, false, "read");
    InputFile physical = invoke(() -> delegate().newInputFile(mapped.location()));
    return CanonicalInputFile.wrap(location, physical, metrics);
  }

  @Override
  public InputFile newInputFile(String location, long length) {
    metrics.read();
    MappingResult mapped = translate(location, false, "read");
    InputFile physical = invoke(() -> delegate().newInputFile(mapped.location(), length));
    return CanonicalInputFile.wrap(location, physical, metrics);
  }

  @Override
  public OutputFile newOutputFile(String location) {
    metrics.write();
    MappingResult mapped = translate(location, true, "write");
    OutputFile physical = invoke(() -> delegate().newOutputFile(mapped.location()));
    return CanonicalOutputFile.wrap(location, physical, metrics);
  }

  @Override
  public void deleteFile(String location) {
    metrics.delete();
    MappingResult mapped = translate(location, true, "delete");
    invokeVoid(() -> delegate().deleteFile(mapped.location()));
  }

  @Override
  public void deleteFiles(Iterable<String> pathsToDelete) throws BulkDeletionFailureException {
    metrics.bulkDelete();
    List<String> physicalPaths = new ArrayList<>();
    for (String path : pathsToDelete) {
      physicalPaths.add(translate(path, true, "bulk-delete").location());
    }

    invokeVoid(() -> delegate().deleteFiles(physicalPaths));
  }

  @Override
  public Iterable<FileInfo> listPrefix(String prefix) {
    metrics.listPrefix();
    MappingResult mappedPrefix = translate(prefix, false, "list-prefix");
    Iterable<FileInfo> physicalFiles = invoke(() -> delegate().listPrefix(mappedPrefix.location()));
    return () -> new CanonicalFileInfoIterator(physicalFiles.iterator(), mappedPrefix.rule());
  }

  @Override
  public void deletePrefix(String prefix) {
    metrics.deletePrefix();
    MappingResult mapped = translate(prefix, true, "delete-prefix");
    invokeVoid(() -> delegate().deletePrefix(mapped.location()));
  }

  @Override
  public Map<String, String> properties() {
    return properties.immutableMap();
  }

  @Override
  public synchronized void initialize(Map<String, String> newProperties) {
    if (config != null) {
      throw new IllegalStateException("CanonicalUriFileIO is already initialized");
    }

    PathAliasConfig newConfig = PathAliasConfig.from(newProperties);
    if (getClass().getName().equals(newConfig.delegateImpl())) {
      throw new IllegalArgumentException(
          "Invalid pathalias.delegate.impl: CanonicalUriFileIO cannot delegate to itself");
    }

    closeDelegate();
    this.properties = SerializableMap.copyOf(newProperties);
    this.config = newConfig;
    this.metrics = new PathAliasMetrics(newConfig.metricsEnabled());
    this.closed = false;

    LOG.info(
        "Initialized CanonicalUriFileIO with {} rules, delegate {}, enabled {}",
        newConfig.mapper().ruleCount(),
        newConfig.delegateImpl(),
        newConfig.enabled());
  }

  /**
   * Returns counters for translation and delegated operations.
   *
   * @return this instance's operation counters
   */
  public PathAliasMetrics metrics() {
    return metrics;
  }

  @Override
  public synchronized void close() {
    closeDelegate();
    this.closed = true;
  }

  @Override
  public synchronized void setConf(Configuration conf) {
    this.hadoopConf = conf == null ? null : new SerializableConfiguration(conf)::get;
    if (delegate != null) {
      CatalogUtil.configureHadoopConf(delegate, conf);
    }
  }

  @Override
  public Configuration getConf() {
    return hadoopConf == null ? null : hadoopConf.get();
  }

  @Override
  public void serializeConfWith(
      Function<Configuration, SerializableSupplier<Configuration>> confSerializer) {
    this.hadoopConf = confSerializer.apply(getConf());
  }

  private MappingResult translate(String location, boolean rejectPhysicalAlias, String operation) {
    ensureOpenAndInitialized();
    long start = System.nanoTime();
    try {
      MappingResult result = config.mapper().toPhysical(location, rejectPhysicalAlias);
      metrics.translated(result.mapped(), System.nanoTime() - start);
      if (LOG.isDebugEnabled() && result.mapped()) {
        if (config.debugLogPhysicalPaths()) {
          LOG.debug(
              "Mapped canonical location for {} using rule {}: canonical={}, physical={}",
              operation,
              result.rule().id(),
              location,
              result.location());
        } else {
          LOG.debug(
              "Mapped canonical location for {} using rule {}", operation, result.rule().id());
        }
      }

      return result;
    } catch (RuntimeException e) {
      metrics.translationFailed(System.nanoTime() - start);
      throw e;
    }
  }

  private String toCanonical(String location, MappingRule expectedRule) {
    long start = System.nanoTime();
    try {
      MappingResult result = config.mapper().toCanonical(location);
      metrics.translated(result.mapped(), System.nanoTime() - start);
      if (expectedRule != null && result.rule() != expectedRule) {
        throw new IllegalStateException(
            "Delegate returned a location outside the mapped physical prefix");
      }

      return result.location();
    } catch (RuntimeException e) {
      metrics.translationFailed(System.nanoTime() - start);
      throw e;
    }
  }

  private DelegateFileIO delegate() {
    ensureOpenAndInitialized();
    DelegateFileIO current = delegate;
    if (current == null) {
      synchronized (this) {
        current = delegate;
        if (current == null) {
          FileIO loaded = CatalogUtil.loadFileIO(config.delegateImpl(), properties, getConf());
          if (!(loaded instanceof DelegateFileIO)) {
            loaded.close();
            throw new IllegalArgumentException(
                "Configured FileIO does not implement DelegateFileIO: "
                    + loaded.getClass().getName());
          }

          current = (DelegateFileIO) loaded;
          delegate = current;
        }
      }
    }

    return current;
  }

  private void ensureOpenAndInitialized() {
    if (closed) {
      throw new IllegalStateException("CanonicalUriFileIO is closed");
    }

    if (config == null) {
      throw new IllegalStateException("CanonicalUriFileIO is not initialized");
    }
  }

  private void closeDelegate() {
    DelegateFileIO current = delegate;
    delegate = null;
    if (current != null) {
      current.close();
    }
  }

  private <T> T invoke(Call<T> call) {
    try {
      return call.invoke();
    } catch (RuntimeException e) {
      metrics.delegateFailed();
      throw e;
    }
  }

  private void invokeVoid(VoidCall call) {
    try {
      call.invoke();
    } catch (RuntimeException e) {
      metrics.delegateFailed();
      throw e;
    }
  }

  private interface Call<T> {
    T invoke();
  }

  private interface VoidCall {
    void invoke();
  }

  private class CanonicalFileInfoIterator implements Iterator<FileInfo> {
    private final Iterator<FileInfo> delegateIterator;
    private final MappingRule expectedRule;

    private CanonicalFileInfoIterator(
        Iterator<FileInfo> delegateIterator, MappingRule expectedRule) {
      this.delegateIterator = delegateIterator;
      this.expectedRule = expectedRule;
    }

    @Override
    public boolean hasNext() {
      return invoke(delegateIterator::hasNext);
    }

    @Override
    public FileInfo next() {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }

      FileInfo physical = invoke(delegateIterator::next);
      String canonical =
          expectedRule == null
              ? physical.location()
              : toCanonical(physical.location(), expectedRule);
      return new FileInfo(canonical, physical.size(), physical.createdAtMillis());
    }
  }
}
