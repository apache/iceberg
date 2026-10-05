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
package org.apache.iceberg.flink.sink;

import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.LocationProvider;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;

/**
 * A Hadoop catalog over an existing warehouse whose tables pass every metadata publication through
 * a hook, so tests can interleave other commits with it or fail it.
 */
public class InterceptingCatalog extends HadoopCatalog {
  /** Publishes {@code metadata} through {@code delegate}, or doesn't. */
  public interface PublishHook {
    void publish(TableOperations delegate, TableMetadata base, TableMetadata metadata);
  }

  private final AtomicInteger publications = new AtomicInteger();
  private volatile PublishHook hook = TableOperations::commit;

  public InterceptingCatalog(String warehouse) {
    setConf(new Configuration());
    initialize("intercepting", ImmutableMap.of(CatalogProperties.WAREHOUSE_LOCATION, warehouse));
  }

  public void onPublish(PublishHook newHook) {
    this.hook = newHook;
  }

  /** The number of publications attempted through this catalog's tables. */
  public int publications() {
    return publications.get();
  }

  @Override
  public Table loadTable(TableIdentifier identifier) {
    Table table = super.loadTable(identifier);
    return new BaseTable(
        new InterceptingOperations(((HasTableOperations) table).operations()), table.name());
  }

  private class InterceptingOperations implements TableOperations {
    private final TableOperations delegate;

    private InterceptingOperations(TableOperations delegate) {
      this.delegate = delegate;
    }

    @Override
    public TableMetadata current() {
      return delegate.current();
    }

    @Override
    public TableMetadata refresh() {
      return delegate.refresh();
    }

    @Override
    public void commit(TableMetadata base, TableMetadata metadata) {
      publications.incrementAndGet();
      hook.publish(delegate, base, metadata);
    }

    @Override
    public FileIO io() {
      return delegate.io();
    }

    @Override
    public EncryptionManager encryption() {
      return delegate.encryption();
    }

    @Override
    public String metadataFileLocation(String fileName) {
      return delegate.metadataFileLocation(fileName);
    }

    @Override
    public LocationProvider locationProvider() {
      return delegate.locationProvider();
    }

    @Override
    public long newSnapshotId() {
      return delegate.newSnapshotId();
    }

    @Override
    public boolean requireStrictCleanup() {
      return delegate.requireStrictCleanup();
    }
  }
}
