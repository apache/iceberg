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

package org.apache.iceberg.view;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.catalog.ViewCatalog;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.exceptions.RuntimeIOException;

/**
 * The Views implementation Based on FileIO.
 */
public class HadoopViewCatalog implements ViewCatalog, Configurable {
  private Configuration conf;

  public HadoopViewCatalog(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public List<TableIdentifier> listViews(Namespace namespace) {
    throw new UnsupportedOperationException("TODO");
  }

  @Override
  public View loadView(TableIdentifier identifier) {
    String location = identifier.name();
    ViewOperations ops = newViewOps(location);
    if (ops.current() == null) {
      throw new NoSuchTableException("View does not exist at location: %s", location);
    }
    return new BaseView(ops, location);
  }

  @Override
  public View createView(TableIdentifier identifier, List<ViewRepresentation> representations,
      Map<String, String> properties) {
    String location = identifier.name();
    ViewOperations ops = newViewOps(location);
    if (ops.current() != null) {
      throw new AlreadyExistsException("View already exists at location: %s", location);
    }

    int parentId = -1;

    ViewUtil.doCommit(ViewDDLOperation.CREATE, 1, parentId, representations, properties, location, ops, null);
    return new BaseView(ops, location);
  }

  @Override
  public boolean dropView(TableIdentifier identifier, boolean purge) {
    String location = identifier.name();
    Path path = new Path(location);
    try {
      FileSystem fs = path.getFileSystem(conf);
      return fs.delete(path, true);
    } catch (IOException e) {
      throw new RuntimeIOException("Failed to delete view metadata.");
    }
  }

  @Override
  public void renameView(TableIdentifier from, TableIdentifier to) {
    throw new UnsupportedOperationException("TODO");
  }

  private ViewOperations newViewOps(String location) {
    return new HadoopViewOperations(new Path(location), conf);
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public Configuration getConf() {
    return conf;
  }
}
