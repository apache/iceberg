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

import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.catalog.ViewCatalog;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.base.Splitter;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;

public abstract class MetastoreViewCatalog implements ViewCatalog {

  private final Configuration conf;

  public MetastoreViewCatalog(Configuration conf) {
    this.conf = conf;
  }

  protected abstract MetastoreViewOperations newViewOps(TableIdentifier viewName);

  protected String defaultWarehouseLocation(TableIdentifier viewIdentifier) {
    throw new UnsupportedOperationException(
        "Implementation for 'defaultWarehouseLocation' not provided.");
  }

  @Override
  public View createView(
      TableIdentifier identifier,
      List<ViewRepresentation> representations,
      Map<String, String> properties) {
    ViewOperations ops = newViewOps(identifier);
    if (ops.current() != null) {
      throw new AlreadyExistsException("View already exists: %s", identifier);
    }

    String location = defaultWarehouseLocation(identifier);
    long parentId = -1;

    ViewUtil.doCommit(
        ViewDDLOperation.CREATE, 1, parentId, representations, properties, location, ops, null);
    return new BaseView(ops, identifier.toString());
  }

  @Override
  public View loadView(TableIdentifier identifier) {
    ViewOperations ops = newViewOps(identifier);
    if (ops.current() == null) {
      throw new NotFoundException("View does not exist: %s", identifier);
    }
    return new BaseView(ops, identifier.toString());
  }

  protected TableIdentifier toCatalogTableIdentifier(String tableIdentifier) {
    List<String> namespace = Lists.newArrayList();
    Iterable<String> parts = Splitter.on(".").split(tableIdentifier);

    String lastPart = "";
    for (String part : parts) {
      if (!lastPart.isEmpty()) {
        namespace.add(lastPart);
      }
      lastPart = part;
    }

    Preconditions.checkState(namespace.size() >= 2, "namespace should have catalog and schema");

    return TableIdentifier.of(Namespace.of(namespace.toArray(new String[0])), lastPart);
  }
}
