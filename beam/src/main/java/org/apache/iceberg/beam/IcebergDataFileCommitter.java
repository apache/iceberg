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

package org.apache.iceberg.beam;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;

import static org.apache.iceberg.beam.CatalogHelper.DEFAULT_NAME;

class IcebergDataFileCommitter extends Combine.CombineFn<DataFile, List<DataFile>, Snapshot> {

  private final TableIdentifier tableIdentifier;
  private final Map<String, String> properties;

  IcebergDataFileCommitter(IcebergIO.Write writeSpec) {
    this.tableIdentifier = writeSpec.getTableIdentifier();
    this.properties = writeSpec.getConfigProperties();
  }

  @Override
  public List<DataFile> createAccumulator() {
    return new ArrayList<>();
  }

  @Override
  public List<DataFile> addInput(List<DataFile> mutableAccumulator, DataFile input) {
    mutableAccumulator.add(input);
    return mutableAccumulator;
  }

  @Override
  public List<DataFile> mergeAccumulators(Iterable<List<DataFile>> accumulators) {
    Iterator<List<DataFile>> itr = accumulators.iterator();
    if (itr.hasNext()) {
      List<DataFile> first = itr.next();
      while (itr.hasNext()) {
        first.addAll(itr.next());
      }
      return first;
    } else {
      return new ArrayList<>();
    }
  }

  @Override
  public Snapshot extractOutput(List<DataFile> datafiles) {
    Catalog catalog = CatalogUtil.buildIcebergCatalog(DEFAULT_NAME, this.properties, null);
    Table table = catalog.loadTable(tableIdentifier);
    if (!datafiles.isEmpty()) {
      final AppendFiles app = table.newAppend();
      for (DataFile datafile : datafiles) {
        app.appendFile(datafile);
      }
      app.commit();
    }
    return table.currentSnapshot();
  }
}