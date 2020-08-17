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

package org.apache.iceberg.flink;

import java.util.List;
import java.util.Map;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.TableSourceFactory;
import org.apache.flink.table.sources.TableSource;
import org.apache.flink.table.utils.TableSchemaUtils;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.source.FlinkTableSource;

/**
 * Flink Iceberg table factory to create table source and sink.
 * Only works for catalog, can not be loaded from Java SPI(Service Provider Interface).
 */
class FlinkTableFactory implements TableSourceFactory<RowData> {

  private final FlinkCatalog catalog;

  FlinkTableFactory(FlinkCatalog catalog) {
    this.catalog = catalog;
  }

  @Override
  public Map<String, String> requiredContext() {
    throw new UnsupportedOperationException("Iceberg Table Factory can not be loaded from Java SPI");
  }

  @Override
  public List<String> supportedProperties() {
    throw new UnsupportedOperationException("Iceberg Table Factory can not be loaded from Java SPI");
  }

  @Override
  public TableSource<RowData> createTableSource(Context context) {
    ObjectIdentifier identifier = context.getObjectIdentifier();
    ObjectPath objectPath = new ObjectPath(identifier.getDatabaseName(), identifier.getObjectName());
    TableIdentifier icebergIdentifier = catalog.toIdentifier(objectPath);
    try {
      Table table = catalog.getIcebergTable(objectPath);
      // Excludes computed columns
      TableSchema icebergSchema = TableSchemaUtils.getPhysicalSchema(context.getTable().getSchema());
      return new FlinkTableSource(
          icebergIdentifier, table, catalog.getCatalogLoader(), catalog.getHadoopConf(), icebergSchema,
          context.getTable().getOptions());
    } catch (TableNotExistException e) {
      throw new ValidationException(String.format("Iceberg Table(%s) not exist.", icebergIdentifier), e);
    }
  }
}
