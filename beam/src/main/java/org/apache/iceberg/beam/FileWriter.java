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

import java.io.IOException;
import java.util.Locale;
import java.util.Map;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.beam.IcebergIO.Write;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.io.PartitionedWriter;
import org.apache.iceberg.io.TaskWriter;
import org.apache.iceberg.io.UnpartitionedWriter;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.iceberg.beam.CatalogHelper.DEFAULT_NAME;

public class FileWriter<T extends GenericRecord> extends DoFn<T, DataFile> {
  private static final Logger LOG = LoggerFactory.getLogger(FileWriter.class);

  private final TableIdentifier tableIdentifier;

  private Map<String, String> properties;
  private Schema schema;
  private FileIO io;
  private EncryptionManager encryptionManager;
  private Catalog catalog;
  private FileFormat fileFormat;
  private PartitionSpec spec;

  private transient TaskWriter<T> writer;
  private transient BoundedWindow lastSeenWindow;
  private Table table;

  public FileWriter(Write writeSpec) {
    this.tableIdentifier = writeSpec.getTableIdentifier();
    this.spec = PartitionSpec.unpartitioned();
    this.schema = writeSpec.getSchema();
    this.properties = writeSpec.getConfigProperties();
  }

  @Setup
  public void setup(){
    this.catalog = CatalogUtil.buildIcebergCatalog(DEFAULT_NAME, this.properties, null);
    table = CatalogHelper.loadOrCreateTable(
            catalog,
            tableIdentifier,
            schema,
            spec,
            properties
    );
    this.schema = table.schema();
    this.properties = table.properties();
    this.io = table.io();
    this.spec = table.spec();
    this.encryptionManager = table.encryption();
    String formatString = table.properties().getOrDefault(
            TableProperties.DEFAULT_FILE_FORMAT, TableProperties.DEFAULT_FILE_FORMAT_DEFAULT);
    this.fileFormat = FileFormat.valueOf(formatString.toUpperCase(Locale.ENGLISH));
  }

  @ProcessElement
  public void processElement(ProcessContext context, BoundedWindow window) {
    appendRecord(
        context.element(),
        window,
        (int) context.pane().getIndex(),
        context.pane().getIndex()
    );
  }

  @FinishBundle
  public void finishBundle(FinishBundleContext fbc) {
    DataFile[] files = finish();
    for (DataFile file : files) {
      fbc.output(file, Instant.now(), lastSeenWindow);
    }
  }

  public void appendRecord(T element, BoundedWindow window, int partitionId, long taskId) {
    if (writer == null) {
      LOG.info("Setting up the writer");

      // We would rather do this in the startBundle, but we don't know the pane
      BeamAppenderFactory<T> appenderFactory = new BeamAppenderFactory<>(table, spec);
      OutputFileFactory fileFactory = OutputFileFactory.builderFor(table, partitionId, taskId)
              .format(fileFormat).defaultSpec(spec).build();

      if (spec.isUnpartitioned()) {
        writer = new UnpartitionedWriter<>(spec, fileFormat, appenderFactory, fileFactory, io, Long.MAX_VALUE);
      } else {
        writer = new PartitionedWriter<T>(
            spec, fileFormat, appenderFactory, fileFactory, io, Long.MAX_VALUE) {
          @Override
          protected PartitionKey partition(T row) {
            return new PartitionKey(spec, schema);
          }
        };
      }
    }
    try {
      lastSeenWindow = window;
      writer.write(element);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public DataFile[] finish() {
    LOG.info("Closing the writer");
    try {
      writer.close();
      return writer.dataFiles();
    } catch (IOException e) {
      throw new RuntimeException(e);
    } finally {
      writer = null;
      catalog = null;
    }
  }
}