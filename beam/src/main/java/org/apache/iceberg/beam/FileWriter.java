/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.iceberg.beam;

import java.io.IOException;
import java.util.Map;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.hive.HiveCatalog;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.LocationProvider;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.io.PartitionedWriter;
import org.apache.iceberg.io.TaskWriter;
import org.apache.iceberg.io.UnpartitionedWriter;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileWriter extends DoFn<GenericRecord, DataFile> {
    private static final Logger LOG = LoggerFactory.getLogger(FileWriter.class);

    private final PartitionSpec spec;
    private final TableIdentifier tableIdentifier;
    private final String hiveMetastoreUrl;

    private Schema schema;
    private Map<String, String> properties;
    private FileFormat format = FileFormat.AVRO;
    private LocationProvider locations;
    private FileIO io;
    private EncryptionManager encryptionManager;
    private HiveCatalog catalog;

    private transient TaskWriter<GenericRecord> writer;
    private transient BoundedWindow lastSeenWindow;

    public FileWriter(TableIdentifier tableIdentifier, Schema schema, PartitionSpec spec, String hiveMetastoreUrl) {
        this.tableIdentifier = tableIdentifier;
        this.spec = spec;
        this.hiveMetastoreUrl = hiveMetastoreUrl;
        this.schema = schema;
    }

    @StartBundle
    public void startBundle(StartBundleContext sbc) {
        catalog = new HiveCatalog(
                HiveCatalog.DEFAULT_NAME,
                this.hiveMetastoreUrl,
                1,
                new Configuration()
        );
        Table table = HiveCatalogHelper.loadOrCreateTable(catalog, tableIdentifier, schema);
        this.schema = table.schema();
        this.locations = table.locationProvider();
        this.properties = table.properties();
        this.io = table.io();
        this.encryptionManager = table.encryption();
//        String formatString = table.properties().getOrDefault(
//                TableProperties.DEFAULT_FILE_FORMAT, TableProperties.DEFAULT_FILE_FORMAT_DEFAULT);
//        this.format = FileFormat.valueOf(formatString.toUpperCase(Locale.ENGLISH));
    }

    @ProcessElement
    public void processElement(ProcessContext c, BoundedWindow window) {
        if (writer == null) {
            LOG.info("Setting up the writer");
            // We would rather do this in the startBundle, but we don't know the pane
            int partitionId = (int) c.pane().getIndex();
            long taskId = c.pane().getIndex();

            BeamAppenderFactory appenderFactory = new BeamAppenderFactory(schema, properties, spec);
            OutputFileFactory fileFactory = new OutputFileFactory(
                    spec, format, locations, io, encryptionManager, partitionId, taskId);

            if (spec.isUnpartitioned()) {
                writer = new UnpartitionedWriter<>(spec, format, appenderFactory, fileFactory, io, Long.MAX_VALUE);
            } else {
                writer = new PartitionedWriter<GenericRecord>(
                        spec, format, appenderFactory, fileFactory, io, Long.MAX_VALUE) {
                    @Override
                    protected PartitionKey partition(GenericRecord row) {
                        return new PartitionKey(spec, schema);
                    }
                };
            }
        }
        try {
            lastSeenWindow = window;
            writer.write(c.element());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @FinishBundle
    public void finishBundle(FinishBundleContext fbc) {
        LOG.info("Closing the writer");
        try {
            writer.close();
            final Instant now = Instant.now();
            for (DataFile f : writer.dataFiles()) {
                fbc.output(f, now, lastSeenWindow);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            writer = null;
        }

        catalog.close();
        catalog = null;
    }
}
