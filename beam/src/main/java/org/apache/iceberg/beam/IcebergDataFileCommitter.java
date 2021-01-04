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
import java.io.InputStream;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.MatchResult;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.values.KV;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.hive.HiveCatalog;
import org.checkerframework.checker.initialization.qual.Initialized;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.UnknownKeyFor;

class IcebergDataFileCommitter extends Combine.CombineFn<WrittenDataFile, List<WrittenDataFile>, Snapshot> {
    private final TableIdentifier tableIdentifier;
    private final Schema schema;
    private final String hiveMetastoreUrl;

    IcebergDataFileCommitter(TableIdentifier table, Schema schema, String hiveMetastoreUrl) {
        this.tableIdentifier = table;
        this.schema = schema;
        this.hiveMetastoreUrl = hiveMetastoreUrl;
    }

    @Override
    public List<WrittenDataFile> createAccumulator() {
        return new ArrayList<>();
    }

    @Override
    public List<WrittenDataFile> addInput(List<WrittenDataFile> mutableAccumulator, WrittenDataFile input) {
        return null;
    }

    @Override
    public List<WrittenDataFile> mergeAccumulators(Iterable<List<WrittenDataFile>> accumulators) {
        Iterator<List<WrittenDataFile>> itr = accumulators.iterator();

        if (itr.hasNext()) {
            List<WrittenDataFile> first = itr.next();

            while (itr.hasNext()) {
                first.addAll(itr.next());
            }

            return first;
        } else {
            return new ArrayList<>();
        }
    }

    @Override
    public Snapshot extractOutput(List<WrittenDataFile> accumulator) {
        try (HiveCatalog catalog = new HiveCatalog(
                HiveCatalog.DEFAULT_NAME,
                this.hiveMetastoreUrl,
                1,
                new Configuration()
        )) {
            Table table;
            try {
                table = catalog.loadTable(this.tableIdentifier);
            } catch (NoSuchTableException e) {
                // If it doesn't exist, we just create the table
                table = catalog.createTable(this.tableIdentifier, schema);
            }

            // Append the new files
            final AppendFiles app = table.newAppend();
            // We need to get the statistics, not easy to get them through Beam
            for (WrittenDataFile dataFile : accumulator) {
                app.appendFile(DataFiles.builder(table.spec())
                        .withPath(dataFile.getFilename())
                        .withFileSizeInBytes(dataFile.getBytes())
                        .withRecordCount(dataFile.getRecords())
                        .build());
            }
            app.commit();

            return table.currentSnapshot();
        }
    }
}
