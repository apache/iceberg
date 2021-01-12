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

import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.WriteFilesResult;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.values.PCollection;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;

import java.util.Map;


public class IcebergIO {

    public static final class Builder {
        private TableIdentifier tableIdentifier;
        private WriteFilesResult<Void> resultFiles;
        private Schema schema;
        private String hiveMetaStoreUrl;

        private final Map<String, String> hadoopConfig = Maps.newHashMap();

        public Builder withTableIdentifier(TableIdentifier tableIdentifier) {
            this.tableIdentifier = tableIdentifier;
            return this;
        }

        public Builder withResultFiles(WriteFilesResult<Void> resultFiles) {
            this.resultFiles = resultFiles;
            return this;
        }

        public Builder withSchema(Schema schema) {
            this.schema = schema;
            return this;
        }

        public Builder withHiveMetastoreUrl(String hiveMetaStoreUrl) {
            assert hiveMetaStoreUrl.startsWith("thrift://");
            this.hiveMetaStoreUrl = hiveMetaStoreUrl;
            return this;
        }

        public Builder conf(String key, String value) {
            this.hadoopConfig.put(key, value);
            return this;
        }

        public PCollection<Snapshot> build() {
            return IcebergIO.write(
                    this.tableIdentifier,
                    this.resultFiles,
                    this.schema,
                    this.hiveMetaStoreUrl,
                    this.hadoopConfig
            );
        }
    }

    public static PCollection<Snapshot> write(
            TableIdentifier tableIdentifier,
            WriteFilesResult<Void> resultFiles,
            Schema schema,
            String hiveMetaStoreUrl,
            Map<String, String> hadoopConfig
    ) {
        // We take the filenames that are emitted by the FileIO
        final PCollection<String> filenames = resultFiles
                .getPerDestinationOutputFilenames()
                .apply(Values.create())
                .setCoder(StringUtf8Coder.of());

        // We compute the required set of statistics, such as the number
        // of rows and the filesize.
        // Probably we want to improve on this later, since it would be nicer
        // to compute this as we write the files
        final PCollection<WrittenDataFile> writtenDataFiles = filenames
                .apply(ParDo.of(new FilenameToDataFile()))
                .setCoder(SerializableCoder.of(WrittenDataFile.class));

        // We use a combiner, to combine all the files to a single commit in
        // the Iceberg log
        final IcebergDataFileCommitter combiner = new IcebergDataFileCommitter(tableIdentifier, schema, hiveMetaStoreUrl, hadoopConfig);
        final Combine.Globally<WrittenDataFile, Snapshot> combined = Combine.globally(combiner).withoutDefaults();

        // We return the latest snapshot, which can be used to notify downstream consumers.
        return writtenDataFiles.apply(combined);
    }
}
