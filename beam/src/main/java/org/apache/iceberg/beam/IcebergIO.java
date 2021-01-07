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


public class IcebergIO {

    private IcebergIO() {
    }

    public static PCollection<Snapshot> write(TableIdentifier table,
                                       Schema schema,
                                       String hiveMetastoreUrl,
                                       WriteFilesResult<Void> resultFiles) {
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
        final IcebergDataFileCommitter combiner = new IcebergDataFileCommitter(table, schema, hiveMetastoreUrl);
        final Combine.Globally<WrittenDataFile, Snapshot> combined = Combine.globally(combiner).withoutDefaults();

        // We return the latest snapshot, which can be used to notify downstream consumers.
        return writtenDataFiles.apply(combined);
    }
}
