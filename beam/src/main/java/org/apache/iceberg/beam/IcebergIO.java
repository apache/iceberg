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

import org.apache.beam.sdk.io.WriteFilesResult;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.values.PCollection;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.catalog.TableIdentifier;
import org.checkerframework.checker.initialization.qual.Initialized;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.UnknownKeyFor;


public class IcebergIO {

    private IcebergIO() {
    }

    static PCollection<Snapshot> write(TableIdentifier table,
                                       Schema schema,
                                       String hiveMetastoreUrl,
                                       WriteFilesResult<Void> resultFiles) {
        final PCollection<String> filenames = resultFiles.getPerDestinationOutputFilenames().apply(Values.create());

        final PCollection<WrittenDataFile> writtenDataFiles = filenames.apply(ParDo.of(new FilenameToDataFile()));

        final IcebergDataFileCommitter combiner = new IcebergDataFileCommitter(table, schema, hiveMetastoreUrl);
        final Combine.Globally<WrittenDataFile, Snapshot> combined = Combine.globally(combiner).withoutDefaults();

        return writtenDataFiles.apply(combined);
    }
}
