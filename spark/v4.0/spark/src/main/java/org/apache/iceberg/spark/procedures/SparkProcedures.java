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
package org.apache.iceberg.spark.procedures;

import java.util.Locale;
import java.util.Map;
import java.util.function.Supplier;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.catalog.procedures.UnboundProcedure;

public class SparkProcedures {

  private static final Map<String, Supplier<ProcedureBuilder>> BUILDERS = initProcedureBuilders();

  private SparkProcedures() {}

  public static ProcedureBuilder newBuilder(String name) {
    // procedure resolution is case insensitive to match the existing Spark behavior for functions
    Supplier<ProcedureBuilder> builderSupplier = BUILDERS.get(name.toLowerCase(Locale.ROOT));
    return builderSupplier != null ? builderSupplier.get() : null;
  }

  private static Map<String, Supplier<ProcedureBuilder>> initProcedureBuilders() {
    ImmutableMap.Builder<String, Supplier<ProcedureBuilder>> mapBuilder = ImmutableMap.builder();
    mapBuilder.put(RollbackToSnapshotProcedure.NAME, RollbackToSnapshotProcedure::builder);
    mapBuilder.put(RollbackToTimestampProcedure.NAME, RollbackToTimestampProcedure::builder);
    mapBuilder.put(SetCurrentSnapshotProcedure.NAME, SetCurrentSnapshotProcedure::builder);
    mapBuilder.put(CherrypickSnapshotProcedure.NAME, CherrypickSnapshotProcedure::builder);
    mapBuilder.put(RewriteDataFilesProcedure.NAME, RewriteDataFilesProcedure::builder);
    mapBuilder.put(RewriteManifestsProcedure.NAME, RewriteManifestsProcedure::builder);
    mapBuilder.put(RemoveOrphanFilesProcedure.NAME, RemoveOrphanFilesProcedure::builder);
    mapBuilder.put(ExpireSnapshotsProcedure.NAME, ExpireSnapshotsProcedure::builder);
    mapBuilder.put(MigrateTableProcedure.NAME, MigrateTableProcedure::builder);
    mapBuilder.put(SnapshotTableProcedure.NAME, SnapshotTableProcedure::builder);
    mapBuilder.put(AddFilesProcedure.NAME, AddFilesProcedure::builder);
    mapBuilder.put(AncestorsOfProcedure.NAME, AncestorsOfProcedure::builder);
    mapBuilder.put(RegisterTableProcedure.NAME, RegisterTableProcedure::builder);
    mapBuilder.put(PublishChangesProcedure.NAME, PublishChangesProcedure::builder);
    mapBuilder.put(CreateChangelogViewProcedure.NAME, CreateChangelogViewProcedure::builder);
    mapBuilder.put(
        RewritePositionDeleteFilesProcedure.NAME, RewritePositionDeleteFilesProcedure::builder);
    mapBuilder.put(FastForwardBranchProcedure.NAME, FastForwardBranchProcedure::builder);
    mapBuilder.put(ComputeTableStatsProcedure.NAME, ComputeTableStatsProcedure::builder);
    mapBuilder.put(ComputePartitionStatsProcedure.NAME, ComputePartitionStatsProcedure::builder);
    mapBuilder.put(RewriteTablePathProcedure.NAME, RewriteTablePathProcedure::builder);
    return mapBuilder.build();
  }

  public interface ProcedureBuilder {
    ProcedureBuilder withTableCatalog(TableCatalog tableCatalog);

    UnboundProcedure build();
  }
}
