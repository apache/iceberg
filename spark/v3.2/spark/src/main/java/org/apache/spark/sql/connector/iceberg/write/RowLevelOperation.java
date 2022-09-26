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
package org.apache.spark.sql.connector.iceberg.write;

import org.apache.spark.sql.connector.expressions.NamedReference;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.connector.write.WriteBuilder;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

/**
 * A logical representation of a data source DELETE, UPDATE, or MERGE operation that requires
 * rewriting data.
 */
public interface RowLevelOperation {

  /** The SQL operation being performed. */
  enum Command {
    DELETE,
    UPDATE,
    MERGE
  }

  /** Returns the description associated with this row-level operation. */
  default String description() {
    return this.getClass().toString();
  }

  /** Returns the actual SQL operation being performed. */
  Command command();

  /**
   * Returns a scan builder to configure a scan for this row-level operation.
   *
   * <p>Sources fall into two categories: those that can handle a delta of rows and those that need
   * to replace groups (e.g. partitions, files). Sources that handle deltas allow Spark to quickly
   * discard unchanged rows and have no requirements for input scans. Sources that replace groups of
   * rows can discard deleted rows but need to keep unchanged rows to be passed back into the
   * source. This means that scans for such data sources must produce all rows in a group if any are
   * returned. Some sources will avoid pushing filters into files (file granularity), while others
   * will avoid pruning files within a partition (partition granularity).
   *
   * <p>For example, if a source can only replace partitions, all rows from a partition must be
   * returned by the scan, even if a filter can narrow the set of changes to a single file in the
   * partition. Similarly, a source that can swap individual files must produce all rows of files
   * where at least one record must be changed, not just the rows that must be changed.
   */
  ScanBuilder newScanBuilder(CaseInsensitiveStringMap options);

  /**
   * Returns a write builder to configure a write for this row-level operation.
   *
   * <p>Note that Spark will first configure the scan and then the write, allowing data sources to
   * pass information from the scan to the write. For example, the scan can report which condition
   * was used to read the data that may be needed by the write under certain isolation levels.
   */
  WriteBuilder newWriteBuilder(ExtendedLogicalWriteInfo info);

  /**
   * Returns metadata attributes that are required to perform this row-level operation.
   *
   * <p>Data sources that can use this method to project metadata columns needed for writing the
   * data back (e.g. metadata columns for grouping data).
   */
  default NamedReference[] requiredMetadataAttributes() {
    return new NamedReference[0];
  }
}
