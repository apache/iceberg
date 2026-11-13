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
package org.apache.iceberg.spark.source;

import org.apache.iceberg.ScanTask;
import org.apache.iceberg.ScanTaskGroup;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.io.FileIO;
import org.apache.spark.sql.connector.read.InputPartition;

/**
 * The input partition of an Iceberg batch scan, as a {@link PartitionReaderFactoryProvider}'s
 * factory receives it: the task group to read, with each task's delete files already chosen by
 * Iceberg's planning, and what the rows must look like.
 */
public interface IcebergInputPartition extends InputPartition {

  /** The scan tasks of this partition. */
  <T extends ScanTask> ScanTaskGroup<T> taskGroup();

  /** Whether every task of this partition is of the given type. */
  <T extends ScanTask> boolean allTasksOfType(Class<T> javaClass);

  /** The table being read. */
  Table table();

  /** The table's file IO. */
  FileIO io();

  /** The columns to produce, in order. */
  Schema projection();

  /** Whether column names are matched case-sensitively. */
  boolean isCaseSensitive();
}
