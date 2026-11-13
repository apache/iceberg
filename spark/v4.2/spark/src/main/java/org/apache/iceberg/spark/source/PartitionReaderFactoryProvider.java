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

import org.apache.iceberg.Table;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;

/**
 * Supplies the {@link PartitionReaderFactory} of Iceberg batch scans from outside Iceberg, for
 * example a native or accelerated reader.
 *
 * <p>Providers are discovered with {@link java.util.ServiceLoader} and used only when selected by
 * {@link #name()}: with the {@value
 * org.apache.iceberg.spark.SparkReadOptions#PARTITION_READER_FACTORY_PROVIDER} read option, or for
 * a session with {@value
 * org.apache.iceberg.spark.SparkSQLProperties#PARTITION_READER_FACTORY_PROVIDER}.
 *
 * <p>The provider is given the factory Iceberg would use. The factory it returns receives {@link
 * IcebergInputPartition}s; it may read any of them itself and pass the rest to the delegate. A
 * partition it reads must produce exactly the rows the delegate would: the live rows after the
 * tasks' deletes, with the partition's projection, in the same columnar or row form. Spark
 * serializes the factory to executors, so it must be serializable; read the table through {@link
 * IcebergInputPartition#table()} rather than capturing it.
 */
public interface PartitionReaderFactoryProvider {

  /** The name that selects this provider. */
  String name();

  /**
   * Returns the factory for a batch scan of the table.
   *
   * @param table the table being read
   * @param delegate the factory Iceberg would use
   * @return a factory, or the delegate to leave the scan to Iceberg
   */
  PartitionReaderFactory create(Table table, PartitionReaderFactory delegate);
}
