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
package org.apache.iceberg.flink.sink;

import java.io.Serializable;
import org.apache.flink.annotation.Experimental;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Table;
import org.apache.iceberg.io.OutputFileFactory;

/**
 * Plugin interface for providing a custom {@link OutputFileFactory}.
 *
 * <p>Implementations can customize how output files are created, enabling use cases such as:
 *
 * <ul>
 *   <li>Custom file naming or path rewriting
 *   <li>Routing writes to non-standard storage endpoints
 *   <li>Region-specific or environment-specific file placement
 * </ul>
 *
 * <p>When set on {@link IcebergSink.Builder#outputFileFactoryProvider}, the provided factory
 * replaces the default {@link OutputFileFactory} created by {@link RowDataTaskWriterFactory}.
 */
@Experimental
@FunctionalInterface
public interface OutputFileFactoryProvider extends Serializable {
  OutputFileFactory create(
      Table table, int taskId, int attemptId, FileFormat format, PartitionSpec spec);
}
