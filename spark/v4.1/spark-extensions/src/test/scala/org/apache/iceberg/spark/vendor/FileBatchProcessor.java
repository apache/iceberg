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
package org.apache.iceberg.spark.vendor;

import java.util.List;
import org.apache.iceberg.io.FileIO;

/**
 * Processes batches of file values on one Spark task.
 *
 * <p>Instantiated by class name on each task, so implementations must have a public no-arg
 * constructor, then initialized with the FileIO of the table the function was invoked on. Each
 * input array holds one element, the file value as a {@link org.apache.spark.sql.Row}. The result
 * must have one array per input array, in the same order, holding the output values in schema
 * order.
 */
public interface FileBatchProcessor extends AutoCloseable {

  default void initialize(FileIO io) {}

  List<Object[]> apply(List<Object[]> batch);

  @Override
  default void close() {}
}
