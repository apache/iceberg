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
package org.apache.iceberg.io;

import java.io.Serializable;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.StructLike;

/**
 * Interface for providing data file locations to write tasks.
 *
 * <p>Implementations must be {@link Serializable} because instances will be serialized to tasks.
 */
public interface LocationProvider extends Serializable {
  /**
   * Return a fully-qualified data file location for the given filename.
   *
   * @param filename a file name
   * @return a fully-qualified location URI for a data file
   */
  String newDataLocation(String filename);

  /**
   * Return a fully-qualified data file location for the given partition and filename.
   *
   * @param spec a partition spec
   * @param partitionData a tuple of partition data for data in the file, matching the given spec
   * @param filename a file name
   * @return a fully-qualified location URI for a data file
   */
  String newDataLocation(PartitionSpec spec, StructLike partitionData, String filename);
}
