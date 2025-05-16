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
package org.apache.iceberg.data;

import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.deletes.EqualityDeleteWriter;
import org.apache.iceberg.deletes.PositionDeleteWriter;
import org.apache.iceberg.encryption.EncryptionKeyMetadata;
import org.apache.iceberg.io.DataWriter;

/**
 * Builder for generating one of the following:
 *
 * <ul>
 *   <li>{@link DataWriter}
 *   <li>{@link EqualityDeleteWriter}
 *   <li>{@link PositionDeleteWriter}
 * </ul>
 *
 * @param <B> type of the builder
 * @param <E> engine specific schema of the input records used for appender initialization
 */
interface FileWriteBuilderBase<B extends FileWriteBuilderBase<B, E>, E>
    extends WriteBuilderBase<B, E> {
  /** Sets the partition specification for the Iceberg metadata. */
  B withSpec(PartitionSpec newSpec);

  /** Sets the partition value for the Iceberg metadata. */
  B withPartition(StructLike newPartition);

  /** Sets the encryption key metadata for Iceberg metadata. */
  B withKeyMetadata(EncryptionKeyMetadata metadata);

  /** Sets the sort order for the Iceberg metadata. */
  B withSortOrder(SortOrder newSortOrder);
}
