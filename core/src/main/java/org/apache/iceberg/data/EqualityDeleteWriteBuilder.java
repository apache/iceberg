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

import java.io.IOException;
import java.util.List;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.Schema;
import org.apache.iceberg.deletes.EqualityDeleteWriter;
import org.apache.iceberg.util.ArrayUtil;

/**
 * A specialized builder for creating equality-based delete file writers.
 *
 * <p>This builder extends the generic {@link ContentFileWriteBuilder} interface with functionality
 * specific to creating {@link EqualityDeleteWriter} instances.
 *
 * <p>The builder provides methods to configure which fields should be used for equality comparison
 * through {@link #equalityFieldIds(List)} or {@link #equalityFieldIds(int...)}, along with schema
 * configuration for the delete records.
 *
 * @param <D> the type of data records the writer will accept
 */
public interface EqualityDeleteWriteBuilder<D>
    extends ContentFileWriteBuilder<EqualityDeleteWriteBuilder<D>> {
  /** Sets the row schema for the delete writers. */
  EqualityDeleteWriteBuilder<D> rowSchema(Schema rowSchema);

  /** Sets the equality field ids for the equality delete writer. */
  default EqualityDeleteWriteBuilder<D> equalityFieldIds(List<Integer> fieldIds) {
    return equalityFieldIds(ArrayUtil.toIntArray(fieldIds));
  }

  /** Sets the equality field ids for the equality delete writer. */
  EqualityDeleteWriteBuilder<D> equalityFieldIds(int... fieldIds);

  /**
   * Creates an equality-based delete file writer configured with the current builder settings.
   *
   * <p>The returned {@link EqualityDeleteWriter} produces files that identify records to be deleted
   * based on field equality, generating proper {@link DeleteFile} metadata on completion.
   *
   * <p>The writer accepts input records exactly matching the input schema specified via {@link
   * #rowSchema(Schema)} for deletion.
   *
   * @return a fully configured {@link EqualityDeleteWriter} instance
   * @throws IOException if the writer cannot be created due to I/O errors
   */
  EqualityDeleteWriter<D> build() throws IOException;
}
