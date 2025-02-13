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
package org.apache.iceberg.io.datafile;

import java.io.IOException;
import java.util.List;
import org.apache.iceberg.deletes.EqualityDeleteWriter;

/**
 * Builder API for creating {@link EqualityDeleteWriter}s.
 *
 * @param <T> the type of the builder for chaining
 */
public interface EqualityDeleteWriterBuilder<T extends EqualityDeleteWriterBuilder<T>>
    extends DeleteWriterBuilder<T> {
  /** Sets the equality field ids which are used in the delete file. */
  T equalityFieldIds(List<Integer> fieldIds);

  /** Sets the equality field ids which are used in the delete file. */
  T equalityFieldIds(int... fieldIds);

  <D> EqualityDeleteWriter<D> buildEqualityWriter() throws IOException;
}
