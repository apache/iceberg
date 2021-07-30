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

import java.io.IOException;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.util.CharSequenceSet;

public interface DeltaTaskWriter<T> extends V2TaskWriter<T> {

  // equality delete
  void delete(T row, PartitionSpec spec, StructLike partition) throws IOException;

  // position delete with persisting row
  void delete(CharSequence path, long pos, T row, PartitionSpec spec, StructLike partition) throws IOException;

  // position delete without persisting row
  default void delete(CharSequence path, long pos, PartitionSpec spec, StructLike partition) throws IOException {
    delete(path, pos, null, spec, partition);
  }

  @Override
  Result result();

  interface Result extends V2TaskWriter.Result {
    DeleteFile[] deleteFiles();
    CharSequenceSet referencedDataFiles();
  }
}
