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
package org.apache.iceberg.actions;

import java.util.List;
import org.apache.iceberg.DeleteFile;

/**
 * An action that removes dangling delete files from the current snapshot. A delete file is dangling
 * if its deletes no longer applies to any data file.
 *
 * <p>The following dangling delete files are removed:
 *
 * <ul>
 *   <li>Position delete files with a sequence number less than that of any data file in the same
 *       partition
 *   <li>Equality delete files with a sequence number less than or equal to that of any data file in
 *       the same partition
 * </ul>
 */
public interface RemoveDanglingDeleteFiles
    extends Action<RemoveDanglingDeleteFiles, RemoveDanglingDeleteFiles.Result> {

  /** The action result that contains a summary of the execution. */
  interface Result {
    /** Removes representation of removed delete files. */
    List<DeleteFile> removedDeleteFiles();
  }
}
