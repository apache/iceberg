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

import org.apache.iceberg.expressions.Expression;

/** An action for converting the equality delete files to position delete files. */
public interface ConvertEqualityDeleteFiles
    extends SnapshotUpdate<ConvertEqualityDeleteFiles, ConvertEqualityDeleteFiles.Result> {

  /**
   * A filter for finding the equality deletes to convert.
   *
   * <p>The filter will be converted to a partition filter with an inclusive projection. Any file
   * that may contain rows matching this filter will be used by the action. The matching delete
   * files will be converted to position delete files.
   *
   * @param expression An iceberg expression used to find deletes.
   * @return this for method chaining
   */
  ConvertEqualityDeleteFiles filter(Expression expression);

  /** The action result that contains a summary of the execution. */
  interface Result {
    /** Returns the count of the deletes that been converted. */
    int convertedEqualityDeleteFilesCount();

    /** Returns the count of the added position delete files. */
    int addedPositionDeleteFilesCount();
  }
}
