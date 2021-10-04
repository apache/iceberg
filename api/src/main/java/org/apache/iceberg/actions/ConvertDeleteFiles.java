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

/**
 * An action for converting the equality delete files according to a convert strategy.
 * Generally used for optimizing the sizing and layout of delete files within a table.
 */
public interface ConvertDeleteFiles extends SnapshotUpdate<ConvertDeleteFiles, ConvertDeleteFiles.Result> {

  /**
   * Convert the equality deletes to the position deletes.
   *
   * @return this for method chaining
   */
  ConvertDeleteFiles convertEqualityDeletes();

  /**
   * A filter for choosing the equality deletes to convert.
   *
   * @param expression An iceberg expression used to choose deletes.
   * @return this for method chaining
   */
  ConvertDeleteFiles filter(Expression expression);

  /**
   * The action result that contains a summary of the execution.
   */
  interface Result {
    /**
     * Returns the count of the deletes that been converted.
     */
    int convertedDeleteFilesCount();

    /**
     * Returns the count of the added position delete files.
     */
    int addedDeleteFilesCount();
  }
}
