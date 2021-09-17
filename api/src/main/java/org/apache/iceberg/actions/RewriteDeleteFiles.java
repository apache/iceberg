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

import java.util.Set;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.expressions.Expression;

/**
 * An action for rewriting delete files according to a rewrite strategy.
 * Generally used for optimizing the sizing and layout of delete files within a table.
 */
public interface RewriteDeleteFiles extends SnapshotUpdate<RewriteDeleteFiles, RewriteDeleteFiles.Result> {

  /**
   * Convert the equality deletes to the position deletes.
   *
   * @return this for method chaining
   */
  RewriteDeleteFiles convertEqDeletesToPosDeletes();

  /**
   * Bin pack the position deletes.
   *
   * @return this for method chaining
   */
  RewriteDeleteFiles binPackPosDeletes();

  /**
   * A filter for choosing deletes to rewrite.
   *
   * @param expression An iceberg expression used to choose deletes.
   * @return this for method chaining
   */
  RewriteDeleteFiles filter(Expression expression);

  /**
   * The action result that contains a summary of the execution.
   */
  interface Result {
    /**
     * Returns the delete files to rewrite.
     */
    Set<DeleteFile> deleteFilesToReplace();

    /**
     * Returns the added delete files.
     */
    Set<DeleteFile> deleteFilesToAdd();
  }
}
