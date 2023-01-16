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
package org.apache.iceberg.exceptions;

/**
 * Exception for a failure to confirm either affirmatively or negatively that a commit was applied.
 * The client cannot take any further action without possibly corrupting the table.
 */
public class CommitStateUnknownException extends RuntimeException {

  private static final String COMMON_INFO =
      "Cannot determine whether the commit was successful or not, the underlying data files may or "
          + "may not be needed. Manual intervention via the Remove Orphan Files Action can remove these "
          + "files when a connection to the Catalog can be re-established if the commit was actually unsuccessful.\n"
          + "Please check to see whether or not your commit was successful before retrying this commit. Retrying "
          + "an already successful operation will result in duplicate records or unintentional modifications.\n"
          + "At this time no files will be deleted including possibly unused manifest lists.";

  public CommitStateUnknownException(Throwable cause) {
    super(cause.getMessage() + "\n" + COMMON_INFO, cause);
  }

  public CommitStateUnknownException(String message, Throwable cause) {
    super(message + "\n" + cause.getMessage() + "\n" + COMMON_INFO, cause);
  }
}
