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
 * This exception occurs when one cherrypicks an ancestor or when the picked snapshot is already
 * linked to a published ancestor. This additionally helps avoid duplicate cherrypicks on non-WAP
 * snapshots.
 */
public class CherrypickAncestorCommitException extends ValidationException {

  public CherrypickAncestorCommitException(long snapshotId) {
    super("Cannot cherrypick snapshot %s: already an ancestor", String.valueOf(snapshotId));
  }

  public CherrypickAncestorCommitException(long snapshotId, long publishedAncestorId) {
    super(
        "Cannot cherrypick snapshot %s: already picked to create ancestor %s",
        String.valueOf(snapshotId), String.valueOf(publishedAncestorId));
  }
}
