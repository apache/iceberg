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

import org.apache.iceberg.relocated.com.google.common.base.Joiner;

/**
 * Thrown when required files to delete are missing from the snapshot and `validateFilesExist` is
 * enabled in a StreamingDelete operation.
 *
 * <p>This exception includes the locations of the missing files for further inspection.
 */
public class MissingRequiredFilesToDeleteException extends ValidationException {

  private static final Joiner COMMA = Joiner.on(",");
  private final Iterable<CharSequence> missingLocations;

  public MissingRequiredFilesToDeleteException(Iterable<CharSequence> missingLocations) {
    super("Missing required files to delete: %s", COMMA.join(missingLocations));
    this.missingLocations = missingLocations;
  }

  public Iterable<CharSequence> getMissingLocations() {
    return missingLocations;
  }
}
