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
package org.apache.iceberg.index;

import org.apache.iceberg.MetadataUpdate;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

/** Represents a requirement for a {@link MetadataUpdate} */
public interface IndexRequirement {
  default void validate(IndexMetadata base) {
    throw new ValidationException(
        "Cannot validate %s against an index", this.getClass().getSimpleName());
  }

  class AssertIndexDoesNotExist implements IndexRequirement {
    public AssertIndexDoesNotExist() {}

    @Override
    public void validate(IndexMetadata base) {
      if (base != null) {
        throw new CommitFailedException("Requirement failed: table already exists");
      }
    }
  }

  class AssertIndexUUID implements IndexRequirement {
    private final String uuid;

    public AssertIndexUUID(String uuid) {
      Preconditions.checkArgument(uuid != null, "Invalid required UUID: null");
      this.uuid = uuid;
    }

    public String uuid() {
      return uuid;
    }

    @Override
    public void validate(IndexMetadata base) {
      if (!uuid.equalsIgnoreCase(base.uuid())) {
        throw new CommitFailedException(
            "Requirement failed: UUID does not match: expected %s != %s", base.uuid(), uuid);
      }
    }
  }
}
