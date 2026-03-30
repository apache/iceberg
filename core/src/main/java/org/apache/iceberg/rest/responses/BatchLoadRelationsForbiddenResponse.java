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
package org.apache.iceberg.rest.responses;

import java.util.List;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.rest.RESTResponse;

/**
 * Error response returned when the batch load request is forbidden due to lack of permissions on
 * one or more requested relations. Extends the standard error model (allOf pattern) with a list of
 * the identifiers that were not accessible.
 *
 * <p>The JSON shape is flat: {@code message}, {@code type}, {@code code} from ErrorModel plus
 * {@code forbidden-identifiers}.
 */
public class BatchLoadRelationsForbiddenResponse implements RESTResponse {

  private String message;
  private String type;
  private int code;
  private List<TableIdentifier> forbiddenIdentifiers;

  public BatchLoadRelationsForbiddenResponse() {
    // Required for Jackson deserialization
  }

  private BatchLoadRelationsForbiddenResponse(
      String message, String type, int code, List<TableIdentifier> forbiddenIdentifiers) {
    this.message = message;
    this.type = type;
    this.code = code;
    this.forbiddenIdentifiers = forbiddenIdentifiers;
  }

  @Override
  public void validate() {
    Preconditions.checkArgument(code == 403, "Invalid code for forbidden response: %s", code);
    Preconditions.checkArgument(
        forbiddenIdentifiers != null && !forbiddenIdentifiers.isEmpty(),
        "Invalid forbidden-identifiers: null or empty");
  }

  public String message() {
    return message;
  }

  public String type() {
    return type;
  }

  public int code() {
    return code;
  }

  public List<TableIdentifier> forbiddenIdentifiers() {
    return forbiddenIdentifiers;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("message", message)
        .add("type", type)
        .add("code", code)
        .add("forbiddenIdentifiers", forbiddenIdentifiers)
        .toString();
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {
    private String message;
    private String type;
    private int code = 403;
    private final ImmutableList.Builder<TableIdentifier> forbiddenIdentifiers =
        ImmutableList.builder();

    private Builder() {}

    public Builder withMessage(String msg) {
      this.message = msg;
      return this;
    }

    public Builder withType(String errorType) {
      this.type = errorType;
      return this;
    }

    public Builder withCode(int errorCode) {
      this.code = errorCode;
      return this;
    }

    public Builder addForbiddenIdentifier(TableIdentifier ident) {
      forbiddenIdentifiers.add(ident);
      return this;
    }

    public Builder addAllForbiddenIdentifiers(List<TableIdentifier> idents) {
      forbiddenIdentifiers.addAll(idents);
      return this;
    }

    public BatchLoadRelationsForbiddenResponse build() {
      BatchLoadRelationsForbiddenResponse response =
          new BatchLoadRelationsForbiddenResponse(
              message, type, code, forbiddenIdentifiers.build());
      response.validate();
      return response;
    }
  }
}
