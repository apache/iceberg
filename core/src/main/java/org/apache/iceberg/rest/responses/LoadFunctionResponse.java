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

import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.rest.RESTResponse;

/** A response that contains a single function spec JSON object. */
public class LoadFunctionResponse implements RESTResponse {

  private ObjectNode spec;

  public LoadFunctionResponse() {
    // Required for Jackson deserialization
  }

  private LoadFunctionResponse(ObjectNode spec) {
    this.spec = spec;
    validate();
  }

  @Override
  public void validate() {
    Preconditions.checkArgument(spec != null, "Invalid function spec: null");
  }

  public ObjectNode spec() {
    return spec;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).add("spec", spec).toString();
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {
    private ObjectNode spec;

    private Builder() {}

    public Builder spec(ObjectNode value) {
      this.spec = value;
      return this;
    }

    public LoadFunctionResponse build() {
      return new LoadFunctionResponse(spec);
    }
  }
}
