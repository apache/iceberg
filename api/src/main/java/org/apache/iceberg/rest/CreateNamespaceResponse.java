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

package org.apache.iceberg.rest;

import java.util.Map;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

/**
 * Represents a REST response to a create a namespace / database request.
 *
 * The properties returned will include all the user provided properties from the
 * request, as well as any server-side added properties.
 *
 * Example server-side added properties could include things such as "created-at"
 * or "owner".
 *
 * Presently, the JSON looks as follows:
 *  { "namespace": "ns1.ns2.ns3", "properties": { "owner": "hank", "created-at": "1425744000000" } }
 *
 * Eventually, the idea is to wrap responses in IcebergHttpResponse so that it has a more standardized
 * structure, including the possibility of richer metadata on errors, such as tracing telemetry or follw
 * up user instruction.
 *
 * {
 *   "error": { },
 *   "data": {
 *     "namespace": "ns1.ns2.ns3",
 *     "properties": {
 *       "owner": "hank",
 *       "created-at": "1425744000000"
 *     }
 *   }
 * }
 *
 * For an error response, we'll see something like the following:
 *
 * {
 *   "data": { },
 *   "error": {
 *     "message": "Namespace already exists",
 *     "type": "AlreadyExistsException",
 *     "code": 40901
 *   }
 * }
 */
public class CreateNamespaceResponse {
  private Namespace namespace;
  private Map<String, String> properties;

  private CreateNamespaceResponse() {
  }

  private CreateNamespaceResponse(Namespace namespace, Map<String, String> properties) {
    this.namespace = namespace;
    this.properties = properties;
  }

  public Namespace getNamespace() {
    return namespace;
  }

  public void setNamespace(Namespace namespace) {
    this.namespace = namespace;
  }

  public Map<String, String> getProperties() {
    return properties;
  }

  public void setProperties(Map<String, String> properties) {
    this.properties = properties;
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {
    private Namespace namespace;
    private Map<String, String> properties;

    private Builder() {

    }

    public Builder withNamespace(Namespace ns) {
      this.namespace = ns;
      return this;
    }

    public Builder withProperties(Map<String, String> props) {
      this.properties = props;
      return this;
    }

    public CreateNamespaceResponse build() {
      Preconditions.checkArgument(namespace != null && !namespace.isEmpty(),
          "Cannot create a CreateNamespaceResponse with a null or empty namespace");
      return new CreateNamespaceResponse(namespace, properties);
    }
  }
}
