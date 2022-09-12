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
package org.apache.iceberg.aws.lakeformation;

import java.util.Objects;
import java.util.Set;
import software.amazon.awssdk.services.lakeformation.LakeFormationClient;
import software.amazon.awssdk.services.sts.model.Tag;

class LakeFormationIdentity {
  private final String roleArn;
  private final String sessionName;
  private final String externalId;
  private final Set<Tag> tags;
  private final String region;
  private final int timeout;
  private final LakeFormationClient client;

  LakeFormationIdentity(
      String roleArn,
      String sessionName,
      String externalId,
      Set<Tag> tags,
      String region,
      int timeout,
      LakeFormationClient client) {
    this.roleArn = roleArn;
    this.sessionName = sessionName;
    this.externalId = externalId;
    this.tags = tags;
    this.region = region;
    this.timeout = timeout;
    this.client = client;
  }

  public String roleArn() {
    return roleArn;
  }

  public String sessionName() {
    return sessionName;
  }

  public String externalId() {
    return externalId;
  }

  public Set<Tag> tags() {
    return tags;
  }

  public String region() {
    return region;
  }

  public int timeout() {
    return timeout;
  }

  public LakeFormationClient lakeFormationClient() {
    return client;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (!(o instanceof LakeFormationIdentity)) {
      return false;
    }

    LakeFormationIdentity identity = (LakeFormationIdentity) o;

    return Objects.equals(roleArn(), identity.roleArn())
        && Objects.equals(sessionName(), identity.sessionName())
        && Objects.equals(externalId(), identity.externalId())
        && Objects.equals(tags(), identity.tags())
        && Objects.equals(region(), identity.region())
        && timeout() == identity.timeout();
  }

  @Override
  public int hashCode() {
    return Objects.hash(roleArn, sessionName, externalId, tags, region, timeout);
  }
}
