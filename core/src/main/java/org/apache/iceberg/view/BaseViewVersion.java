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
package org.apache.iceberg.view;

import java.util.List;
import java.util.Map;
import org.apache.iceberg.relocated.com.google.common.base.Objects;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;

public class BaseViewVersion implements ViewVersion {
  private final long versionId;
  private final Long parentId;
  private final long timestampMillis;
  private final Map<String, String> summary;
  private final List<ViewRepresentation> representations;

  public static Builder builder() {
    return new Builder();
  }

  private BaseViewVersion(
      long versionId,
      Long parentId,
      long timestampMillis,
      Map<String, String> summary,
      List<ViewRepresentation> representations) {
    this.versionId = versionId;
    this.parentId = parentId;
    this.timestampMillis = timestampMillis;
    this.summary = summary;
    Preconditions.checkArgument(representations.size() > 0);
    this.representations = representations;
  }

  @Override
  public long versionId() {
    return versionId;
  }

  @Override
  public Long parentId() {
    return parentId;
  }

  @Override
  public long timestampMillis() {
    return timestampMillis;
  }

  @Override
  public Map<String, String> summary() {
    return summary;
  }

  @Override
  public List<ViewRepresentation> representations() {
    return representations;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    BaseViewVersion that = (BaseViewVersion) o;

    if (versionId != that.versionId) {
      return false;
    }
    if (timestampMillis != that.timestampMillis) {
      return false;
    }
    if (!Objects.equal(parentId, that.parentId)) {
      return false;
    }
    if (!Objects.equal(summary, that.summary)) {
      return false;
    }
    return Objects.equal(representations, that.representations);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(
        this.versionId, this.parentId, this.timestampMillis, this.summary, this.representations);
  }

  @Override
  public String toString() {
    return "BaseViewVersion{"
        + "versionId="
        + versionId
        + ", parentId="
        + parentId
        + ", timestampMillis="
        + timestampMillis
        + ", summary="
        + summary
        + ", representations="
        + representations
        + '}';
  }

  public static final class Builder {
    private long versionId;
    private Long parentId;
    private long timestampMillis;
    private Map<String, String> summary = Maps.newHashMap();
    private List<ViewRepresentation> representations = Lists.newArrayList();

    private Builder() {}

    public Builder versionId(long value) {
      versionId = value;
      return this;
    }

    public Builder parentId(Long value) {
      parentId = value;
      return this;
    }

    public Builder timestampMillis(long value) {
      timestampMillis = value;
      return this;
    }

    public Builder summary(Map<String, String> value) {
      summary = value;
      return this;
    }

    public Builder representations(List<ViewRepresentation> value) {
      representations = value;
      return this;
    }

    public Builder addRepresentation(ViewRepresentation representation) {
      representations.add(representation);
      return this;
    }

    public BaseViewVersion build() {
      return new BaseViewVersion(versionId, parentId, timestampMillis, summary, representations);
    }
  }
}
