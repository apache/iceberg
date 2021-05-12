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
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;

public class BaseRewriteManifestsActionResult implements RewriteManifests.Result {

  private final Iterable<ManifestFile> rewrittenManifests;
  private final Iterable<ManifestFile> addedManifests;
  private final Iterable<?> repairedManifests;

  public BaseRewriteManifestsActionResult(Iterable<ManifestFile> rewrittenManifests,
                                          Iterable<ManifestFile> addedManifests,
                                          Iterable<BaseRepairedManifestFile> repairedManifests) {
    this.rewrittenManifests = rewrittenManifests;
    this.addedManifests = addedManifests;
    this.repairedManifests = repairedManifests;
  }

  public static RewriteManifests.Result empty() {
    return new BaseRewriteManifestsActionResult(ImmutableList.of(), ImmutableList.of(), ImmutableList.of());
  }

  @Override
  public Iterable<ManifestFile> rewrittenManifests() {
    return rewrittenManifests;
  }

  @Override
  public Iterable<ManifestFile> addedManifests() {
    return addedManifests;
  }

  @Override
  public Iterable<RepairedManifest> repairedManifests() {
    return (Iterable<RepairedManifest>) repairedManifests;
  }

  public static class BaseRepairedManifestFile implements RewriteManifests.Result.RepairedManifest {
    private ManifestFile manifestFile;
    private Set<String> fields;

    public BaseRepairedManifestFile(ManifestFile mf, Set<String> fields) {
      Preconditions.checkNotNull(mf, "Manifest file is null");
      Preconditions.checkNotNull(fields, "Fields are null");
      this.manifestFile = mf;
      this.fields = fields;
    }

    @Override
    public Set<String> fields() {
      return fields;
    }

    @Override
    public ManifestFile manifest() {
      return manifestFile;
    }
  }
}
