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
package org.apache.iceberg.spark.actions;

import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;

public class ManifestFileInfo {
  public static final Encoder<ManifestFileInfo> ENCODER = Encoders.bean(ManifestFileInfo.class);

  private String manifest;

  private int manifestContent;

  public ManifestFileInfo(String manifest, int manifestContent) {
    this.manifest = manifest;
    this.manifestContent = manifestContent;
  }

  public ManifestFileInfo() {}

  public String getManifest() {
    return manifest;
  }

  public void setManifest(String manifest) {
    this.manifest = manifest;
  }

  public int getManifestContent() {
    return manifestContent;
  }

  public void setManifestContent(int manifestContent) {
    this.manifestContent = manifestContent;
  }
}
