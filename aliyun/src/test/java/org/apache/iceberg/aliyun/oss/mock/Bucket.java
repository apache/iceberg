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

package org.apache.iceberg.aliyun.oss.mock;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonRootName;

@JsonRootName("Bucket")
public class Bucket {
  @JsonProperty("Name")
  private String name;

  @JsonProperty("CreationDate")
  private String creationDate;

  @JsonProperty("ExtranetEndpoint")
  private String extranetEndpoint;

  @JsonProperty("IntranetEndpoint")
  private String intranetEndpoint;

  @JsonProperty("Location")
  private String location;

  @JsonProperty("Region")
  private String region;

  @JsonProperty("StorageClass")
  private String storageClass;

  /**
   * Constructs a new {@link Models.Bucket}.
   *
   * @param name         of bucket
   * @param creationDate date of creation.
   */
  public Bucket(final String name, final String creationDate) {
    this.name = name;
    this.creationDate = creationDate;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getCreationDate() {
    return creationDate;
  }

  public void setCreationDate(String creationDate) {
    this.creationDate = creationDate;
  }

  public String getExtranetEndpoint() {
    return extranetEndpoint;
  }

  public void setExtranetEndpoint(String extranetEndpoint) {
    this.extranetEndpoint = extranetEndpoint;
  }

  public String getIntranetEndpoint() {
    return intranetEndpoint;
  }

  public void setIntranetEndpoint(String intranetEndpoint) {
    this.intranetEndpoint = intranetEndpoint;
  }

  public String getLocation() {
    return location;
  }

  public void setLocation(String location) {
    this.location = location;
  }

  public String getRegion() {
    return region;
  }

  public void setRegion(String region) {
    this.region = region;
  }

  public String getStorageClass() {
    return storageClass;
  }

  public void setStorageClass(String storageClass) {
    this.storageClass = storageClass;
  }
}
