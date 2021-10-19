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

package org.apache.iceberg.aliyun;

import java.io.Serializable;
import java.util.Map;
import org.apache.iceberg.util.PropertyUtil;

public class AliyunProperties implements Serializable {
  /**
   * Location to put staging files for uploading to OSS, defaults to the directory value of java.io.tmpdir.
   */
  public static final String OSS_STAGING_DIRECTORY = "oss.staging-dir";
  private final String ossStagingDirectory;

  public AliyunProperties(Map<String, String> properties) {
    this.ossStagingDirectory = PropertyUtil.propertyAsString(properties, OSS_STAGING_DIRECTORY,
        System.getProperty("java.io.tmpdir"));
  }

  public String ossStagingDirectory() {
    return ossStagingDirectory;
  }
}
