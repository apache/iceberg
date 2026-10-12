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
package org.apache.iceberg.flink.maintenance.operator;

import java.util.Map;
import org.apache.flink.annotation.Internal;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.actions.FileURI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A key selector implementation that extracts a normalized file path from a file URI string.
 *
 * <p>This selector groups file URIs by their normalized path, ignoring differences in scheme and
 * authority that are considered equivalent according to the provided mappings.
 */
@Internal
public class FileUriKeySelector implements KeySelector<String, String> {
  private static final Logger LOG = LoggerFactory.getLogger(FileUriKeySelector.class);

  static final String INVALID_URI = "__INVALID_URI__";

  private final Map<String, String> equalSchemes;
  private final Map<String, String> equalAuthorities;

  public FileUriKeySelector(
      Map<String, String> equalSchemes, Map<String, String> equalAuthorities) {
    this.equalSchemes = equalSchemes;
    this.equalAuthorities = equalAuthorities;
  }

  @Override
  public String getKey(String value) throws Exception {
    try {
      FileURI fileUri = new FileURI(new Path(value).toUri(), equalSchemes, equalAuthorities);
      return fileUri.getPath();
    } catch (Exception e) {
      LOG.warn("Uri convert to FileURI error! Uri is {}.", value, e);
      return INVALID_URI;
    }
  }
}
