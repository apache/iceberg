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

import java.net.URI;
import java.util.Map;
import org.apache.iceberg.relocated.com.google.common.base.Strings;

/**
 * A normalized file identifier used to compare locations by path, scheme, and authority.
 *
 * <p>This type does not retain the original URI string. Use {@link FileURI} when the original
 * location is required for an operation such as deleting a file.
 */
public class FileIdentifier {

  private String authority;
  private String path;
  private String scheme;

  public FileIdentifier(String scheme, String authority, String path) {
    this.scheme = scheme;
    this.authority = authority;
    this.path = path;
  }

  public FileIdentifier(
      URI uri, Map<String, String> equalSchemes, Map<String, String> equalAuthorities) {
    this(
        equalSchemes.getOrDefault(uri.getScheme(), uri.getScheme()),
        equalAuthorities.getOrDefault(uri.getAuthority(), uri.getAuthority()),
        uri.getPath());
  }

  public FileIdentifier() {}

  public String getAuthority() {
    return authority;
  }

  public void setAuthority(String authority) {
    this.authority = authority;
  }

  public String getPath() {
    return path;
  }

  public void setPath(String path) {
    this.path = path;
  }

  public String getScheme() {
    return scheme;
  }

  public void setScheme(String scheme) {
    this.scheme = scheme;
  }

  public boolean authorityMatch(FileIdentifier another) {
    return uriComponentMatch(authority, another.getAuthority());
  }

  public boolean schemeMatch(FileIdentifier another) {
    return uriComponentMatch(scheme, another.getScheme());
  }

  private boolean uriComponentMatch(String valid, String actual) {
    return Strings.isNullOrEmpty(valid) || valid.equalsIgnoreCase(actual);
  }
}
