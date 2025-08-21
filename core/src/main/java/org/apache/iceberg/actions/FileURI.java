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
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;
import org.apache.iceberg.relocated.com.google.common.base.Strings;

public class FileURI {

  private String scheme;
  private String authority;
  private String path;
  private String uriAsString;

  public FileURI(String scheme, String authority, String path, String uriAsString) {
    this.scheme = scheme;
    this.authority = authority;
    this.path = path;
    this.uriAsString = uriAsString;
  }

  public FileURI(URI uri, Map<String, String> equalSchemes, Map<String, String> equalAuthorities) {
    this.scheme = equalSchemes.getOrDefault(uri.getScheme(), uri.getScheme());
    this.authority = equalAuthorities.getOrDefault(uri.getAuthority(), uri.getAuthority());
    this.path = uri.getPath();
    this.uriAsString = uri.toString();
  }

  public FileURI() {}

  public String getScheme() {
    return scheme;
  }

  public void setScheme(String scheme) {
    this.scheme = scheme;
  }

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

  public String getUriAsString() {
    return uriAsString;
  }

  public void setUriAsString(String uriAsString) {
    this.uriAsString = uriAsString;
  }

  public boolean schemeMatch(FileURI another) {
    return uriComponentMatch(scheme, another.getScheme());
  }

  public boolean authorityMatch(FileURI another) {
    return uriComponentMatch(authority, another.getAuthority());
  }

  private boolean uriComponentMatch(String valid, String actual) {
    return Strings.isNullOrEmpty(valid) || valid.equalsIgnoreCase(actual);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("scheme", scheme)
        .add("authority", authority)
        .add("path", path)
        .add("uriAsString", uriAsString)
        .toString();
  }
}
