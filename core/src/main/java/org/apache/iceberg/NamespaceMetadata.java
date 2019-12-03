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

package org.apache.iceberg;

import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.UUID;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.io.InputFile;

public class NamespaceMetadata {
  static final int NAMESPACE_FORMAT_VERSION = 1;

  public static NamespaceMetadata newNamespaceMetadata(String location, Namespace namespace) {

    return new NamespaceMetadata(null, UUID.randomUUID().toString(), location,
        System.currentTimeMillis(), ImmutableList.of(namespace));
  }


  private final InputFile file;
  // stored metadata
  private final String uuid;
  private final String location;
  private final long lastUpdatedMillis;
  private List<Namespace> namespaces;

  NamespaceMetadata(InputFile file,
                     String uuid,
                     String location,
                     long lastUpdatedMillis,
                     List<Namespace> namespaces) {

    this.file = file;
    this.uuid = uuid;
    this.location = location;
    this.lastUpdatedMillis = lastUpdatedMillis;
    this.namespaces = namespaces;
  }

  public InputFile file() {
    return file;
  }

  public String uuid() {
    return uuid;
  }

  public long lastUpdatedMillis() {
    return lastUpdatedMillis;
  }

  public String location() {
    return location;
  }

  public List<Namespace> namespaces() {
    return namespaces;
  }

  public void addNamespace(List<Namespace> baseSpace) {
    baseSpace.addAll(this.namespaces);
    this.namespaces =  baseSpace;
  }

  public void replaceNamespace(List<Namespace> baseSpace) {
    String name = this.namespaces().get(0).toString();
    for (int i = baseSpace.size() - 1; i >= 0; i--) {
      Namespace item = baseSpace.get(i);
      if (item.toString().equals(name)) {
        baseSpace.remove(item);
      }
    }
    baseSpace.addAll(this.namespaces);
    this.namespaces = baseSpace;
  }
}
