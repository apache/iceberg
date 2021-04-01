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

package org.apache.iceberg.nessie;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Predicate;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.projectnessie.model.ContentsKey;
import org.projectnessie.model.EntriesResponse;

public final class NessieUtil {

  private NessieUtil() {

  }

  static Predicate<EntriesResponse.Entry> namespacePredicate(Namespace ns) {
    // TODO: filter to just iceberg tables.
    if (ns == null) {
      return e -> true;
    }

    final List<String> namespace = Arrays.asList(ns.levels());
    Predicate<EntriesResponse.Entry> predicate = e -> {
      List<String> names = e.getName().getElements();

      if (names.size() <= namespace.size()) {
        return false;
      }

      return namespace.equals(names.subList(0, namespace.size()));
    };
    return predicate;
  }

  static TableIdentifier toIdentifier(EntriesResponse.Entry entry) {
    List<String> elements = entry.getName().getElements();
    return TableIdentifier.of(elements.toArray(new String[elements.size()]));
  }

  static TableIdentifier removeCatalogName(TableIdentifier to, String name) {

    String[] levels = to.namespace().levels();
    // check if the identifier includes the catalog name and remove it
    if (levels.length >= 2 && name.equalsIgnoreCase(to.namespace().level(0))) {
      Namespace trimmedNamespace = Namespace.of(Arrays.copyOfRange(levels, 1, levels.length));
      return TableIdentifier.of(trimmedNamespace, to.name());
    }

    // return the original unmodified
    return to;
  }

  static ContentsKey toKey(TableIdentifier tableIdentifier) {
    List<String> identifiers = new ArrayList<>();
    if (tableIdentifier.hasNamespace()) {
      identifiers.addAll(Arrays.asList(tableIdentifier.namespace().levels()));
    }
    identifiers.add(tableIdentifier.name());

    ContentsKey key = ContentsKey.of(identifiers);
    return key;
  }

}
