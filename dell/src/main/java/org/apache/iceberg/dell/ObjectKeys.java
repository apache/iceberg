/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.iceberg.dell;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.relocated.com.google.common.base.Splitter;

/**
 * The operations of {@link ObjectKey}
 */
public interface ObjectKeys {

  /**
   * The default and generally be-used delimiter.
   */
  String DELIMITER = "/";
  /**
   * The default suffix of table metadata object
   */
  String TABLE_METADATA_SUFFIX = ".table";
  /**
   * The default suffix of namespace metadata object
   */
  String NAMESPACE_METADATA_SUFFIX = ".namespace";

  /**
   * The base key of catalog.
   */
  ObjectBaseKey baseKey();

  /**
   * The base key parts for calculate sub key.
   *
   * @return the parts of the base key
   * @see #subParts(ObjectKey)
   */
  default List<String> baseKeyParts() {
    ObjectBaseKey baseKey = baseKey();
    if (baseKey.getBucket() == null) {
      return Collections.emptyList();
    } else if (baseKey.getKey() == null) {
      return Collections.singletonList(baseKey.getBucket());
    } else {
      List<String> parts = new ArrayList<>();
      parts.add(baseKey.getBucket());
      for (String result : Splitter.on(getDelimiter()).split(baseKey.getKey())) {
        parts.add(result);
      }
      if (!checkParts(parts)) {
        throw new IllegalArgumentException(String.format("invalid base key %s with delimiter %s",
            baseKey, getDelimiter()));
      }
      return Collections.unmodifiableList(parts);
    }
  }

  /**
   * Get current delimiter.
   * <p>
   * Now, we only support "/".
   */
  default String getDelimiter() {
    return DELIMITER;
  }

  /**
   * Get current namespace object suffix.
   * <p>
   * Now, we only support ".namespace".
   */
  default String getNamespaceMetadataSuffix() {
    return NAMESPACE_METADATA_SUFFIX;
  }

  /**
   * Get current table object suffix.
   * <p>
   * Now, we only support ".table".
   */
  default String getTableMetadataSuffix() {
    return TABLE_METADATA_SUFFIX;
  }

  /**
   * Convert relative parts to object key
   *
   * @param parts that relative to base key
   * @return object key
   */
  default ObjectKey createObjectKey(List<String> parts) {
    ObjectBaseKey baseKey = baseKey();
    if (parts.isEmpty()) {
      return baseKey.asKey();
    }
    String delimiter = getDelimiter();
    if (parts.stream().anyMatch(it -> it.contains(delimiter))) {
      throw new IllegalArgumentException(String.format("delimiter %s in key parts: %s", delimiter, parts));
    }
    if (baseKey.getBucket() == null) {
      return new ObjectKey(parts.get(0), String.join(delimiter, parts.subList(1, parts.size())));
    } else {
      String prefix = baseKey.getKey() == null ? "" : (baseKey.getKey() + delimiter);
      return new ObjectKey(baseKey.getBucket(), prefix + String.join(delimiter, parts));
    }
  }

  /**
   * Create an {@link ObjectKey} for namespace object
   */
  default ObjectKey createMetadataKey(Namespace namespace) {
    if (namespace.isEmpty()) {
      return createObjectKey(Collections.singletonList(getNamespaceMetadataSuffix()));
    }
    // copy namespace levels
    List<String> keyParts = new ArrayList<>(Arrays.asList(namespace.levels()));
    int lastIndex = keyParts.size() - 1;
    keyParts.set(lastIndex, keyParts.get(lastIndex) + getNamespaceMetadataSuffix());
    return createObjectKey(keyParts);
  }

  /**
   * Create a prefix {@link ObjectKey} to list tables or namespaces in the specific namespace.
   * <p>
   * The prefix key lack namespace metadata suffix.
   */
  default ObjectKey createPrefixKey(Namespace namespace) {
    return createObjectKey(Arrays.asList(namespace.levels()));
  }

  /**
   * Try to extract {@link Namespace} from specific key. If the key is not a namespace object key pattern, or parent
   * namespace is not matched, the method will return {@link Optional#empty()}
   */
  default Optional<Namespace> extractNamespace(ObjectKey key, Namespace parent) {
    if (!key.getKey().endsWith(getNamespaceMetadataSuffix())) {
      return Optional.empty();
    }
    Optional<String> lastPartOpt = extractLastPart(key, parent);
    if (!lastPartOpt.isPresent()) {
      return Optional.empty();
    }
    String lastPart = lastPartOpt.get();
    String namespaceName = lastPart.substring(0, lastPart.length() - getNamespaceMetadataSuffix().length());
    String[] levels = Arrays.copyOf(parent.levels(), parent.levels().length + 1);
    levels[levels.length - 1] = namespaceName;
    return Optional.of(Namespace.of(levels));
  }

  /**
   * Create an {@link ObjectKey} for table object.
   */
  default ObjectKey createMetadataKey(TableIdentifier tableIdentifier) {
    if (tableIdentifier.hasNamespace()) {
      List<String> parts = new ArrayList<>(tableIdentifier.namespace().levels().length + 1);
      parts.addAll(Arrays.asList(tableIdentifier.namespace().levels()));
      parts.add(tableIdentifier.name() + getTableMetadataSuffix());
      return createObjectKey(parts);
    } else {
      return createObjectKey(Collections.singletonList(tableIdentifier.name() + getTableMetadataSuffix()));
    }
  }

  /**
   * Try to extract {@link TableIdentifier} from specific key. If the key is not a table object key pattern, or
   * namespace is not matched, the method will return {@link Optional#empty()}
   */
  default Optional<TableIdentifier> extractTableIdentifier(ObjectKey key, Namespace namespace) {
    if (!key.getKey().endsWith(getTableMetadataSuffix())) {
      return Optional.empty();
    }
    Optional<String> lastPartOpt = extractLastPart(key, namespace);
    if (!lastPartOpt.isPresent()) {
      return Optional.empty();
    }
    String lastPart = lastPartOpt.get();
    String tableName = lastPart.substring(0, lastPart.length() - getTableMetadataSuffix().length());
    return Optional.of(TableIdentifier.of(namespace, tableName));
  }

  /**
   * Try to extract last part from specific key. If the parent key is not match the input namespace, the method
   * will return {@link Optional#empty()}
   */
  default Optional<String> extractLastPart(ObjectKey key, Namespace expectNamespace) {
    Optional<List<String>> partsOpt = subParts(key);
    if (!partsOpt.isPresent()) {
      return Optional.empty();
    }
    List<String> parts = partsOpt.get();
    if (parts.isEmpty()) {
      return Optional.empty();
    }
    int lastIndex = parts.size() - 1;
    Namespace namespace = Namespace.of(parts.subList(0, lastIndex).toArray(new String[] {}));
    if (expectNamespace != null && !Objects.equals(expectNamespace, namespace)) {
      throw new IllegalArgumentException(String.format("namespace not match: %s != %s", namespace, expectNamespace));
    }
    return Optional.of(parts.get(lastIndex));
  }

  /**
   * Get the relative parts of {@link #baseKey()}. The object key will be spilt by {@link #getDelimiter()}
   */
  default Optional<List<String>> subParts(ObjectKey key) {
    List<String> parts = new ArrayList<>();
    parts.add(key.getBucket());
    for (String result : Splitter.on(getDelimiter()).split(key.getKey())) {
      parts.add(result);
    }
    if (!checkParts(parts)) {
      return Optional.empty();
    }
    List<String> baseParts = baseKeyParts();
    if (parts.size() < baseParts.size() || !Objects.equals(parts.subList(0, baseParts.size()), baseParts)) {
      return Optional.empty();
    } else {
      return Optional.of(parts.subList(baseParts.size(), parts.size()));
    }
  }

  /**
   * Check all parts are valid in Iceberg.
   */
  default boolean checkParts(List<String> parts) {
    return parts.stream().noneMatch(String::isEmpty);
  }

  /**
   * Get default warehouse location of table id
   *
   * @param tableIdentifier is table id
   * @return default warehouse location prefix key
   */
  default ObjectKey warehouseLocation(TableIdentifier tableIdentifier) {
    if (!tableIdentifier.hasNamespace()) {
      return createObjectKey(Collections.singletonList(tableIdentifier.name()));
    } else {
      List<String> parts = new ArrayList<>(tableIdentifier.namespace().levels().length + 1);
      parts.addAll(Arrays.asList(tableIdentifier.namespace().levels()));
      parts.add(tableIdentifier.name());
      return createObjectKey(parts);
    }
  }

  /**
   * Convert {@link ObjectKey} to string
   */
  default String toString(ObjectKey key) {
    return key.getBucket() + getDelimiter() + key.getKey();
  }

  /**
   * Convert string to {@link ObjectKey}
   */
  default ObjectKey parse(String key) {
    String[] r = key.split(getDelimiter(), 2);
    if (r.length < 2) {
      throw new IllegalArgumentException("failed to parse key " + key);
    }
    return new ObjectKey(r[0], r[1]);
  }
}
