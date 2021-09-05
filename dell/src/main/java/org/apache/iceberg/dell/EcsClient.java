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

import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

/**
 * ECS client and its util-methods.
 */
public interface EcsClient extends AutoCloseable {

  /**
   * Get an {@link ObjectKeys} instance to convert between {@link ObjectKey} and {@link String}.
   */
  ObjectKeys objectKeys();

  /**
   * Get a {@link PropertiesSerDes} instance to convert between {@link Map} and object content.
   */
  default PropertiesSerDes propertiesSerDes() {
    return PropertiesSerDes.useJdk();
  }

  /**
   * Get the object info of specific key. If object is absent, return {@link Optional#empty()}
   */
  Optional<ObjectHeadInfo> head(ObjectKey key);

  /**
   * Get the {@link InputStream} of specific key and position. If object is absent, an exception will be thrown.
   */
  InputStream inputStream(ObjectKey key, long pos);

  /**
   * Get the {@link OutputStream} of specific key. If object is present, the behaviour is undefined.
   */
  OutputStream outputStream(ObjectKey key);

  /**
   * A tuple interface for {@link #readAll(ObjectKey)}
   */
  interface ContentAndHeadInfo {
    ObjectHeadInfo getHeadInfo();

    byte[] getContent();
  }

  /**
   * Get the object content and the head info of object. If object is absent, an exception will be thrown.
   */
  ContentAndHeadInfo readAll(ObjectKey key);

  /**
   * A CAS operation to replace an object with the previous E-Tag.
   * <p>
   * We assume that E-Tag can distinct content of the objects.
   * <p>
   * If E-Tag is not matched, the method will return false, and the object won't be changed.
   */
  boolean replace(ObjectKey key, String eTag, byte[] bytes, Map<String, String> userMetadata);

  /**
   * A CAS operation to create an object.
   * <p>
   * If the specific object is existed, the method will return false, and the existed object won't be changed.
   */
  boolean writeIfAbsent(ObjectKey key, byte[] bytes, Map<String, String> userMetadata);

  /**
   * A CAS operation to copy an object.
   * <p>
   * We assume that E-Tag can distinct content of the objects.
   * <p>
   * If the destination object is existed, or the original object is not matched with E-Tag, the method will return
   * false, the both objects won't be changed.
   */
  boolean copyObjectIfAbsent(ObjectKey fromKey, String eTag, ObjectKey toKey);

  /**
   * Delete object
   */
  void deleteObject(ObjectKey key);

  /**
   * List all objects with delimiter.
   * <p>
   * For example: there are objects like:
   * <ul>
   * <li>namespace1/namespace2.namespace</li>
   * <li>namespace1/namespace2/table1.table</li>
   * <li>namespace1/table1.table</li>
   * <li>namespace1/table2.table</li>
   * </ul>
   * If prefix is namespace1 and delimiter is /, then return value will be
   * <ul>
   * <li>namespace1/table1.table</li>
   * <li>namespace1/table2.table</li>
   * </ul>
   * <p>
   * The function can filter and convert to the object that user want to use
   * <p>
   * note: the common prefixes, such as namespace1/namespace2/, won't return by this method.
   *
   * @param prefix          prefix key
   * @param filterAndMapper map object key to specify item
   * @param <T>             search item type
   * @return all items with given prefix
   */
  <T> List<T> listDelimiterAll(ObjectKey prefix, Function<ObjectKey, Optional<T>> filterAndMapper);
}
