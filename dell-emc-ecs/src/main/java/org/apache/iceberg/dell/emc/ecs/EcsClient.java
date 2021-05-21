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

package org.apache.iceberg.dell.emc.ecs;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;

/**
 * ECS client and its util-methods.
 */
public interface EcsClient extends AutoCloseable {

    /**
     * Operations of object key.
     *
     * @return object key operations
     */
    ObjectKeys getKeys();

    /**
     * get original properties.
     * <p>
     * when try to implement this interface, make this properties same as input of {@link EcsClient#create(Map)}
     *
     * @return properties of client
     */
    Map<String, String> getProperties();

    /**
     * get object info of specific key. If object is absent, return {@link Optional#empty()}
     *
     * @param key object key
     * @return head info if present
     */
    Optional<ObjectHeadInfo> head(ObjectKey key);

    /**
     * get object input stream of specific key.
     *
     * @param key object key
     * @return input stream
     */
    InputStream inputStream(ObjectKey key, long pos);

    /**
     * get object output stream of specific key.
     *
     * @param key object key
     * @return output stream
     */
    OutputStream outputStream(ObjectKey key);

    /**
     * return tuple of {@link #readAll(ObjectKey)}
     */
    interface ContentAndETag {
        String getETag();

        byte[] getContent();
    }

    /**
     * read all bytes of object
     *
     * @return bytes and e-tag
     */
    ContentAndETag readAll(ObjectKey key);

    /**
     * compare-and-swap operation
     * <p>
     * we assume that E-Tag can distinct content of the objects.
     */
    boolean replace(ObjectKey key, String eTag, byte[] bytes);

    /**
     * compare-and-swap operation
     */
    boolean writeIfAbsent(ObjectKey key, byte[] bytes);

    /**
     * compare-and-swap operation
     * <p>
     * we assume that E-Tag can distinct content of the objects.
     */
    boolean copyObjectIfAbsent(ObjectKey fromKey, String eTag, ObjectKey toKey);

    /**
     * delete object
     *
     * @param key is object key
     */
    void deleteObject(ObjectKey key);

    /**
     * list with delimiter
     *
     * @param prefix          prefix key
     * @param filterAndMapper map object key to specify item
     * @param <T>             search item type
     * @return all items with given prefix
     */
    <T> List<T> listDelimiterAll(ObjectKey prefix, Function<ObjectKey, Optional<T>> filterAndMapper);

    /**
     * list all tables under this namespace.
     *
     * @param namespace a namespace
     * @return a list of identifiers for tables
     */
    default List<TableIdentifier> listTables(Namespace namespace) throws NoSuchNamespaceException {
        assertNamespaceExist(namespace);
        return listDelimiterAll(getKeys().getPrefix(namespace), key -> getKeys().extractTableIdentifier(key, namespace));
    }

    /**
     * list all namespaces under this namespace.
     *
     * @param namespace a namespace
     * @return a list of namespace
     */
    default List<Namespace> listNamespaces(Namespace namespace) throws NoSuchNamespaceException {
        assertNamespaceExist(namespace);
        return listDelimiterAll(getKeys().getPrefix(namespace), key -> getKeys().extractNamespace(key, namespace));
    }

    default void assertNamespaceExist(Namespace namespace) throws NoSuchNamespaceException {
        if (namespace.isEmpty()) {
            return;
        }
        ObjectKey key = getKeys().getMetadataKey(namespace);
        if (!head(key).isPresent()) {
            throw new NoSuchNamespaceException("namespace %s(%s) is not found", namespace, key);
        }
    }

    /**
     * copy a client
     *
     * @return a new ecs client
     */
    default EcsClient copy() {
        return EcsClient.create(getProperties());
    }

    /**
     * static factory method of {@link EcsClient}
     *
     * @param properties is properties
     * @return ecs client
     */
    static EcsClient create(Map<String, String> properties) {
        return EcsCatalogProperties.getEcsClientFromFactory(properties)
                .orElseGet(() -> EcsCatalogProperties.getBuiltInEcsClient(properties));
    }
}
