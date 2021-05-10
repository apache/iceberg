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

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.dell.emc.ecs.impl.EcsClientImpl;
import org.apache.iceberg.dell.emc.ecs.impl.ObjectKeysImpl;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.util.PropertyUtil;

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
        assertNamespaceKeyExist(namespace, getKeys().getMetadataKey(namespace), NoSuchNamespaceException::new);
    }

    default <EX extends Throwable> void assertNamespaceKeyExist(Namespace namespace, ObjectKey key, Function<String, EX> exceptionFn) throws EX {
        if (namespace.isEmpty()) {
            return;
        }
        if (!head(key).isPresent()) {
            throw exceptionFn.apply(String.format("namespace %s(%s) is not found", namespace, key));
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
        AmazonS3ClientBuilder builder = AmazonS3ClientBuilder.standard()
                .withCredentials(
                        new AWSStaticCredentialsProvider(
                                new BasicAWSCredentials(
                                        properties.get(EcsCatalogProperties.ACCESS_KEY_ID),
                                        properties.get(EcsCatalogProperties.SECRET_ACCESS_KEY)
                                )
                        )
                );
        String endpoint = properties.get(EcsCatalogProperties.ENDPOINT);
        if (endpoint != null) {
            builder.withEndpointConfiguration(
                    new AwsClientBuilder.EndpointConfiguration(
                            endpoint,
                            PropertyUtil.propertyAsString(properties, EcsCatalogProperties.REGION, "-")
                    )
            );
        } else {
            builder.withRegion(properties.get(EcsCatalogProperties.REGION));
        }

        return new EcsClientImpl(
                builder.build(),
                Collections.unmodifiableMap(new LinkedHashMap<>(properties)),
                new ObjectKeysImpl(EcsCatalogProperties.getObjectBaseKey(properties)));
    }
}
