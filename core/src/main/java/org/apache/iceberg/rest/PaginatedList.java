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
package org.apache.iceberg.rest;

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.Supplier;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.rest.responses.ListNamespacesResponse;
import org.apache.iceberg.rest.responses.ListTablesResponse;
import org.apache.iceberg.rest.responses.Route;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PaginatedList<T> implements List<T> {
  private static final Logger LOG = LoggerFactory.getLogger(PaginatedList.class);

  private static final String UNMODIFIABLE_MESSAGE = "This is an unmodifiable list";
  private RESTClient client;
  private ResourcePaths paths;
  private Supplier<Map<String, String>> headers;
  private String pageToken;
  private String pageSize;
  private List<T> pageItems;
  private Map<String, String> queryParams;
  private Namespace namespace;
  private Route route;

  public PaginatedList(
      RESTClient client,
      ResourcePaths paths,
      Supplier<Map<String, String>> headers,
      String pageSize,
      Namespace namespace,
      Route route) {
    this.client = client;
    this.paths = paths;
    this.headers = headers;
    this.pageSize = pageSize;
    this.pageToken = ""; // start protocol with empty token
    this.pageItems = Lists.newArrayList();
    this.queryParams = Maps.newHashMap();
    this.namespace = namespace;
    this.route = route;
  }

  @Override
  public int size() {
    return pageItems.size();
  }

  @Override
  public boolean isEmpty() {
    return !iterator().hasNext();
  }

  @Override
  public boolean contains(Object o) {
    return pageItems.contains(o);
  }

  @Override
  public Iterator<T> iterator() {
    return new Iterator<T>() {
      @Override
      public boolean hasNext() {
        if (!pageItems.isEmpty()) {
          return true;
        }
        getPaginatedItems();
        return !pageItems.isEmpty();
      }

      @Override
      public T next() {
        return pageItems.remove(0);
      }
    };
  }

  public void getPaginatedItems() {
    if (pageToken == null) {
      return;
    }
    if (pageSize == null) {
      throw new ValidationException("if pageToken is present, pageSize must be set");
    }
    queryParams.put("pageToken", pageToken);
    queryParams.put("pageSize", pageSize);
    LOG.info("Sending request with pageToken: {}, pageSize: {}", pageToken, pageSize);

    switch (route) {
      case LIST_NAMESPACES:
        if (!namespace.isEmpty()) {
          queryParams.put("parent", RESTUtil.NAMESPACE_JOINER.join(namespace.levels()));
        }
        ListNamespacesResponse listNamespacesResponse =
            client.get(
                paths.namespaces(),
                queryParams,
                ListNamespacesResponse.class,
                headers,
                ErrorHandlers.namespaceErrorHandler());
        LOG.info("Received paginated response {}", listNamespacesResponse);
        pageToken = listNamespacesResponse.nextPageToken();
        pageItems.addAll((Collection<? extends T>) listNamespacesResponse.namespaces());
        return;

      case LIST_TABLES:
        ListTablesResponse listTablesResponse =
            client.get(
                paths.tables(namespace),
                queryParams,
                ListTablesResponse.class,
                headers,
                ErrorHandlers.namespaceErrorHandler());
        LOG.info("Received paginated response {}", listTablesResponse);
        pageToken = listTablesResponse.nextPageToken();
        pageItems.addAll((Collection<? extends T>) listTablesResponse.identifiers());
        return;

      case LIST_VIEWS:
        ListTablesResponse listViewsResponse =
            client.get(
                paths.views(namespace),
                queryParams,
                ListTablesResponse.class,
                headers,
                ErrorHandlers.namespaceErrorHandler());
        LOG.info("Received paginated response {}", listViewsResponse);
        pageToken = listViewsResponse.nextPageToken();
        pageItems.addAll((Collection<? extends T>) listViewsResponse.identifiers());
        return;

      default:
        throw new UnsupportedOperationException("Invalid route: " + route);
    }
  }

  @Override
  public Object[] toArray() {
    return pageItems.toArray();
  }

  @Override
  public <T> T[] toArray(T[] a) {
    return pageItems.toArray(a);
  }

  @Override
  public boolean add(T item) {
    throw new UnsupportedOperationException(UNMODIFIABLE_MESSAGE);
  }

  @Override
  public boolean remove(Object o) {
    throw new UnsupportedOperationException(UNMODIFIABLE_MESSAGE);
  }

  @Override
  public boolean containsAll(Collection<?> c) {
    return pageItems.containsAll(c);
  }

  @Override
  public boolean addAll(Collection<? extends T> c) {
    throw new UnsupportedOperationException(UNMODIFIABLE_MESSAGE);
  }

  @Override
  public boolean addAll(int index, Collection<? extends T> c) {
    throw new UnsupportedOperationException(UNMODIFIABLE_MESSAGE);
  }

  @Override
  public boolean removeAll(Collection<?> c) {
    throw new UnsupportedOperationException(UNMODIFIABLE_MESSAGE);
  }

  @Override
  public boolean retainAll(Collection<?> c) {
    throw new UnsupportedOperationException(UNMODIFIABLE_MESSAGE);
  }

  @Override
  public void clear() {
    throw new UnsupportedOperationException(UNMODIFIABLE_MESSAGE);
  }

  @Override
  public T get(int index) {
    return pageItems.get(index);
  }

  @Override
  public T set(int index, T element) {
    throw new UnsupportedOperationException(UNMODIFIABLE_MESSAGE);
  }

  @Override
  public void add(int index, T element) {
    throw new UnsupportedOperationException(UNMODIFIABLE_MESSAGE);
  }

  @Override
  public T remove(int index) {
    throw new UnsupportedOperationException(UNMODIFIABLE_MESSAGE);
  }

  @Override
  public int indexOf(Object o) {
    int indexOf = pageItems.indexOf(o);
    if (indexOf >= 0) {
      return indexOf;
    }
    return -1;
  }

  @Override
  public int lastIndexOf(Object o) {
    return pageItems.lastIndexOf(o);
  }

  @Override
  public ListIterator<T> listIterator() {
    throw new UnsupportedOperationException("ListIterators are not supported for this list");
  }

  @Override
  public ListIterator<T> listIterator(int index) {
    throw new UnsupportedOperationException("ListIterators are not supported for this list");
  }

  @Override
  public List<T> subList(int fromIndex, int toIndex) {
    return Collections.unmodifiableList(pageItems.subList(fromIndex, toIndex));
  }

  @Override
  public Spliterator<T> spliterator() {
    // Required to override, else encounter: Accept exceeded fixed size of 0
    return Spliterators.spliteratorUnknownSize(this.iterator(), Spliterator.ORDERED);
  }
}
