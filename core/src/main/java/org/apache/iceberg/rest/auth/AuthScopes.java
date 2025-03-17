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
package org.apache.iceberg.rest.auth;

import java.util.Map;
import javax.annotation.Nonnull;
import org.apache.iceberg.catalog.SessionCatalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.rest.RESTUtil;
import org.immutables.value.Value;

/** Well-known scopes for authentication. */
@Value.Enclosing
public interface AuthScopes {

  /**
   * A scope that represents the initial authentication request from the catalog to the config
   * endpoint. This is a short-lived scope that has no parent and is never cached.
   */
  @Value.Immutable
  abstract class Initial implements AuthScope {

    @Override
    public abstract Map<String, String> properties();

    @Override
    public final AuthSession parent() {
      return null;
    }

    @Override
    public final boolean cacheable() {
      return false;
    }

    @Override
    public final Initial withParent(AuthSession parent) {
      return this;
    }

    public static Initial of(Map<String, String> properties) {
      return ImmutableAuthScopes.Initial.builder().properties(properties).build();
    }
  }

  /**
   * A scope that represents the catalog. The catalog scope is long-lived, has no parent and is
   * never cached.
   */
  @Value.Immutable
  abstract class Catalog implements AuthScope {

    @Override
    public abstract Map<String, String> properties();

    @Override
    public final AuthSession parent() {
      return null;
    }

    @Override
    public final boolean cacheable() {
      return false;
    }

    @Override
    public final Catalog withParent(AuthSession parent) {
      return this;
    }

    public static Catalog of(Map<String, String> properties) {
      return ImmutableAuthScopes.Catalog.builder().properties(properties).build();
    }
  }

  /**
   * A scope that represents a {@link SessionCatalog.SessionContext}. It is always cached. Its
   * mandatory parent is expected to be in {@link Catalog} scope.
   */
  @Value.Immutable
  abstract class Contextual implements AuthScope {

    public abstract SessionCatalog.SessionContext context();

    @Value.Lazy
    @Override
    public Map<String, String> properties() {
      Map<String, String> properties =
          context().properties() == null ? Map.of() : context().properties();
      Map<String, String> credentials =
          context().credentials() == null ? Map.of() : context().credentials();
      return RESTUtil.merge(properties, credentials);
    }

    @Override
    @Nonnull
    public abstract AuthSession parent();

    @Override
    public final boolean cacheable() {
      return true;
    }

    public static Contextual of(SessionCatalog.SessionContext context, AuthSession parent) {
      return ImmutableAuthScopes.Contextual.builder().context(context).parent(parent).build();
    }
  }

  /**
   * A scope that represents a table or view. It is always cached. Its mandatory parent is expected
   * to be in {@link Contextual} scope.
   */
  @Value.Immutable
  abstract class Table implements AuthScope {

    public abstract TableIdentifier identifier();

    @Override
    public abstract Map<String, String> properties();

    @Override
    @Nonnull
    public abstract AuthSession parent();

    @Override
    public final boolean cacheable() {
      return true;
    }

    public static Table of(
        TableIdentifier identifier, Map<String, String> properties, AuthSession parent) {
      return ImmutableAuthScopes.Table.builder()
          .identifier(identifier)
          .properties(properties)
          .parent(parent)
          .build();
    }
  }

  /**
   * A general-purpose scope. Such scopes are used in components like request signers or credential
   * refreshers. Standalone scopes by default are not cached, and have no parent.
   */
  @Value.Immutable
  abstract class Standalone implements AuthScope {

    @Override
    public abstract Map<String, String> properties();

    @Value.Default
    @Override
    public boolean cacheable() {
      return false;
    }

    public static Standalone of(Map<String, String> properties) {
      return ImmutableAuthScopes.Standalone.builder().properties(properties).build();
    }
  }
}
