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

import java.util.function.Supplier;
import java.util.stream.Stream;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.projectnessie.client.NessieConfigConstants;
import org.projectnessie.client.api.NessieApiV1;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.Branch;
import org.projectnessie.model.Content;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.IcebergTable;
import org.projectnessie.model.Reference;
import org.projectnessie.model.Tag;

public class NessieIcebergClient implements AutoCloseable {

  private final NessieApiV1 api;
  private final Supplier<UpdateableReference> reference;

  public NessieIcebergClient(NessieApiV1 api, String requestedRef, String requestedHash) {
    this.api = api;
    this.reference = () -> loadReference(requestedRef, requestedHash);
  }

  public NessieApiV1 getApi() {
    return api;
  }

  public UpdateableReference getRef() {
    return reference.get();
  }

  public void refresh() throws NessieNotFoundException {
    getRef().refresh(api);
  }

  private UpdateableReference loadReference(String requestedRef, String hash) {
    try {
      Reference ref =
          requestedRef == null ? api.getDefaultBranch() : api.getReference().refName(requestedRef).get();
      if (hash != null) {
        if (ref instanceof Branch) {
          ref = Branch.of(ref.getName(), hash);
        } else {
          ref = Tag.of(ref.getName(), hash);
        }
      }
      return new UpdateableReference(ref, hash != null);
    } catch (NessieNotFoundException ex) {
      if (requestedRef != null) {
        throw new IllegalArgumentException(String.format("Nessie ref '%s' does not exist. This ref must exist " +
            "before creating a NessieCatalog.", requestedRef), ex);
      }

      throw new IllegalArgumentException(String.format("Nessie does not have an existing default branch. " +
              "Either configure an alternative ref via '%s' or create the default branch on the server.",
              NessieConfigConstants.CONF_NESSIE_REF), ex);
    }
  }

  public Stream<TableIdentifier> tableStream(Namespace namespace) {
    try {
      return api.getEntries().reference(getRef().getReference()).get().getEntries().stream()
          .filter(NessieUtil.namespacePredicate(namespace))
          .map(NessieUtil::toIdentifier);
    } catch (NessieNotFoundException ex) {
      throw new NoSuchNamespaceException(ex, "Unable to list tables due to missing ref '%s'", getRef().getName());
    }
  }

  public IcebergTable table(TableIdentifier tableIdentifier) {
    try {
      ContentKey key = NessieUtil.toKey(tableIdentifier);
      Content table = api.getContent().key(key).reference(getRef().getReference()).get().get(key);
      return table != null ? table.unwrap(IcebergTable.class).orElse(null) : null;
    } catch (NessieNotFoundException e) {
      return null;
    }
  }

  @Override
  public void close() {
    if (null != api) {
      api.close();
    }
  }
}
