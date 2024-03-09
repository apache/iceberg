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

import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.projectnessie.client.api.NessieApiV1;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.Branch;
import org.projectnessie.model.Reference;

class UpdateableReference {

  private Reference reference;
  private final boolean mutable;

  /**
   * Construct a new {@link UpdateableReference} using a Nessie reference object and a flag whether
   * an explicit hash was used to create the reference object.
   */
  UpdateableReference(Reference reference, boolean hashReference) {
    this.reference = reference;
    this.mutable = reference instanceof Branch && !hashReference;
  }

  public boolean refresh(NessieApiV1 api) throws NessieNotFoundException {
    if (!mutable) {
      return false;
    }
    Reference oldReference = reference;
    reference = api.getReference().refName(reference.getName()).get();
    return !oldReference.equals(reference);
  }

  public void updateReference(Reference ref) {
    Preconditions.checkState(mutable, "Hash references cannot be updated.");
    this.reference = Preconditions.checkNotNull(ref, "ref is null");
  }

  public String getHash() {
    return reference.getHash();
  }

  public Reference getReference() {
    return reference;
  }

  public void checkMutable() {
    Preconditions.checkArgument(
        mutable,
        "You can only mutate tables/views when using a branch without a hash or timestamp.");
  }

  public String getName() {
    return reference.getName();
  }

  public boolean isMutable() {
    return mutable;
  }
}
