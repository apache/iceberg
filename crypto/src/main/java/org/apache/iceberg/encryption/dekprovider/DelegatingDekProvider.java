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

package org.apache.iceberg.encryption.dekprovider;

import java.util.Map;
import java.util.Objects;
import org.apache.iceberg.encryption.Dek;
import org.apache.iceberg.encryption.KekId;
import org.apache.iceberg.util.conf.Conf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DelegatingDekProvider
    extends DekProvider<DelegatingDekProvider.DelegateKekId<? extends KekId>> {
  private static final Logger log = LoggerFactory.getLogger(DelegatingDekProvider.class);
  public static final String PROVIDER = "provider";

  private final Map<String, DekProvider<? extends KekId>> providers;
  private final String defaultProviderName;

  public DelegatingDekProvider(
      String defaultProviderName, Map<String, DekProvider<? extends KekId>> providers) {
    if (!providers.containsKey(defaultProviderName)) {
      throw new IllegalStateException(
          String.format("Could not find a provider for default provider %s", defaultProviderName));
    }
    this.providers = providers;
    this.defaultProviderName = defaultProviderName;
  }

  @Override
  public Dek getNewDek(DelegateKekId<? extends KekId> delegateKekId, int dekLength, int ivLength) {
    return getNewDekTyped(delegateKekId, dekLength, ivLength);
  }

  private <T extends KekId> Dek getNewDekTyped(
      DelegateKekId<T> delegateKekId, int dekLength, int ivLength) {
    return delegateKekId.provider().getNewDek(delegateKekId.kekId(), dekLength, ivLength);
  }

  @Override
  public Dek getPlaintextDek(DelegateKekId<? extends KekId> delegateKekId, Dek dek) {
    return getPlaintextDekTyped(delegateKekId, dek);
  }

  private <T extends KekId> Dek getPlaintextDekTyped(DelegateKekId<T> delegateKekId, Dek dek) {
    return delegateKekId.provider().getPlaintextDek(delegateKekId.kekId(), dek);
  }

  @Override
  public DelegateKekId<? extends KekId> loadKekId(Conf conf) {
    if (!conf.containsKey(PROVIDER)) {
      log.warn("Could not find provider for kek id. Assuming default provider");
    }
    String providerName = conf.propertyAsString(PROVIDER, defaultProviderName);
    DekProvider<? extends KekId> dekProvider = providers.get(providerName);
    if (dekProvider == null) {
      throw new IllegalArgumentException(
          String.format("Tried to load kek id for unknown dek provider %s", providerName));
    }
    return new DelegateKekId<>(providerName, dekProvider, conf);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (this.getClass() != o.getClass()) {
      return false;
    }
    DelegatingDekProvider that = (DelegatingDekProvider) o;
    return providers.equals(that.providers) && defaultProviderName.equals(that.defaultProviderName);
  }

  @Override
  public int hashCode() {
    return Objects.hash(providers, defaultProviderName);
  }

  public static class DelegateKekId<T extends KekId> implements KekId {
    /** * PRIVATE VARIABLES ** */
    private final String providerName;

    private final DekProvider<T> provider;
    private final T kekId;

    /** * CONSTRUCTOR ** */
    public DelegateKekId(String providerName, DekProvider<T> provider, Conf conf) {
      this.providerName = providerName;
      this.provider = provider;
      this.kekId = provider.loadKekId(conf);
    }

    /** * CONSTRUCTOR ** */
    public DelegateKekId(String providerName, DekProvider<T> provider, T kekId) {
      this.providerName = providerName;
      this.provider = provider;
      this.kekId = kekId;
    }

    /** * DUMPER ** */
    @Override
    public void dump(Conf conf) {
      conf.setString(PROVIDER, providerName);
      kekId.dump(conf);
    }

    /** * GETTERS ** */
    public String providerName() {
      return providerName;
    }

    public DekProvider<T> provider() {
      return provider;
    }

    public T kekId() {
      return kekId;
    }

    /** * EQUALS HASH CODE ** */
    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (this.getClass() != o.getClass()) {
        return false;
      }
      DelegateKekId<?> that = (DelegateKekId<?>) o;
      return providerName.equals(that.providerName) &&
          provider.equals(that.provider) &&
          kekId.equals(that.kekId);
    }

    @Override
    public int hashCode() {
      return Objects.hash(providerName, provider, kekId);
    }

    @Override
    public String toString() {
      return "DelegateKekId{" +
          "providerName='" + providerName + '\'' +
          ", provider=" + provider +
          ", kekId=" + kekId + '}';
    }
  }
}
