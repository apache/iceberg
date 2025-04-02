package org.apache.iceberg.vortex;

import java.util.Map;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;

public class VortexAzureProperties implements ObjectStoreProperties {
  private static final String ACCOUNT_KEY = "azure_storage_account_key";
  private static final String SAS_KEY = "azure_storage_sas_key";
  private static final String SKIP_SIGNATURE = "azure_skip_signature";

  private final Map<String, String> properties = Maps.newHashMap();

  public VortexAzureProperties setAccessKey(String accountKey) {
    properties.put(ACCOUNT_KEY, accountKey);
    return this;
  }

  public VortexAzureProperties setSasKey(String sasKey) {
    properties.put(SAS_KEY, sasKey);
    return this;
  }

  public VortexAzureProperties setSkipSignature(boolean skipSignature) {
    properties.put(SKIP_SIGNATURE, String.valueOf(skipSignature));
    return this;
  }

  @Override
  public Map<String, String> asProperties() {
    return ImmutableMap.copyOf(properties);
  }
}
