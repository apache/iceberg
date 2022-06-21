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

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.io.StringWriter;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import javax.annotation.Nullable;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.common.DynMethods;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.util.JsonUtil;
import org.projectnessie.client.NessieClientBuilder;
import org.projectnessie.client.http.HttpClientBuilder;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.IcebergTable;
import org.projectnessie.model.ImmutableCommitMeta;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class NessieUtil {

  private static final Logger LOG = LoggerFactory.getLogger(NessieUtil.class);

  public static final String NESSIE_CONFIG_PREFIX = "nessie.";
  static final String APPLICATION_TYPE = "application-type";

  private NessieUtil() {
  }

  static TableIdentifier removeCatalogName(TableIdentifier to, String name) {

    String[] levels = to.namespace().levels();
    // check if the identifier includes the catalog name and remove it
    if (levels.length >= 2 && name.equalsIgnoreCase(to.namespace().level(0))) {
      Namespace trimmedNamespace = Namespace.of(Arrays.copyOfRange(levels, 1, levels.length));
      return TableIdentifier.of(trimmedNamespace, to.name());
    }

    // return the original unmodified
    return to;
  }

  static ContentKey toKey(TableIdentifier tableIdentifier) {
    List<String> identifiers = Lists.newArrayList();
    if (tableIdentifier.hasNamespace()) {
      identifiers.addAll(Arrays.asList(tableIdentifier.namespace().levels()));
    }
    identifiers.add(tableIdentifier.name());

    return ContentKey.of(identifiers);
  }

  static CommitMeta buildCommitMetadata(String commitMsg, Map<String, String> catalogOptions) {
    return catalogOptions(CommitMeta.builder().message(commitMsg), catalogOptions).build();
  }

  static ImmutableCommitMeta.Builder catalogOptions(ImmutableCommitMeta.Builder commitMetaBuilder,
      Map<String, String> catalogOptions) {
    Preconditions.checkArgument(null != catalogOptions, "catalogOptions must not be null");
    commitMetaBuilder.author(NessieUtil.commitAuthor(catalogOptions));
    commitMetaBuilder.putProperties(APPLICATION_TYPE, "iceberg");
    if (catalogOptions.containsKey(CatalogProperties.APP_ID)) {
      commitMetaBuilder.putProperties(CatalogProperties.APP_ID, catalogOptions.get(CatalogProperties.APP_ID));
    }
    return commitMetaBuilder;
  }

  /**
   * @param catalogOptions The options where to look for the <b>user</b>
   * @return The author that can be used for a commit, which is either the <b>user</b> from the given
   * <code>catalogOptions</code> or the logged in user as defined in the <b>user.name</b> JVM properties.
   */
  @Nullable
  private static String commitAuthor(Map<String, String> catalogOptions) {
    return Optional.ofNullable(catalogOptions.get(CatalogProperties.USER))
        .orElseGet(() -> System.getProperty("user.name"));
  }

  static TableMetadata tableMetadataFromIcebergTable(FileIO io, IcebergTable table, String metadataLocation) {
    TableMetadata deserialized;
    if (table.getMetadata() != null) {
      String jsonString;
      try (StringWriter writer = new StringWriter()) {
        try (JsonGenerator generator = JsonUtil.factory().createGenerator(writer)) {
          generator.writeObject(table.getMetadata().getMetadata());
        }
        jsonString = writer.toString();
      } catch (IOException e) {
        throw new RuntimeException("Failed to generate JSON string from metadata", e);
      }
      deserialized = TableMetadataParser.fromJson(io, metadataLocation, jsonString);
    } else {
      deserialized = TableMetadataParser.read(io, metadataLocation);
    }
    return deserialized;
  }

  static JsonNode tableMetadataAsJsonNode(TableMetadata metadata) {
    JsonNode newMetadata;
    try {
      String jsonString = TableMetadataParser.toJson(metadata);
      try (JsonParser parser = JsonUtil.factory().createParser(jsonString)) {
        newMetadata = parser.readValueAs(JsonNode.class);
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return newMetadata;
  }

  static NessieClientBuilder<?> createNessieClientBuilder(String customBuilder) {
    NessieClientBuilder<?> clientBuilder;
    if (customBuilder != null) {
      try {
        clientBuilder = DynMethods.builder("builder").impl(customBuilder).build().asStatic().invoke();
      } catch (Exception e) {
        throw new RuntimeException(String.format("Failed to use custom NessieClientBuilder '%s'.", customBuilder), e);
      }
    } else {
      clientBuilder = HttpClientBuilder.builder();
    }
    return clientBuilder;
  }

  static String validateWarehouseLocation(String name, Map<String, String> catalogOptions) {
    String warehouseLocation = catalogOptions.get(CatalogProperties.WAREHOUSE_LOCATION);
    if (warehouseLocation == null) {
      // Explicitly log a warning, otherwise the thrown exception can get list in the "silent-ish catch"
      // in o.a.i.spark.Spark3Util.catalogAndIdentifier(o.a.s.sql.SparkSession, List<String>,
      //     o.a.s.sql.connector.catalog.CatalogPlugin)
      // in the code block
      //    Pair<CatalogPlugin, Identifier> catalogIdentifier = SparkUtil.catalogAndIdentifier(nameParts,
      //        catalogName ->  {
      //          try {
      //            return catalogManager.catalog(catalogName);
      //          } catch (Exception e) {
      //            return null;
      //          }
      //        },
      //        Identifier::of,
      //        defaultCatalog,
      //        currentNamespace
      //    );
      LOG.warn("Catalog creation for inputName={} and options {} failed, because parameter " +
          "'warehouse' is not set, Nessie can't store data.", name, catalogOptions);
      throw new IllegalStateException("Parameter 'warehouse' not set, Nessie can't store data.");
    }
    return warehouseLocation;
  }
}
