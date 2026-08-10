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
package org.apache.iceberg.flink;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;
import org.apache.flink.streaming.api.lineage.DatasetConfigFacet;
import org.apache.flink.streaming.api.lineage.LineageDataset;
import org.apache.flink.streaming.api.lineage.LineageDatasetFacet;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;

class TestIcebergLineageUtil {

  private static final String CATALOG_ALIAS = "analytics_catalog";
  private static final String DB = "analytics";
  private static final String TABLE = "page_views";
  private static final TableIdentifier IDENTIFIER = TableIdentifier.of(DB, TABLE);
  private static final String FULL_TABLE_NAME = CATALOG_ALIAS + "." + IDENTIFIER;
  private static final String WAREHOUSE = "gs://warehouse-bucket";

  @Test
  void reportsIcebergCoordinatesWithoutVendorNaming() {
    LineageDataset dataset =
        datasetOf(
            IDENTIFIER,
            FULL_TABLE_NAME,
            ImmutableMap.of(CatalogProperties.WAREHOUSE_LOCATION, WAREHOUSE));

    assertThat(dataset.namespace()).isEqualTo(WAREHOUSE);
    assertThat(dataset.name()).isEqualTo(FULL_TABLE_NAME);

    assertThat(facetConfig(dataset))
        .containsEntry(IcebergLineageUtil.CONFIG_CATALOG, CATALOG_ALIAS)
        .containsEntry(IcebergLineageUtil.CONFIG_CATALOG_WAREHOUSE, WAREHOUSE)
        .containsEntry(IcebergLineageUtil.CONFIG_NAMESPACE, DB)
        .containsEntry(IcebergLineageUtil.CONFIG_TABLE, TABLE);
  }

  @Test
  void catalogIsNeverDerivedFromTheWarehouseBucket() {
    LineageDataset dataset =
        datasetOf(
            IDENTIFIER,
            FULL_TABLE_NAME,
            ImmutableMap.of(CatalogProperties.WAREHOUSE_LOCATION, "gs://some-bucket/some/prefix"));

    // A catalog can span many buckets, so a bucket-derived catalog name would be wrong.
    Map<String, String> config = facetConfig(dataset);
    assertThat(config)
        .containsEntry(IcebergLineageUtil.CONFIG_CATALOG_WAREHOUSE, "gs://some-bucket/some/prefix")
        .containsEntry(IcebergLineageUtil.CONFIG_CATALOG, CATALOG_ALIAS)
        .doesNotContainKey(IcebergLineageUtil.CONFIG_CATALOG_PREFIX);
  }

  @Test
  void multiLevelNamespaceIsReportedVerbatim() {
    // BigLake's four-part FQN assumes a single-level namespace, but Iceberg allows any depth.
    // Mapping a deeper one onto a vendor scheme is the listener's problem, not ours.
    TableIdentifier nested = TableIdentifier.of(Namespace.of("bronze", "events"), TABLE);
    String fullName = CATALOG_ALIAS + "." + nested;

    LineageDataset dataset = datasetOf(nested, fullName, ImmutableMap.of());

    assertThat(dataset.name()).isEqualTo(fullName);
    assertThat(facetConfig(dataset))
        .containsEntry(IcebergLineageUtil.CONFIG_NAMESPACE, "bronze.events")
        .containsEntry(IcebergLineageUtil.CONFIG_TABLE, TABLE)
        .containsEntry(IcebergLineageUtil.CONFIG_CATALOG, CATALOG_ALIAS);
  }

  @Test
  void catalogUriIsPreferredOverWarehouseAsTheDatasetNamespace() {
    String uri = "https://biglake.googleapis.com/iceberg/v1/restcatalog";

    LineageDataset dataset =
        datasetOf(
            IDENTIFIER,
            FULL_TABLE_NAME,
            ImmutableMap.of(
                CatalogProperties.URI, uri,
                CatalogProperties.WAREHOUSE_LOCATION, WAREHOUSE));

    assertThat(dataset.namespace()).isEqualTo(uri);
    assertThat(facetConfig(dataset)).containsEntry(IcebergLineageUtil.CONFIG_CATALOG_URI, uri);
  }

  @Test
  void fallsBackToAStableNamespaceWhenTheCatalogDeclaresNeither() {
    LineageDataset dataset = datasetOf(IDENTIFIER, FULL_TABLE_NAME, ImmutableMap.of());

    assertThat(dataset.namespace()).isEqualTo(IcebergLineageUtil.DEFAULT_NAMESPACE);
  }

  @Test
  void secretsInCatalogPropertiesAreNotPublished() {
    // Catalog properties routinely carry credentials, and the facet is forwarded off-cluster.
    LineageDataset dataset =
        datasetOf(
            IDENTIFIER,
            FULL_TABLE_NAME,
            ImmutableMap.of(
                CatalogProperties.WAREHOUSE_LOCATION,
                WAREHOUSE,
                "token",
                "super-secret-token",
                "credential",
                "client:secret",
                "header.x-goog-user-project",
                "some-gcp-project"));

    assertThat(facetConfig(dataset).keySet())
        .containsExactlyInAnyOrder(
            IcebergLineageUtil.CONFIG_CATALOG,
            IcebergLineageUtil.CONFIG_CATALOG_WAREHOUSE,
            IcebergLineageUtil.CONFIG_NAMESPACE,
            IcebergLineageUtil.CONFIG_TABLE);
  }

  @Test
  void noDatasetForPathBasedTablesThatHaveNoCatalog() {
    TableLoader loader = TableLoader.fromHadoopTable("file:///tmp/does-not-need-to-exist");

    assertThat(IcebergLineageUtil.datasetsOf(loader, FULL_TABLE_NAME)).isEmpty();
  }

  @Test
  void noCatalogKeyWhenTheTableNameDoesNotEndWithTheIdentifier() {
    LineageDataset dataset = datasetOf(IDENTIFIER, "unrelated_name", ImmutableMap.of());

    assertThat(facetConfig(dataset))
        .doesNotContainKey(IcebergLineageUtil.CONFIG_CATALOG)
        .containsEntry(IcebergLineageUtil.CONFIG_NAMESPACE, DB)
        .containsEntry(IcebergLineageUtil.CONFIG_TABLE, TABLE);
  }

  @Test
  void unreachableRestCatalogCostsOnlyThePrefix() {
    CountingRestCatalogLoader catalogLoader = new CountingRestCatalogLoader();
    TableLoader loader = TableLoader.fromCatalog(catalogLoader, IDENTIFIER);

    List<LineageDataset> datasets = IcebergLineageUtil.datasetsOf(loader, FULL_TABLE_NAME);

    // The rest of the coordinates are known locally, so they are still reported.
    assertThat(datasets).hasSize(1);
    assertThat(facetConfig(datasets.get(0)))
        .doesNotContainKey(IcebergLineageUtil.CONFIG_CATALOG_PREFIX)
        .containsEntry(IcebergLineageUtil.CONFIG_CATALOG, CATALOG_ALIAS)
        .containsEntry(IcebergLineageUtil.CONFIG_NAMESPACE, DB)
        .containsEntry(IcebergLineageUtil.CONFIG_TABLE, TABLE);
    assertThat(catalogLoader.loads).isEqualTo(1);
  }

  @Test
  void aCapturedPrefixIsReportedWithoutOpeningACatalog() {
    // The point of capturing: lineage costs the submission path no catalog initialization. The
    // loader here fails if opened, so reaching the prefix at all proves none was.
    CountingRestCatalogLoader catalogLoader = new CountingRestCatalogLoader();
    TableLoader loader = TableLoader.fromCatalog(catalogLoader, IDENTIFIER);

    List<LineageDataset> datasets =
        IcebergLineageUtil.datasetsOf(loader, FULL_TABLE_NAME, "projects/1234/catalogs/analytics");

    assertThat(facetConfig(datasets.get(0)))
        .containsEntry(
            IcebergLineageUtil.CONFIG_CATALOG_PREFIX, "projects/1234/catalogs/analytics");
    assertThat(catalogLoader.loads).isZero();
  }

  @Test
  void aCatalogThatDeclaredNoPrefixIsNotAskedTwice() {
    // NO_REST_PREFIX means a live catalog already answered "none". Falling back here would open a
    // catalog per submission, forever, to re-learn the same nothing.
    CountingRestCatalogLoader catalogLoader = new CountingRestCatalogLoader();
    TableLoader loader = TableLoader.fromCatalog(catalogLoader, IDENTIFIER);

    List<LineageDataset> datasets =
        IcebergLineageUtil.datasetsOf(loader, FULL_TABLE_NAME, IcebergLineageUtil.NO_REST_PREFIX);

    assertThat(facetConfig(datasets.get(0)))
        .doesNotContainKey(IcebergLineageUtil.CONFIG_CATALOG_PREFIX)
        .containsEntry(IcebergLineageUtil.CONFIG_TABLE, TABLE);
    assertThat(catalogLoader.loads).isZero();
  }

  @Test
  void noPrefixIsCapturedFromALoaderThatIsNotOpen() {
    TableLoader loader = TableLoader.fromCatalog(new CountingRestCatalogLoader(), IDENTIFIER);

    // Null, not NO_REST_PREFIX: nothing was consulted, so nothing has been ruled out.
    assertThat(IcebergLineageUtil.restPrefixOf(loader)).isNull();
    assertThat(IcebergLineageUtil.restPrefixOf(TableLoader.fromHadoopTable("file:///tmp/nope")))
        .isNull();
  }

  /**
   * A REST-typed catalog that cannot be loaded — an unreachable or unauthorized endpoint — which
   * counts the attempts, so tests can assert that lineage opened no catalog at all.
   */
  private static class CountingRestCatalogLoader implements CatalogLoader {
    private int loads;

    @Override
    public Catalog loadCatalog() {
      loads++;
      throw new RuntimeException("catalog is unreachable");
    }

    @Override
    public Map<String, String> properties() {
      return ImmutableMap.of(
          CatalogUtil.ICEBERG_CATALOG_TYPE, CatalogUtil.ICEBERG_CATALOG_TYPE_REST);
    }

    @Override
    @SuppressWarnings({"checkstyle:NoClone", "checkstyle:SuperClone"})
    public CatalogLoader clone() {
      return this;
    }
  }

  private static LineageDataset datasetOf(
      TableIdentifier identifier, String fullTableName, Map<String, String> catalogProperties) {
    List<LineageDataset> datasets =
        IcebergLineageUtil.datasetsOf(tableLoader(identifier, catalogProperties), fullTableName);
    assertThat(datasets).hasSize(1);
    return datasets.get(0);
  }

  private static TableLoader tableLoader(
      TableIdentifier identifier, Map<String, String> catalogProperties) {
    // HadoopCatalogLoader only records its properties, and a non-REST catalog is never loaded, so
    // nothing here touches the filesystem or the network.
    return TableLoader.fromCatalog(
        CatalogLoader.hadoop(CATALOG_ALIAS, new Configuration(false), catalogProperties),
        identifier);
  }

  private static Map<String, String> facetConfig(LineageDataset dataset) {
    LineageDatasetFacet facet = dataset.facets().get(IcebergLineageUtil.FACET_NAME);
    assertThat(facet).isInstanceOf(DatasetConfigFacet.class);
    return ((DatasetConfigFacet) facet).config();
  }
}
