package org.apache.iceberg.flink.sink.dynamic;

import static org.assertj.core.api.Assertions.assertThat;

import java.net.URI;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.flink.table.data.GenericRowData;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.aws.s3.S3FileIO;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SessionCatalog;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.rest.RESTCatalog;
import org.apache.iceberg.rest.RESTCatalogAdapter;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MinIOContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Configuration;

@Testcontainers
class TestDynamicTableUpdateOperatorWithS3FileIO {

  private static final String BUCKET = "iceberg-test-bucket";
  private static final String MINIO_IMAGE = "minio/minio:RELEASE.2023-09-04T19-57-37Z";

  @Container
  private static final MinIOContainer MINIO =
          new MinIOContainer(MINIO_IMAGE)
                  .withUserName("testuser")
                  .withPassword("testpassword");

  private static final Schema SCHEMA =
          new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get()));

  private DynamicTableUpdateOperator operator;
  private RESTCatalog restCatalog;
  private S3Client s3Client;

  @BeforeEach
  void setUp() throws Exception {
    // 1. Cria S3Client manualmente apontando para o MinIO
    s3Client =
            S3Client.builder()
                    .endpointOverride(URI.create(MINIO.getS3URL()))
                    .credentialsProvider(
                            StaticCredentialsProvider.create(
                                    AwsBasicCredentials.create(
                                            MINIO.getUserName(), MINIO.getPassword())))
                    .region(Region.US_EAST_1)
                    .serviceConfiguration(
                            S3Configuration.builder()
                                    .pathStyleAccessEnabled(true)
                                    .build())
                    .build();

    s3Client.createBucket(b -> b.bucket(BUCKET));

    // 2. Propriedades do S3FileIO apontando para o MinIO
    Map<String, String> s3Props = new HashMap<>();
    s3Props.put("s3.endpoint", MINIO.getS3URL());
    s3Props.put("s3.access-key-id", MINIO.getUserName());
    s3Props.put("s3.secret-access-key", MINIO.getPassword());
    s3Props.put("s3.path-style-access", "true");
    s3Props.put("client.region", "us-east-1");

    // 3. Backend InMemory configurado com S3FileIO
    InMemoryCatalog backendCatalog = new InMemoryCatalog();
    Map<String, String> backendProps = new HashMap<>(s3Props);
    backendProps.put(CatalogProperties.WAREHOUSE_LOCATION, "s3://" + BUCKET + "/warehouse");
    backendProps.put(CatalogProperties.FILE_IO_IMPL, S3FileIO.class.getName());
    backendCatalog.initialize("backend", backendProps);
    backendCatalog.createNamespace(Namespace.of("default"));

    // 4. RESTCatalog com adapter direto (sem HTTP server)
    RESTCatalogAdapter adapter = new RESTCatalogAdapter(backendCatalog);
    restCatalog =
            new RESTCatalog(SessionCatalog.SessionContext.createEmpty(), properties -> adapter);
    restCatalog.initialize("test-rest", new HashMap<>(backendProps));

    // 5. CatalogLoader
    CatalogLoader restCatalogLoader =
            new CatalogLoader() {
              @Override
              public Catalog loadCatalog() {
                return restCatalog;
              }

              @Override
              public CatalogLoader clone() {
                try {
                  return (CatalogLoader) super.clone();
                } catch (CloneNotSupportedException e) {
                  return this;
                }
              }
            };

    // 6. Operator
    operator =
            new DynamicTableUpdateOperator(
                    restCatalogLoader, 10, 1000, 10, TableCreator.DEFAULT, true, false);
  }

  @AfterEach
  void tearDown() throws Exception {
    if (operator != null) {
      operator.close();
    }
    if (restCatalog != null) {
      restCatalog.close();
    }
    if (s3Client != null) {
      s3Client.close();
    }
  }

  @Test
  void testOperatorComS3FileIONormal() throws Exception {
    // ✅ operator.close() chamado no @AfterEach — sem warning de unclosed S3FileIO
    operator.open(null);

    DynamicRecordInternal input =
            new DynamicRecordInternal(
                    "default.test_table",
                    "main",
                    SCHEMA,
                    GenericRowData.of(1),
                    PartitionSpec.unpartitioned(),
                    42,
                    false,
                    Collections.emptySet());

    DynamicRecordInternal output = operator.map(input);
    assertThat(output).isEqualTo(input);
  }

  @Test
  void testUnclosedS3FileIOLeak() throws Exception {
    // ❌ operator NÃO é fechado — deve aparecer no log:
    // WARN S3FileIO - Unclosed S3FileIO instance created by:
    operator.open(null);

    DynamicRecordInternal input =
            new DynamicRecordInternal(
                    "default.leak_table",
                    "main",
                    SCHEMA,
                    GenericRowData.of(99),
                    PartitionSpec.unpartitioned(),
                    1,
                    false,
                    Collections.emptySet());

    operator.map(input);

    // Simula o leak — não fecha o operator
    operator = null;
    System.gc();
    Thread.sleep(1000);
  }
}