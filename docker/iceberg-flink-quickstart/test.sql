-- =============================================================================
-- Iceberg Flink Quickstart Test Script
-- =============================================================================
--
-- Prerequisites:
--   docker compose -f docker/iceberg-flink-quickstart/docker-compose.yml up -d --build
--   docker exec -it jobmanager ./bin/sql-client.sh
--
-- Then paste this script or run line by line
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 1. Create the Iceberg REST catalog
-- -----------------------------------------------------------------------------
CREATE CATALOG iceberg_catalog WITH (
  'type'                 = 'iceberg',
  'catalog-impl'         = 'org.apache.iceberg.rest.RESTCatalog',
  'uri'                  = 'http://iceberg-rest:8181',
  'warehouse'            = 's3://warehouse/',
  'io-impl'              = 'org.apache.iceberg.aws.s3.S3FileIO',
  's3.endpoint'          = 'http://minio:9000',
  's3.access-key-id'     = 'admin',
  's3.secret-access-key' = 'password',
  's3.path-style-access' = 'true'
);

-- -----------------------------------------------------------------------------
-- 2. Create a database and table
-- -----------------------------------------------------------------------------
CREATE DATABASE IF NOT EXISTS iceberg_catalog.nyc;

CREATE TABLE iceberg_catalog.nyc.taxis (
    vendor_id BIGINT,
    trip_id BIGINT,
    trip_distance FLOAT,
    fare_amount DOUBLE,
    store_and_fwd_flag STRING
);

-- -----------------------------------------------------------------------------
-- 3. Enable checkpointing (required for Iceberg commits)
-- -----------------------------------------------------------------------------
SET 'execution.checkpointing.interval' = '10s';

-- -----------------------------------------------------------------------------
-- 4. Insert data
-- -----------------------------------------------------------------------------
INSERT INTO iceberg_catalog.nyc.taxis
VALUES
    (1, 1000371, 1.8, 15.32, 'N'),
    (2, 1000372, 2.5, 22.15, 'N'),
    (2, 1000373, 0.9, 9.01, 'N'),
    (1, 1000374, 8.4, 42.13, 'Y');

-- -----------------------------------------------------------------------------
-- 5. Query the data
-- -----------------------------------------------------------------------------
SET 'sql-client.execution.result-mode' = 'tableau';
SELECT * FROM iceberg_catalog.nyc.taxis;

-- -----------------------------------------------------------------------------
-- 6. Inspect Iceberg metadata
-- -----------------------------------------------------------------------------
-- Snapshots
SELECT * FROM iceberg_catalog.nyc.`taxis$snapshots`;

-- Data files
SELECT content, file_path, file_format, record_count
FROM iceberg_catalog.nyc.`taxis$files`;

-- History
SELECT * FROM iceberg_catalog.nyc.`taxis$history`;

-- -----------------------------------------------------------------------------
-- 7. Cleanup (optional)
-- -----------------------------------------------------------------------------
DROP TABLE iceberg_catalog.nyc.taxis;
DROP DATABASE iceberg_catalog.nyc;
