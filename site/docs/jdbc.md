# Iceberg JDBC Integration

## JDBC Catalog

Iceberg supports using a table in a relational database to manage Iceberg tables through JDBC.
The database that JDBC connects to must support atomic transaction to allow the JDBC catalog implementation to 
properly support atomic Iceberg table commits and read serializable isolation.

### Configurations

Because each database and database service provider might require different configurations,
the JDBC catalog allows arbitrary configurations through:

| Property             | Default                           | Description                                            |
| -------------------- | --------------------------------- | ------------------------------------------------------ |
| uri                  |                                   | the JDBC connection string |
| jdbc.<property_key\> |                                   | any key value pairs to configure the JDBC connection | 

For example, you can start a Spark session with a MySQL JDBC connection using the following configurations:

```shell
spark-sql --packages org.apache.iceberg:iceberg-spark3-runtime:{{ versions.iceberg }} \
    --conf spark.sql.catalog.my_catalog=org.apache.iceberg.spark.SparkCatalog \
    --conf spark.sql.catalog.my_catalog.warehouse=s3://my-bucket/my/key/prefix \
    --conf spark.sql.catalog.my_catalog.catalog-impl=org.apache.iceberg.jdbc.JdbcCatalog \
    --conf spark.sql.catalog.my_catalog.uri=mysql://test.1234567890.us-west-2.rds.amazonaws.com:3306/default \
    --conf spark.sql.catalog.my_catalog.jdbc.verifyServerCertificate=true \
    --conf spark.sql.catalog.my_catalog.jdbc.useSSL=true \
    --conf spark.sql.catalog.my_catalog.jdbc.user=admin \
    --conf spark.sql.catalog.my_catalog.jdbc.password=pass
```