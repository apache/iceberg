package com.netflix.iceberg.metastore;

import com.netflix.iceberg.Schema;
import com.netflix.iceberg.types.Types;
import org.apache.hadoop.conf.Configuration;

import static com.netflix.iceberg.types.Types.NestedField.required;

/**
 * Created by parth on 8/15/18.
 */
public class Test {

  public static void main(String[] args) {
    final Configuration configuration = new Configuration();
    configuration.set("metastore.thrift.uris", "thrift://localhost:9083");
    configuration.set("hive.metastore.warehouse.dir", "s3n://netflix-dataoven-prod-users/hive/warehouse");
    final HiveTables tables = new HiveTables(configuration, "prodhive");
    Schema schema = new Schema(Types.StructType.of(
            required(100, "id", Types.LongType.get())).fields());
    tables.create(schema, "pbrahmbhatt", "hivetabletest");
  }
}
