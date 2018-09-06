package com.netflix.iceberg;

import com.netflix.iceberg.metastore.HiveTables;
import com.netflix.iceberg.types.Types;
import org.apache.hadoop.conf.Configuration;

import static com.netflix.iceberg.types.Types.NestedField.required;

/**
 * Created by parth on 8/15/18.
 */
public class TestHiveTables
{

  public static void main(String[] args) {
    final Configuration configuration = new Configuration();
    configuration.set("metastore.thrift.uris", "thrift://metacat.dynprod.netflix.net:12001");
    configuration.set("hive.metastore.warehouse.dir", "s3n://netflix-dataoven-prod-users/hive/warehouse");
    final HiveTables tables = new HiveTables(configuration, "prodhive");
    Schema schema = new Schema(Types.StructType.of(
            required(100, "id", Types.LongType.get())).fields());
    tables.create(schema, "pbrahmbhatt", "hivetable");
  }
}
