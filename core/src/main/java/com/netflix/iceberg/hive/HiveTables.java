package com.netflix.iceberg.hive;

import com.google.common.base.Splitter;
import com.netflix.iceberg.BaseMetastoreTableOperations;
import com.netflix.iceberg.BaseMetastoreTables;
import com.netflix.iceberg.PartitionSpec;
import com.netflix.iceberg.Schema;
import com.netflix.iceberg.Table;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.MetaException;

import java.util.List;

public class HiveTables extends BaseMetastoreTables {
  private static final Splitter DOT = Splitter.on('.').limit(2);

  public HiveTables(Configuration conf) {
    super(conf);
  }

  @Override
  public Table create(Schema schema, PartitionSpec spec, String tableIdentifier) {
    List<String> parts = DOT.splitToList(tableIdentifier);
    if (parts.size() == 2) {
      return create(schema, spec, parts.get(0), parts.get(1));
    }
    throw new UnsupportedOperationException("Could not parse table identifier: " + tableIdentifier);
  }

  @Override
  public Table load(String tableIdentifier) {
    List<String> parts = DOT.splitToList(tableIdentifier);
    if (parts.size() == 2) {
      return load(parts.get(0), parts.get(1));
    }
    throw new UnsupportedOperationException("Could not parse table identifier: " + tableIdentifier);
  }

  @Override
  public BaseMetastoreTableOperations newTableOps(Configuration conf, String database, String table) {
    return new HiveTableOperations(conf, getClient(), database, table);
  }

  private IMetaStoreClient getClient() {
    try {
      return new HiveMetaStoreClient(conf);
    } catch (MetaException e) {
      throw new RuntimeException(e);
    }
  }
}
