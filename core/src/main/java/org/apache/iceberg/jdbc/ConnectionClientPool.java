package org.apache.iceberg.jdbc;

import java.io.Closeable;
import java.sql.Connection;
import java.sql.SQLException;
import org.apache.iceberg.ClientPool;

public interface ConnectionClientPool extends Closeable, ClientPool<Connection, SQLException> {
}
