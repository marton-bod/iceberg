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

package org.apache.iceberg.mr.hive;

import java.io.File;
import java.io.IOException;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.hive.TestHiveMetastore;
import org.apache.iceberg.mr.TestHelper;
import org.apache.iceberg.types.Types;
import org.apache.thrift.TException;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.apache.iceberg.types.Types.NestedField.required;

/**
 * Creates tables and write data, using a real running HMS (under METASTORE_HOST).
 * Running this class will create a couple of Iceberg tables (TABLE_NAME_1 and TABLE_NAME_2),
 * and append some records into them, using Hive catalog. The actual data files will be under DATA_FILES_DIRECTORY.
 */
public class HiveCatalogTestWriter {

  // settings - change this to your local flavour
  public static final String METASTORE_HOST = "thrift://localhost:9083";
  public static final String TABLE_NAME_1 = "customers10";
  public static final String TABLE_NAME_2 = "orders10";
  public static final String DATA_FILES_DIRECTORY = "/tmp/hive/iceberg/hive_catalog/";

  @Test
  public void createTablesAndAppendRecords() throws IOException {
    createTable(TABLE_NAME_1, CUSTOMER_SCHEMA, CUSTOMER_RECORDS);
    createTable(TABLE_NAME_2, ORDER_SCHEMA, ORDER_RECORDS);
  }

  @Rule
  public TemporaryFolder temp = new NonDeletingFolder(new File(DATA_FILES_DIRECTORY)); // not so temporary

  private static final Schema CUSTOMER_SCHEMA = new Schema(
          required(1, "customer_id", Types.LongType.get()),
          required(2, "first_name", Types.StringType.get())
  );

  private static final List<Record> CUSTOMER_RECORDS = TestHelper.RecordsBuilder.newInstance(CUSTOMER_SCHEMA)
          .add(0L, "Alice")
          .add(1L, "Bob")
          .add(2L, "Trudy")
          .build();

  private static final Schema ORDER_SCHEMA = new Schema(
          required(1, "order_id", Types.LongType.get()),
          required(2, "customer_id", Types.LongType.get()),
          required(3, "total", Types.DoubleType.get()));

  private static final List<Record> ORDER_RECORDS = TestHelper.RecordsBuilder.newInstance(ORDER_SCHEMA)
          .add(100L, 0L, 11.11d)
          .add(101L, 0L, 22.22d)
          .add(102L, 1L, 33.33d)
          .build();

  private static final PartitionSpec SPEC = PartitionSpec.unpartitioned();

  // before variables
  protected TestHiveMetastore metastore;
  private TestTables testTables;

  @Before
  public void before() {
    metastore = new TestHiveMetastore();
    metastore.start();

    metastore.hiveConf().setVar(HiveConf.ConfVars.METASTOREURIS, METASTORE_HOST);
    metastore.hiveConf().set("fs.pfile.impl", "org.apache.hadoop.fs.ProxyLocalFileSystem");
    testTables = testTables(metastore.hiveConf(), temp);
  }

  @After
  public void after() {
    metastore.stop();
    metastore = null;
  }

  protected void createTable(String tableName, Schema schema, List<Record> records)
          throws IOException {
    Table table = createIcebergTable(tableName, schema, records);
    createHiveTable(tableName, table.location());
  }

  protected Table createIcebergTable(String tableName, Schema schema, List<Record> records)
          throws IOException {
    String identifier = testTables.identifier("default." + tableName);
    TestHelper helper = new TestHelper(
            metastore.hiveConf(), testTables.tables(), identifier, schema, SPEC, FileFormat.PARQUET, temp);
    Table table = helper.createTable();

    if (!records.isEmpty()) {
      helper.appendToTable(helper.writeFile(null, records));
    }

    return table;
  }

  public TestTables testTables(Configuration conf, TemporaryFolder temp) {
    return new TestTables.HiveTestTables(conf, temp);
  }

  protected void createHiveTable(String tableName, String location) {
    // The Hive catalog has already created the Hive table so there's no need to issue another
    // 'CREATE TABLE ...' statement. However, we still need to set up the storage handler properly,
    // which can't be done directly using the Hive DDL so we resort to the HMS API.
    try {
      IMetaStoreClient client = new HiveMetaStoreClient(metastore.hiveConf());
      org.apache.hadoop.hive.metastore.api.Table table = client.getTable("default", tableName);

      table.getParameters().put("storage_handler", HiveIcebergStorageHandler.class.getName());
      table.getSd().getSerdeInfo().setSerializationLib(HiveIcebergSerDe.class.getName());
      table.getSd().setInputFormat(null);
      table.getSd().setOutputFormat(null);

      client.alter_table("default", tableName, table);
    } catch (TException te) {
      throw new RuntimeException(te);
    }
  }

  static class NonDeletingFolder extends TemporaryFolder {
    public NonDeletingFolder(File root) {
      super(root);
    }

    @Override
    protected void after() { }
  }
}
