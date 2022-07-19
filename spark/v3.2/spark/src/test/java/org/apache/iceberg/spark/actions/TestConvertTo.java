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

package org.apache.iceberg.spark.actions;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;
import org.apache.iceberg.actions.MigrateTable;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.spark.SparkCatalogTestBase;
import org.apache.iceberg.spark.source.LessSimpleRecord;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.connector.catalog.CatalogExtension;
import org.apache.spark.sql.connector.catalog.CatalogPlugin;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.delta.catalog.DeltaCatalog;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runners.Parameterized;

public class TestConvertTo extends SparkCatalogTestBase {
  private static final String CREATE_PARTITIONED_PARQUET =
      "CREATE TABLE %s (id INT, data STRING) " + "using delta PARTITIONED BY (id) LOCATION '%s'";
  private static final String CREATE_PARQUET = "CREATE TABLE %s (id INT, data STRING) " + "using parquet LOCATION '%s'";
  private static final String CREATE_HIVE_EXTERNAL_PARQUET =
      "CREATE EXTERNAL TABLE %s (data STRING) " + "PARTITIONED BY (id INT) STORED AS parquet LOCATION '%s'";
  private static final String CREATE_HIVE_PARQUET =
      "CREATE TABLE %s (data STRING) " + "PARTITIONED BY (id INT) STORED AS parquet";

  private static final String NAMESPACE = "default";

  @Parameterized.Parameters(name = "Catalog Name {0} - Options {2}")
  public static Object[][] parameters() {
    return new Object[][] {
        new Object[] {"delta", DeltaCatalog.class.getName(), ImmutableMap.of(
            "type", "hive",
            "default-namespace", "default",
            "parquet-enabled", "true",
            "cache-enabled", "false" // Spark will delete tables using v1, leaving the cache out of sync
        )}
    };
  }

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

  private String baseTableName = "baseTable";
  private File tableDir;
  private String tableLocation;
  private final String type;
  private TableCatalog catalog;

  private String catalogName;

  public TestConvertTo(String catalogName, String implementation, Map<String, String> config) {
    super(catalogName, implementation, config);
    this.catalog = (TableCatalog) spark.sessionState().catalogManager().catalog(catalogName);
    this.type = config.get("type");
    this.catalogName = catalogName;
  }

  @Before
  public void before() {
    try {
      this.tableDir = temp.newFolder();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    this.tableLocation = tableDir.toURI().toString();

    String tableIdentifier = destName(baseTableName);
    CatalogPlugin catalogPlugin = spark.sessionState().catalogManager().v2SessionCatalog();

    CatalogExtension delta = (CatalogExtension) spark.sessionState().catalogManager().catalog("delta");
    delta.setDelegateCatalog(spark.sessionState().catalogManager().currentCatalog());

    spark.sql(String.format("DROP TABLE IF EXISTS %s", tableIdentifier));
    spark.sessionState().catalogManager().v2SessionCatalog();

    ArrayList<LessSimpleRecord> lessSimpleRecords = Lists.newArrayList(
        new LessSimpleRecord(1, 4, "a"),
        new LessSimpleRecord(2, 5, "b"),
        new LessSimpleRecord(3, 6, "c"),
        new LessSimpleRecord(3, 6, "e"),
        new LessSimpleRecord(3, 4, "f"),
        new LessSimpleRecord(4, 4, "g")
    );

    Dataset<Row> df = spark.createDataFrame(lessSimpleRecords, LessSimpleRecord.class);

    this.catalog = (TableCatalog) spark.sessionState().catalogManager().catalog("delta");
    this.catalogName = "delta";

    df.write()
        .format("delta")
        .mode(SaveMode.Overwrite)
        .partitionBy("id", "secondId")
        .option("path", tableLocation)
        .saveAsTable(tableIdentifier);
  }

  @After
  public void after() throws IOException {
    // Drop the hive table.
    spark.sql(String.format("DROP TABLE IF EXISTS %s", baseTableName));
  }

  @Test
  public void testMigratePartitioned() throws Exception {
    System.out.println("Beginning...");

    MigrateTable.Result result = SparkActions.get().migrateTable(destName(baseTableName)).execute();
    System.out.println("End....");
  }

  // private void threeLevelList(boolean useLegacyMode) throws Exception {
  //   spark.conf().set("spark.sql.parquet.writeLegacyFormat", useLegacyMode);
  //
  //   String tableName = sourceName(String.format("threeLevelList_%s", useLegacyMode));
  //   File location = temp.newFolder();
  //   sql(
  //       "CREATE TABLE %s (col1 ARRAY<STRUCT<col2 INT>>)" + " STORED AS parquet" + " LOCATION '%s'",
  //       tableName,
  //       location);
  //
  //   int testValue = 12345;
  //   sql("INSERT INTO %s VALUES (ARRAY(STRUCT(%s)))", tableName, testValue);
  //   List<Object[]> expected = sql(String.format("SELECT * FROM %s", tableName));
  //
  //   // migrate table
  //   SparkActions.get().migrateTable(tableName).execute();
  //
  //   // check migrated table is returning expected result
  //   List<Object[]> results = sql("SELECT * FROM %s", tableName);
  //   Assert.assertTrue(results.size() > 0);
  //   assertEquals("Output must match", expected, results);
  // }
  //
  // private void threeLevelListWithNestedStruct(boolean useLegacyMode) throws Exception {
  //   spark.conf().set("spark.sql.parquet.writeLegacyFormat", useLegacyMode);
  //
  //   String tableName = sourceName(String.format("threeLevelListWithNestedStruct_%s", useLegacyMode));
  //   File location = temp.newFolder();
  //   sql(
  //       "CREATE TABLE %s (col1 ARRAY<STRUCT<col2 STRUCT<col3 INT>>>)" + " STORED AS parquet" + " LOCATION '%s'",
  //       tableName,
  //       location);
  //
  //   int testValue = 12345;
  //   sql("INSERT INTO %s VALUES (ARRAY(STRUCT(STRUCT(%s))))", tableName, testValue);
  //   List<Object[]> expected = sql(String.format("SELECT * FROM %s", tableName));
  //
  //   // migrate table
  //   SparkActions.get().migrateTable(tableName).execute();
  //
  //   // check migrated table is returning expected result
  //   List<Object[]> results = sql("SELECT * FROM %s", tableName);
  //   Assert.assertTrue(results.size() > 0);
  //   assertEquals("Output must match", expected, results);
  // }
  //
  // private void threeLevelLists(boolean useLegacyMode) throws Exception {
  //   spark.conf().set("spark.sql.parquet.writeLegacyFormat", useLegacyMode);
  //
  //   String tableName = sourceName(String.format("threeLevelLists_%s", useLegacyMode));
  //   File location = temp.newFolder();
  //   sql("CREATE TABLE %s (col1 ARRAY<STRUCT<col2 INT>>, col3 ARRAY<STRUCT<col4 INT>>)" + " STORED AS parquet" +
  //       " LOCATION '%s'", tableName, location);
  //
  //   int testValue1 = 12345;
  //   int testValue2 = 987654;
  //   sql("INSERT INTO %s VALUES (ARRAY(STRUCT(%s)), ARRAY(STRUCT(%s)))", tableName, testValue1, testValue2);
  //   List<Object[]> expected = sql(String.format("SELECT * FROM %s", tableName));
  //
  //   // migrate table
  //   SparkActions.get().migrateTable(tableName).execute();
  //
  //   // check migrated table is returning expected result
  //   List<Object[]> results = sql("SELECT * FROM %s", tableName);
  //   Assert.assertTrue(results.size() > 0);
  //   assertEquals("Output must match", expected, results);
  // }
  //
  // private void structOfThreeLevelLists(boolean useLegacyMode) throws Exception {
  //   spark.conf().set("spark.sql.parquet.writeLegacyFormat", useLegacyMode);
  //
  //   String tableName = sourceName(String.format("structOfThreeLevelLists_%s", useLegacyMode));
  //   File location = temp.newFolder();
  //   sql(
  //       "CREATE TABLE %s (col1 STRUCT<col2 ARRAY<STRUCT<col3 INT>>>)" + " STORED AS parquet" + " LOCATION '%s'",
  //       tableName,
  //       location);
  //
  //   int testValue1 = 12345;
  //   sql("INSERT INTO %s VALUES (STRUCT(STRUCT(ARRAY(STRUCT(%s)))))", tableName, testValue1);
  //   List<Object[]> expected = sql(String.format("SELECT * FROM %s", tableName));
  //
  //   // migrate table
  //   SparkActions.get().migrateTable(tableName).execute();
  //
  //   // check migrated table is returning expected result
  //   List<Object[]> results = sql("SELECT * FROM %s", tableName);
  //   Assert.assertTrue(results.size() > 0);
  //   assertEquals("Output must match", expected, results);
  // }
  //
  // private SparkTable loadTable(String name) throws NoSuchTableException, ParseException {
  //   return (SparkTable) catalog.loadTable(Spark3Util.catalogAndIdentifier(spark, name).identifier());
  // }
  //
  // private CatalogTable loadSessionTable(String name)
  //     throws NoSuchTableException, NoSuchDatabaseException, ParseException {
  //   Identifier identifier = Spark3Util.catalogAndIdentifier(spark, name).identifier();
  //   Some<String> namespace = Some.apply(identifier.namespace()[0]);
  //   return spark.sessionState().catalog().getTableMetadata(new TableIdentifier(identifier.name(), namespace));
  // }
  //
  // private void createSourceTable(String createStatement, String tableName)
  //     throws IOException, NoSuchTableException, NoSuchDatabaseException, ParseException {
  //   File location = temp.newFolder();
  //   spark.sql(String.format(createStatement, tableName, location));
  //   CatalogTable table = loadSessionTable(tableName);
  //   Seq<String> partitionColumns = table.partitionColumnNames();
  //   String format = table.provider().get();
  //   spark.table(baseTableName)
  //       .write()
  //       .mode(SaveMode.Append)
  //       .format(format)
  //       .partitionBy(partitionColumns.toSeq())
  //       .saveAsTable(tableName);
  // }
  //
  // // Counts the number of files in the source table, makes sure the same files exist in the destination table
  // private void assertMigratedFileCount(MigrateTable migrateAction, String source, String dest)
  //     throws NoSuchTableException, NoSuchDatabaseException, ParseException {
  //   long expectedFiles = expectedFilesCount(source);
  //   MigrateTable.Result migratedFiles = migrateAction.execute();
  //   validateTables(source, dest);
  //   Assert.assertEquals("Expected number of migrated files", expectedFiles, migratedFiles.migratedDataFilesCount());
  // }
  //
  // // Counts the number of files in the source table, makes sure the same files exist in the destination table
  // private void assertSnapshotFileCount(SnapshotTable snapshotTable, String source, String dest)
  //     throws NoSuchTableException, NoSuchDatabaseException, ParseException {
  //   long expectedFiles = expectedFilesCount(source);
  //   SnapshotTable.Result snapshotTableResult = snapshotTable.execute();
  //   validateTables(source, dest);
  //   Assert.assertEquals(
  //       "Expected number of imported snapshot files",
  //       expectedFiles,
  //       snapshotTableResult.importedDataFilesCount());
  // }
  //
  // private void validateTables(String source, String dest) throws NoSuchTableException, ParseException {
  //   List<Row> expected = spark.table(source).collectAsList();
  //   SparkTable destTable = loadTable(dest);
  //   Assert.assertEquals(
  //       "Provider should be iceberg",
  //       "iceberg",
  //       destTable.properties().get(TableCatalog.PROP_PROVIDER));
  //   List<Row> actual = spark.table(dest).collectAsList();
  //   Assert.assertTrue(String.format(
  //       "Rows in migrated table did not match\nExpected :%s rows \nFound    :%s",
  //       expected,
  //       actual), expected.containsAll(actual) && actual.containsAll(expected));
  // }
  //
  // private long expectedFilesCount(String source) throws
  // NoSuchDatabaseException, NoSuchTableException, ParseException {
  //   CatalogTable sourceTable = loadSessionTable(source);
  //   List<URI> uris;
  //   if (sourceTable.partitionColumnNames().size() == 0) {
  //     uris = Lists.newArrayList();
  //     uris.add(sourceTable.location());
  //   } else {
  //     Seq<CatalogTablePartition> catalogTablePartitionSeq =
  //         spark.sessionState().catalog().listPartitions(sourceTable.identifier(), Option.apply(null));
  //     uris = JavaConverters.seqAsJavaList(catalogTablePartitionSeq)
  //         .stream()
  //         .map(CatalogTablePartition::location)
  //         .collect(Collectors.toList());
  //   }
  //   return uris.stream()
  //       .flatMap(uri ->
  //       FileUtils.listFiles(Paths.get(uri).toFile(), TrueFileFilter.INSTANCE, TrueFileFilter.INSTANCE)
  //           .stream())
  //       .filter(file -> !file.toString().endsWith("crc") && !file.toString().contains("_SUCCESS"))
  //       .count();
  // }
  //
  // // Insert records into the destination, makes sure those records exist and source table is unchanged
  // private void assertIsolatedSnapshot(String source, String dest) {
  //   List<Row> expected = spark.sql(String.format("SELECT * FROM %s", source)).collectAsList();
  //
  //   List<SimpleRecord> extraData = Lists.newArrayList(new SimpleRecord(4, "d"));
  //   Dataset<Row> df = spark.createDataFrame(extraData, SimpleRecord.class);
  //   df.write().format("iceberg").mode("append").saveAsTable(dest);
  //
  //   List<Row> result = spark.sql(String.format("SELECT * FROM %s", source)).collectAsList();
  //   Assert.assertEquals("No additional rows should be added to the original table", expected.size(), result.size());
  //
  //   List<Row> snapshot =
  //   spark.sql(String.format("SELECT * FROM %s WHERE id = 4 AND data = 'd'", dest)).collectAsList();
  //   Assert.assertEquals("Added row not found in snapshot", 1, snapshot.size());
  // }
  //
  // private String sourceName(String source) {
  //   return NAMESPACE + "." + catalogName + "_" + type + "_" + source;
  // }
  //
  private String destName(String dest) {
    if (catalogName.equals("spark_catalog")) {
      return NAMESPACE + "." + catalogName + "_" + type + "_" + dest;
    } else {
      return catalogName + "." + NAMESPACE + "." + catalogName + "_" + type + "_" + dest;
    }
  }
}
