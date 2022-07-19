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

import io.delta.standalone.DeltaLog;
import io.delta.standalone.actions.AddFile;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotSummary;
import org.apache.iceberg.Table;
import org.apache.iceberg.actions.BaseMigrateDeltaLakeTableActionResult;
import org.apache.iceberg.actions.MigrateDeltaLakeTable;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.iceberg.spark.SparkTableUtil;
import org.apache.iceberg.spark.SparkTypeToType;
import org.apache.iceberg.spark.SparkTypeVisitor;
import org.apache.iceberg.spark.source.StagedSparkTable;
import org.apache.iceberg.types.Type;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.TableIdentifier;
import org.apache.spark.sql.catalyst.analysis.NoSuchDatabaseException;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException;
import org.apache.spark.sql.catalyst.catalog.CatalogTable;
import org.apache.spark.sql.connector.catalog.CatalogPlugin;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.StagingTableCatalog;
import org.apache.spark.sql.connector.expressions.LogicalExpressions;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Some;
import scala.collection.JavaConverters;

/**
 * Takes a Delta Lake table and attempts to transform it into an Iceberg table in the same location with the same
 * identifier. Once complete the identifier which previously referred to a non-Iceberg table will refer to the newly
 * migrated Iceberg table.
 */
public class BaseMigrateDeltaLakeTableSparkAction
    implements MigrateDeltaLakeTable {

  private static final Logger LOG = LoggerFactory.getLogger(BaseMigrateDeltaLakeTableSparkAction.class);
  private static final String BACKUP_SUFFIX = "_BACKUP_";

  private final Map<String, String> additionalProperties = new HashMap<>();
  private final SparkSession spark;
  private final DeltaLog deltaLog;
  private final StagingTableCatalog destCatalog;
  private final String location;
  private final Identifier identifier;
  private final Identifier backupIdent;

  BaseMigrateDeltaLakeTableSparkAction(
      SparkSession spark,
      CatalogPlugin sourceCatalog,
      Identifier sourceTableIdent
  ) {
    this.spark = spark;
    this.destCatalog = checkDestinationCatalog(sourceCatalog);
    this.identifier = sourceTableIdent;
    this.backupIdent = Identifier.of(sourceTableIdent.namespace(), sourceTableIdent.name() + BACKUP_SUFFIX);
    try {
      CatalogTable tableMetadata = spark.sessionState().catalogManager().v1SessionCatalog()
          .getTableMetadata(new TableIdentifier(sourceTableIdent.name()));
      this.location = tableMetadata.location().getPath();
      this.deltaLog = DeltaLog.forTable(spark.sessionState().newHadoopConf(), tableMetadata.location().getPath());
    } catch (NoSuchTableException | NoSuchDatabaseException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Result execute() {
    // Rename the table
    renameAndBackupSourceTable();

    // Get a DeltaLog instance and retrieve the partitions (if applicable) of the table
    io.delta.standalone.Snapshot updatedSnapshot = deltaLog.update();
    List<SparkTableUtil.SparkPartition> partitions =
        getSparkPartitionsFromDeltaSnapshot(updatedSnapshot, deltaLog.getPath());

    StructType structType = getStructTypeFromDeltaSnapshot(updatedSnapshot);
    StagedSparkTable stagedTable =
        stageDestTable(updatedSnapshot, location, destCatalog, identifier, structType, additionalProperties);
    Table icebergTable = stagedTable.table();

    PartitionSpec partitionSpec = getPartitionSpecFromDeltaSnapshot(updatedSnapshot, structType);
    String stagingLocation = SparkTableUtil.getIcebergMetadataLocation(icebergTable);

    SparkTableUtil.importSparkTable(
        spark,
        new TableIdentifier(backupIdent.name(), Some.apply(backupIdent.namespace()[0])),
        icebergTable,
        stagingLocation,
        Collections.emptyMap(),
        false,
        partitionSpec,
        partitions
    );

    stagedTable.commitStagedChanges();
    Snapshot snapshot = icebergTable.currentSnapshot();
    long numFilesMigrated = Long.parseLong(snapshot.summary().get(SnapshotSummary.TOTAL_DATA_FILES_PROP));
    LOG.info("Successfully loaded Iceberg metadata for {} files to {}", numFilesMigrated, identifier);
    return new BaseMigrateDeltaLakeTableActionResult(numFilesMigrated);
  }

  private static List<SparkTableUtil.SparkPartition> getSparkPartitionsFromDeltaSnapshot(
      io.delta.standalone.Snapshot updatedSnapshot,
      Path deltaLogPath
  ) {
    return updatedSnapshot.getAllFiles()
        .stream()
        // Map each partition to the list of files within it
        .collect(Collectors.groupingBy(AddFile::getPartitionValues))
        .entrySet()
        .stream()
        .map(entry -> {
              // We don't care what value we take since they will all have the same prefix.
              // The arbitrary file will have a path that looks like "partition1/partition2/file.parquet,
              // We're interested in the part prior to the filename
              AddFile addFile = entry.getValue().get(0);
              String pathBeforeFileName = addFile.getPath().substring(0, addFile.getPath().lastIndexOf("/"));
              String fullPath = new Path(deltaLogPath, pathBeforeFileName).toString();

              return new SparkTableUtil.SparkPartition(
                  entry.getKey(), // Map containing name and values of partitions
                  fullPath,
                  // Delta tables only support parquet
                  "parquet"
              );
            }
        )
        .collect(Collectors.toList());
  }

  private static PartitionSpec getPartitionSpecFromDeltaSnapshot(
      io.delta.standalone.Snapshot updatedSnapshot,
      StructType structType
  ) {
    Type converted = SparkTypeVisitor.visit(structType, new SparkTypeToType(structType));
    Schema schema = new Schema(converted.asNestedType().asStructType().fields());
    return SparkSchemaUtil.identitySpec(schema, updatedSnapshot.getMetadata().getPartitionColumns());
  }

  private static StructType getStructTypeFromDeltaSnapshot(io.delta.standalone.Snapshot updatedSnapshot) {
    io.delta.standalone.types.StructField[] fields =
        Optional.ofNullable(updatedSnapshot.getMetadata().getSchema())
            .map(io.delta.standalone.types.StructType::getFields)
            .orElseThrow(() -> new RuntimeException("Cannot determine table schema!"));

    // Convert from Delta StructFields to Spark StructFields
    return new StructType(
        Arrays.stream(fields)
            .map(s -> new StructField(
                    s.getName(),
                    DataType.fromJson(s.getDataType().toJson()),
                    s.isNullable(),
                    Metadata.fromJson(s.getMetadata().toString())
                )
            )
            .toArray(StructField[]::new)
    );
  }

  private void renameAndBackupSourceTable() {
    try {
      LOG.info("Renaming {} as {} for backup", identifier, backupIdent);
      destCatalog.renameTable(identifier, backupIdent);
    } catch (NoSuchTableException e) {
      throw new org.apache.iceberg.exceptions.NoSuchTableException("Cannot find source table %s", identifier);
    } catch (TableAlreadyExistsException e) {
      throw new AlreadyExistsException(
          "Cannot rename %s as %s for backup. The backup table already exists.",
          identifier, backupIdent);
    }
  }

  @Override
  public MigrateDeltaLakeTable tableProperties(Map<String, String> properties) {
    additionalProperties.putAll(properties);
    return this;
  }

  private static StagedSparkTable stageDestTable(
      io.delta.standalone.Snapshot deltaSnapshot,
      String tableLocation,
      StagingTableCatalog destinationCatalog,
      Identifier destIdentifier,
      org.apache.spark.sql.types.StructType structType,
      Map<String, String> additionalProperties) {
    try {
      Map<String, String> props = destTableProperties(deltaSnapshot, tableLocation, additionalProperties);
      io.delta.standalone.types.StructType schema = deltaSnapshot.getMetadata().getSchema();
      if (schema == null) {
        throw new IllegalStateException("Could not find schema in existing Delta Lake table.");
      }

      // Partitioning
      Transform[] partitioning = getPartitioning(deltaSnapshot);

      return (StagedSparkTable) destinationCatalog.stageCreate(destIdentifier, structType, partitioning, props);
    } catch (org.apache.spark.sql.catalyst.analysis.NoSuchNamespaceException e) {
      throw new NoSuchNamespaceException("Cannot create table %s as the namespace does not exist", destIdentifier);
    } catch (org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException e) {
      throw new AlreadyExistsException("Cannot create table %s as it already exists", destIdentifier);
    }
  }

  private static Transform[] getPartitioning(io.delta.standalone.Snapshot deltaSnapshot) {
    return deltaSnapshot
        .getMetadata()
        .getPartitionColumns()
        .stream()
        .map(name -> LogicalExpressions.identity(
                LogicalExpressions.reference(JavaConverters.asScalaBuffer(Collections.singletonList(name)))
            )
        )
        .toArray(Transform[]::new);
  }

  private static Map<String, String> destTableProperties(
      io.delta.standalone.Snapshot deltaSnapshot,
      String tableLocation,
      Map<String, String> additionalProperties
  ) {
    Map<String, String> properties = Maps.newHashMap();

    properties.putAll(deltaSnapshot.getMetadata().getConfiguration());
    properties.putAll(ImmutableMap.of(
        "provider", "iceberg",
        "migrated", "true",
        "table_type", "iceberg",
        "location", tableLocation
    ));
    properties.putAll(additionalProperties);

    return properties;
  }

  private StagingTableCatalog checkDestinationCatalog(CatalogPlugin catalog) {

    return (StagingTableCatalog) catalog;
  }
}
