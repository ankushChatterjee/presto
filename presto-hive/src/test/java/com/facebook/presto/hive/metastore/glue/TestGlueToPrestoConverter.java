/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.hive.metastore.glue;

import com.facebook.presto.hive.HiveBucketProperty;
import com.facebook.presto.hive.metastore.Column;
import com.facebook.presto.hive.metastore.Storage;
import com.facebook.presto.hive.metastore.glue.converter.GlueToPrestoConverter;
import com.facebook.presto.hive.metastore.glue.converter.GlueToPrestoConverter.GluePartitionConverter;
import com.facebook.presto.spi.security.PrincipalType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import software.amazon.awssdk.services.glue.model.Database;
import software.amazon.awssdk.services.glue.model.Partition;
import software.amazon.awssdk.services.glue.model.SerDeInfo;
import software.amazon.awssdk.services.glue.model.StorageDescriptor;
import software.amazon.awssdk.services.glue.model.Table;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;

import static com.facebook.presto.hive.metastore.MetastoreUtil.DELTA_LAKE_PROVIDER;
import static com.facebook.presto.hive.metastore.MetastoreUtil.ICEBERG_TABLE_TYPE_NAME;
import static com.facebook.presto.hive.metastore.MetastoreUtil.ICEBERG_TABLE_TYPE_VALUE;
import static com.facebook.presto.hive.metastore.MetastoreUtil.SPARK_TABLE_PROVIDER_KEY;
import static com.facebook.presto.hive.metastore.PrestoTableType.EXTERNAL_TABLE;
import static com.facebook.presto.hive.metastore.glue.TestingMetastoreObjects.getGlueTestColumn;
import static com.facebook.presto.hive.metastore.glue.TestingMetastoreObjects.getGlueTestDatabase;
import static com.facebook.presto.hive.metastore.glue.TestingMetastoreObjects.getGlueTestPartition;
import static com.facebook.presto.hive.metastore.glue.TestingMetastoreObjects.getGlueTestSerdeInfo;
import static com.facebook.presto.hive.metastore.glue.TestingMetastoreObjects.getGlueTestStorageDescriptor;
import static com.facebook.presto.hive.metastore.glue.TestingMetastoreObjects.getGlueTestTable;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNotSame;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;
import static software.amazon.awssdk.utils.CollectionUtils.isNullOrEmpty;

@Test(singleThreaded = true)
public class TestGlueToPrestoConverter
{
    private static final String PUBLIC_OWNER = "PUBLIC";

    private Database.Builder testDb;
    private Table.Builder testTbl;
    private Partition.Builder testPartition;

    @BeforeMethod
    public void setup()
    {
        testDb = getGlueTestDatabase();
        testTbl = getGlueTestTable(testDb.build().name());
        testPartition = getGlueTestPartition(testDb.build().name(), testTbl.build().name(), ImmutableList.of("val1"));
    }

    @Test
    public void testConvertDatabase()
    {
        Database database = testDb.build();
        com.facebook.presto.hive.metastore.Database prestoDb = GlueToPrestoConverter.convertDatabase(database);
        assertEquals(prestoDb.getDatabaseName(), database.name());
        assertEquals(prestoDb.getLocation().get(), database.locationUri());
        assertEquals(prestoDb.getComment().get(), database.description());
        assertEquals(prestoDb.getParameters(), database.parameters());
        assertEquals(prestoDb.getOwnerName(), PUBLIC_OWNER);
        assertEquals(prestoDb.getOwnerType(), PrincipalType.ROLE);
    }

    @Test
    public void testConvertTable()
    {
        Database database = testDb.build();
        Table table = testTbl.build();
        com.facebook.presto.hive.metastore.Table prestoTbl = GlueToPrestoConverter.convertTable(table, database.name());
        assertEquals(prestoTbl.getTableName(), table.name());
        assertEquals(prestoTbl.getDatabaseName(), database.name());
        assertEquals(prestoTbl.getTableType().toString(), table.tableType());
        assertEquals(prestoTbl.getOwner(), table.owner());
        assertEquals(prestoTbl.getParameters(), table.parameters());
        assertColumnList(prestoTbl.getDataColumns(), table.storageDescriptor().columns());
        assertColumnList(prestoTbl.getPartitionColumns(), table.partitionKeys());
        assertStorage(prestoTbl.getStorage(), table.storageDescriptor());
        assertEquals(prestoTbl.getViewOriginalText().get(), table.viewOriginalText());
        assertEquals(prestoTbl.getViewExpandedText().get(), table.viewExpandedText());
    }

    @Test
    public void testConvertTableNullPartitions()
    {
        testTbl.partitionKeys((Collection<software.amazon.awssdk.services.glue.model.Column>) null);
        com.facebook.presto.hive.metastore.Table prestoTbl = GlueToPrestoConverter.convertTable(testTbl.build(), testDb.build().name());
        assertTrue(prestoTbl.getPartitionColumns().isEmpty());
    }

    @Test
    public void testConvertTableUppercaseColumnType()
    {
        software.amazon.awssdk.services.glue.model.Column uppercaseCol = getGlueTestColumn().toBuilder().type("String").build();
        testTbl.storageDescriptor(getGlueTestStorageDescriptor().columns(ImmutableList.of(uppercaseCol)).build());
        GlueToPrestoConverter.convertTable(testTbl.build(), testDb.build().name());
    }

    @Test
    public void testConvertPartition()
    {
        Partition partition = testPartition.build();
        GluePartitionConverter converter = new GluePartitionConverter(partition.databaseName(), partition.tableName());
        com.facebook.presto.hive.metastore.Partition prestoPartition = converter.apply(partition);
        assertEquals(prestoPartition.getDatabaseName(), partition.databaseName());
        assertEquals(prestoPartition.getTableName(), partition.tableName());
        assertColumnList(prestoPartition.getColumns(), partition.storageDescriptor().columns());
        assertEquals(prestoPartition.getValues(), partition.values());
        assertStorage(prestoPartition.getStorage(), partition.storageDescriptor());
        assertEquals(prestoPartition.getParameters(), partition.parameters());
    }

    @Test
    public void testPartitionConversionMemoization()
    {
        String fakeS3Location = "s3://some-fake-location";
        testPartition.storageDescriptor(getGlueTestStorageDescriptor().location(fakeS3Location).build()).build();
        Partition partition = testPartition.build();
        //  Second partition to convert with equal (but not aliased) values
        Partition.Builder partitionTwo = getGlueTestPartition(partition.databaseName(), partition.tableName(), new ArrayList<>(partition.values()));
        //  Ensure storage fields are equal but not aliased as well

        StorageDescriptor storageDescriptor = getGlueTestStorageDescriptor()
                .columns(new ArrayList<>(partition.storageDescriptor().columns()))
                .bucketColumns(new ArrayList<>(partition.storageDescriptor().bucketColumns()))
                .location("" + fakeS3Location)
                .inputFormat("" + partition.storageDescriptor().inputFormat())
                .outputFormat("" + partition.storageDescriptor().outputFormat())
                .parameters(new HashMap<>(partition.storageDescriptor().parameters())).build();

        partitionTwo.storageDescriptor(storageDescriptor);

        GluePartitionConverter converter = new GluePartitionConverter(testDb.build().name(), testTbl.build().name());
        com.facebook.presto.hive.metastore.Partition prestoPartition = converter.apply(partition);
        com.facebook.presto.hive.metastore.Partition prestoPartition2 = converter.apply(partitionTwo.build());

        assertNotSame(prestoPartition, prestoPartition2);
        assertSame(prestoPartition2.getDatabaseName(), prestoPartition.getDatabaseName());
        assertSame(prestoPartition2.getTableName(), prestoPartition.getTableName());
        assertSame(prestoPartition2.getColumns(), prestoPartition.getColumns());
        assertSame(prestoPartition2.getParameters(), prestoPartition.getParameters());
        assertNotSame(prestoPartition2.getValues(), prestoPartition.getValues());

        Storage storage = prestoPartition.getStorage();
        Storage storage2 = prestoPartition2.getStorage();

        assertSame(storage2.getStorageFormat(), storage.getStorageFormat());
        assertSame(storage2.getBucketProperty(), storage.getBucketProperty());
        assertSame(storage2.getSerdeParameters(), storage.getSerdeParameters());
        assertNotSame(storage2.getLocation(), storage.getLocation());
    }

    @Test
    public void testDatabaseNullParameters()
    {
        testDb.parameters(null).build();
        assertNotNull(GlueToPrestoConverter.convertDatabase(testDb.build()).getParameters());
    }

    @Test
    public void testTableNullParameters()
    {
        testTbl.parameters(null);
        StorageDescriptor.Builder storageDescriptor = getGlueTestStorageDescriptor();
        SerDeInfo serDeInfo = getGlueTestSerdeInfo().parameters(null).build();
        testTbl.storageDescriptor(storageDescriptor.serdeInfo(serDeInfo).build());
        com.facebook.presto.hive.metastore.Table prestoTable = GlueToPrestoConverter.convertTable(testTbl.build(), testDb.build().name());
        assertNotNull(prestoTable.getParameters());
        assertNotNull(prestoTable.getStorage().getSerdeParameters());
    }

    @Test
    public void testPartitionNullParameters()
    {
        Table table = testTbl.parameters(null).build();
        assertNotNull(new GluePartitionConverter(testDb.build().name(), table.name()).apply(testPartition.build()).getParameters());
    }

    @Test
    public void testConvertTableWithoutTableType()
    {
        Table table = getGlueTestTable(testDb.build().name()).tableType(null).build();
        com.facebook.presto.hive.metastore.Table prestoTable = GlueToPrestoConverter.convertTable(table, testDb.build().name());
        assertEquals(prestoTable.getTableType(), EXTERNAL_TABLE);
    }

    @Test
    public void testIcebergTableNonNullStorageDescriptor()
    {
        testTbl.parameters(ImmutableMap.of(ICEBERG_TABLE_TYPE_NAME, ICEBERG_TABLE_TYPE_VALUE));
        assertNotNull(testTbl.build().storageDescriptor());
        com.facebook.presto.hive.metastore.Table prestoTable = GlueToPrestoConverter.convertTable(testTbl.build(), testDb.build().name());
        assertEquals(prestoTable.getDataColumns().size(), 1);
    }

    @Test
    public void testDeltaTableNonNullStorageDescriptor()
    {
        testTbl.parameters(ImmutableMap.of(SPARK_TABLE_PROVIDER_KEY, DELTA_LAKE_PROVIDER));
        assertNotNull(testTbl.build().storageDescriptor());
        com.facebook.presto.hive.metastore.Table prestoTable = GlueToPrestoConverter.convertTable(testTbl.build(), testDb.build().name());
    }

    private static void assertColumnList(List<Column> actual, List<software.amazon.awssdk.services.glue.model.Column> expected)
    {
        if (expected == null) {
            assertNull(actual);
        }
        assertEquals(actual.size(), expected.size());

        for (int i = 0; i < expected.size(); i++) {
            assertColumn(actual.get(i), expected.get(i));
        }
    }

    private static void assertColumn(Column actual, software.amazon.awssdk.services.glue.model.Column expected)
    {
        assertEquals(actual.getName(), expected.name());
        assertEquals(actual.getType().getHiveTypeName().toString(), expected.type());
        assertEquals(actual.getComment().get(), expected.comment());
    }

    private static void assertStorage(Storage actual, StorageDescriptor expected)
    {
        assertEquals(actual.getLocation(), expected.location());
        assertEquals(actual.getStorageFormat().getSerDe(), expected.serdeInfo().serializationLibrary());
        assertEquals(actual.getStorageFormat().getInputFormat(), expected.inputFormat());
        assertEquals(actual.getStorageFormat().getOutputFormat(), expected.outputFormat());
        if (!isNullOrEmpty(expected.bucketColumns())) {
            HiveBucketProperty bucketProperty = actual.getBucketProperty().get();
            assertEquals(bucketProperty.getBucketedBy(), expected.bucketColumns());
            assertEquals(bucketProperty.getBucketCount(), expected.numberOfBuckets().intValue());
        }
    }
}
