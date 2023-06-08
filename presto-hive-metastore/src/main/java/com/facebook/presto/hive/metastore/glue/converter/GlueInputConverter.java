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
package com.facebook.presto.hive.metastore.glue.converter;

import com.facebook.presto.hive.HiveBucketProperty;
import com.facebook.presto.hive.metastore.Column;
import com.facebook.presto.hive.metastore.Database;
import com.facebook.presto.hive.metastore.Partition;
import com.facebook.presto.hive.metastore.PartitionStatistics;
import com.facebook.presto.hive.metastore.PartitionWithStatistics;
import com.facebook.presto.hive.metastore.Storage;
import com.facebook.presto.hive.metastore.Table;
import com.facebook.presto.spi.PrestoException;
import com.google.common.collect.ImmutableMap;
import software.amazon.awssdk.services.glue.model.DatabaseInput;
import software.amazon.awssdk.services.glue.model.Order;
import software.amazon.awssdk.services.glue.model.PartitionInput;
import software.amazon.awssdk.services.glue.model.SerDeInfo;
import software.amazon.awssdk.services.glue.model.StorageDescriptor;
import software.amazon.awssdk.services.glue.model.TableInput;

import java.util.EnumSet;
import java.util.List;
import java.util.Optional;

import static com.facebook.presto.hive.metastore.MetastoreUtil.updateStatisticsParameters;
import static com.facebook.presto.hive.metastore.PrestoTableType.EXTERNAL_TABLE;
import static com.facebook.presto.hive.metastore.PrestoTableType.MANAGED_TABLE;
import static com.facebook.presto.hive.metastore.PrestoTableType.VIRTUAL_VIEW;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.stream.Collectors.toList;

public final class GlueInputConverter
{
    private GlueInputConverter() {}

    public static DatabaseInput convertDatabase(Database database)
    {
        DatabaseInput.Builder inputBuilder = DatabaseInput.builder();
        inputBuilder.name(database.getDatabaseName());
        inputBuilder.parameters(database.getParameters());
        database.getComment().ifPresent(inputBuilder::description);
        database.getLocation().ifPresent(inputBuilder::locationUri);
        return inputBuilder.build();
    }

    public static TableInput convertTable(Table table)
    {
        TableInput.Builder input = TableInput.builder();
        input.name(table.getTableName());
        input.owner(table.getOwner());
        checkArgument(EnumSet.of(MANAGED_TABLE, EXTERNAL_TABLE, VIRTUAL_VIEW).contains(table.getTableType()), "Invalid table type: %s", table.getTableType());
        input.tableType(table.getTableType().toString());
        input.storageDescriptor(convertStorage(table.getStorage(), table.getDataColumns()));
        input.partitionKeys(table.getPartitionColumns().stream().map(GlueInputConverter::convertColumn).collect(toList()));
        input.parameters(table.getParameters());
        table.getViewOriginalText().ifPresent(input::viewOriginalText);
        table.getViewExpandedText().ifPresent(input::viewExpandedText);
        return input.build();
    }

    public static TableInput toTableInput(software.amazon.awssdk.services.glue.model.Table table)
    {
        TableInput.Builder input = TableInput.builder();
        input.name(table.name());
        input.owner(table.owner());
        input.tableType(table.tableType());
        input.storageDescriptor(table.storageDescriptor());
        input.partitionKeys(table.partitionKeys());
        input.parameters(table.parameters());
        input.viewOriginalText(table.viewOriginalText());
        input.viewExpandedText(table.viewExpandedText());
        return input.build();
    }

    public static PartitionInput convertPartition(PartitionWithStatistics partitionWithStatistics)
    {
        PartitionInput input = convertPartition(partitionWithStatistics.getPartition());
        PartitionStatistics statistics = partitionWithStatistics.getStatistics();
        if (!statistics.getColumnStatistics().isEmpty()) {
            throw new PrestoException(NOT_SUPPORTED, "Glue metastore does not support column level statistics");
        }
        input = input.toBuilder().parameters(updateStatisticsParameters(input.parameters(), statistics.getBasicStatistics())).build();
        return input;
    }

    public static PartitionInput convertPartition(Partition partition)
    {
        PartitionInput.Builder input = PartitionInput.builder();
        input.values(partition.getValues());
        input.storageDescriptor(convertStorage(partition.getStorage(), partition.getColumns()));
        input.parameters(partition.getParameters());
        return input.build();
    }

    private static StorageDescriptor convertStorage(Storage storage, List<Column> columns)
    {
        if (storage.isSkewed()) {
            throw new IllegalArgumentException("Writing to skewed table/partition is not supported");
        }
        SerDeInfo serdeInfo = SerDeInfo.builder()
                .serializationLibrary(storage.getStorageFormat().getSerDeNullable())
                .parameters(storage.getSerdeParameters()).build();

        StorageDescriptor.Builder sd = StorageDescriptor.builder();
        sd.location(storage.getLocation());
        sd.columns(columns.stream().map(GlueInputConverter::convertColumn).collect(toList()));
        sd.serdeInfo(serdeInfo);
        sd.inputFormat(storage.getStorageFormat().getInputFormatNullable());
        sd.outputFormat(storage.getStorageFormat().getOutputFormatNullable());
        sd.parameters(ImmutableMap.of());

        Optional<HiveBucketProperty> bucketProperty = storage.getBucketProperty();
        if (bucketProperty.isPresent()) {
            sd.numberOfBuckets(bucketProperty.get().getBucketCount());
            sd.bucketColumns(bucketProperty.get().getBucketedBy());
            if (!bucketProperty.get().getSortedBy().isEmpty()) {
                sd.sortColumns(bucketProperty.get().getSortedBy().stream()
                        .map(column -> Order.builder().column(column.getColumnName()).sortOrder(column.getOrder().getHiveOrder()).build())
                        .collect(toImmutableList()));
            }
        }

        return sd.build();
    }

    public static software.amazon.awssdk.services.glue.model.Column convertColumn(Column prestoColumn)
    {
        return software.amazon.awssdk.services.glue.model.Column.builder()
                .name(prestoColumn.getName())
                .type(prestoColumn.getType().toString())
                .comment(prestoColumn.getComment().orElse(null)).build();
    }
}
