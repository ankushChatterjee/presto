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

import com.facebook.presto.common.predicate.Domain;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.hive.HdfsContext;
import com.facebook.presto.hive.HdfsEnvironment;
import com.facebook.presto.hive.HiveType;
import com.facebook.presto.hive.PartitionNotFoundException;
import com.facebook.presto.hive.SchemaAlreadyExistsException;
import com.facebook.presto.hive.TableAlreadyExistsException;
import com.facebook.presto.hive.metastore.Column;
import com.facebook.presto.hive.metastore.Database;
import com.facebook.presto.hive.metastore.ExtendedHiveMetastore;
import com.facebook.presto.hive.metastore.HivePrivilegeInfo;
import com.facebook.presto.hive.metastore.MetastoreContext;
import com.facebook.presto.hive.metastore.MetastoreOperationResult;
import com.facebook.presto.hive.metastore.MetastoreUtil;
import com.facebook.presto.hive.metastore.Partition;
import com.facebook.presto.hive.metastore.PartitionNameWithVersion;
import com.facebook.presto.hive.metastore.PartitionStatistics;
import com.facebook.presto.hive.metastore.PartitionWithStatistics;
import com.facebook.presto.hive.metastore.PrincipalPrivileges;
import com.facebook.presto.hive.metastore.Table;
import com.facebook.presto.hive.metastore.glue.converter.GlueInputConverter;
import com.facebook.presto.hive.metastore.glue.converter.GlueToPrestoConverter;
import com.facebook.presto.hive.metastore.glue.converter.GlueToPrestoConverter.GluePartitionConverter;
import com.facebook.presto.spi.ColumnNotFoundException;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaNotFoundException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.TableNotFoundException;
import com.facebook.presto.spi.security.ConnectorIdentity;
import com.facebook.presto.spi.security.PrestoPrincipal;
import com.facebook.presto.spi.security.RoleGrant;
import com.facebook.presto.spi.statistics.ColumnStatisticType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import io.airlift.units.Duration;
import org.apache.hadoop.fs.Path;
import org.weakref.jmx.Flatten;
import org.weakref.jmx.Managed;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.retry.RetryPolicy;
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient;
import software.amazon.awssdk.metrics.MetricPublisher;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.regions.internal.util.EC2MetadataUtils;
import software.amazon.awssdk.services.glue.GlueAsyncClient;
import software.amazon.awssdk.services.glue.GlueAsyncClientBuilder;
import software.amazon.awssdk.services.glue.model.AlreadyExistsException;
import software.amazon.awssdk.services.glue.model.BatchCreatePartitionRequest;
import software.amazon.awssdk.services.glue.model.BatchCreatePartitionResponse;
import software.amazon.awssdk.services.glue.model.BatchGetPartitionRequest;
import software.amazon.awssdk.services.glue.model.BatchGetPartitionResponse;
import software.amazon.awssdk.services.glue.model.CreateDatabaseRequest;
import software.amazon.awssdk.services.glue.model.CreateTableRequest;
import software.amazon.awssdk.services.glue.model.DatabaseInput;
import software.amazon.awssdk.services.glue.model.DeleteDatabaseRequest;
import software.amazon.awssdk.services.glue.model.DeletePartitionRequest;
import software.amazon.awssdk.services.glue.model.DeleteTableRequest;
import software.amazon.awssdk.services.glue.model.EntityNotFoundException;
import software.amazon.awssdk.services.glue.model.ErrorDetail;
import software.amazon.awssdk.services.glue.model.GetDatabaseRequest;
import software.amazon.awssdk.services.glue.model.GetDatabaseResponse;
import software.amazon.awssdk.services.glue.model.GetDatabasesRequest;
import software.amazon.awssdk.services.glue.model.GetDatabasesResponse;
import software.amazon.awssdk.services.glue.model.GetPartitionRequest;
import software.amazon.awssdk.services.glue.model.GetPartitionResponse;
import software.amazon.awssdk.services.glue.model.GetPartitionsRequest;
import software.amazon.awssdk.services.glue.model.GetPartitionsResponse;
import software.amazon.awssdk.services.glue.model.GetTableRequest;
import software.amazon.awssdk.services.glue.model.GetTableResponse;
import software.amazon.awssdk.services.glue.model.GetTablesRequest;
import software.amazon.awssdk.services.glue.model.GetTablesResponse;
import software.amazon.awssdk.services.glue.model.PartitionError;
import software.amazon.awssdk.services.glue.model.PartitionInput;
import software.amazon.awssdk.services.glue.model.PartitionValueList;
import software.amazon.awssdk.services.glue.model.Segment;
import software.amazon.awssdk.services.glue.model.StorageDescriptor;
import software.amazon.awssdk.services.glue.model.TableInput;
import software.amazon.awssdk.services.glue.model.UpdateDatabaseRequest;
import software.amazon.awssdk.services.glue.model.UpdatePartitionRequest;
import software.amazon.awssdk.services.glue.model.UpdateTableRequest;
import software.amazon.awssdk.services.sts.auth.StsAssumeRoleCredentialsProvider;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;

import javax.annotation.Nullable;
import javax.inject.Inject;

import java.net.URI;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.Future;
import java.util.function.Function;

import static com.facebook.presto.hive.HiveErrorCode.HIVE_METASTORE_ERROR;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_PARTITION_DROPPED_DURING_QUERY;
import static com.facebook.presto.hive.metastore.MetastoreOperationResult.EMPTY_RESULT;
import static com.facebook.presto.hive.metastore.MetastoreUtil.createDirectory;
import static com.facebook.presto.hive.metastore.MetastoreUtil.deleteDirectoryRecursively;
import static com.facebook.presto.hive.metastore.MetastoreUtil.getHiveBasicStatistics;
import static com.facebook.presto.hive.metastore.MetastoreUtil.isManagedTable;
import static com.facebook.presto.hive.metastore.MetastoreUtil.makePartName;
import static com.facebook.presto.hive.metastore.MetastoreUtil.toPartitionValues;
import static com.facebook.presto.hive.metastore.MetastoreUtil.updateStatisticsParameters;
import static com.facebook.presto.hive.metastore.MetastoreUtil.verifyCanDropColumn;
import static com.facebook.presto.hive.metastore.PrestoTableType.VIRTUAL_VIEW;
import static com.facebook.presto.hive.metastore.glue.AwsSdkUtil.awsSyncRequest;
import static com.facebook.presto.hive.metastore.glue.GlueExpressionUtil.buildGlueExpression;
import static com.facebook.presto.hive.metastore.glue.converter.GlueInputConverter.convertColumn;
import static com.facebook.presto.hive.metastore.glue.converter.GlueInputConverter.toTableInput;
import static com.facebook.presto.hive.metastore.glue.converter.GlueToPrestoConverter.mappedCopy;
import static com.facebook.presto.spi.StandardErrorCode.ALREADY_EXISTS;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.spi.security.PrincipalType.USER;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.collect.Comparators.lexicographical;
import static java.util.Comparator.comparing;
import static java.util.Objects.requireNonNull;
import static java.util.function.UnaryOperator.identity;
import static java.util.stream.Collectors.toMap;

public class GlueHiveMetastore
        implements ExtendedHiveMetastore
{
    private static final String PUBLIC_ROLE_NAME = "public";
    private static final String DEFAULT_METASTORE_USER = "presto";
    private static final String WILDCARD_EXPRESSION = "";

    // This is the total number of partitions allowed to process in a big batch chunk which splits multiple smaller batch of partitions allowed by BATCH_CREATE_PARTITION_MAX_PAGE_SIZE
    // Here's an example diagram on how async batches are handled for Create Partition:
    // |--------BATCH_CREATE_PARTITION_MAX_PAGE_SIZE------------| ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
    // | p0, p1, p2 .....................................   p99 |
    // |--------------------------------------------------------|
    // | p0, p1, p2 .....................................   p99 |
    // |--------------------------------------------------------|
    // BATCH_PARTITION_COMMIT_TOTAL_SIZE / BATCH_CREATE_PARTITION_MAX_PAGE_SIZE ..... (10k/100=100 batches)
    // |--------------------------------------------------------|++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
    // | p0, p1, p2 .....................................   p99 |
    // |--------------------------------------------------------|
    // | p0, p1, p2 .....................................   p99 |
    // |--------------------------------------------------------|.......... (100 batches)
    // |--------------------------------------------------------|++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
    private static final int BATCH_PARTITION_COMMIT_TOTAL_SIZE = 10000;
    private static final int BATCH_GET_PARTITION_MAX_PAGE_SIZE = 1000;
    // this is the total number of partitions allowed per batch that glue metastore can process to create partitions
    private static final int BATCH_CREATE_PARTITION_MAX_PAGE_SIZE = 100;
    private static final int AWS_GLUE_GET_PARTITIONS_MAX_RESULTS = 1000;
    private static final Comparator<Partition> PARTITION_COMPARATOR = comparing(Partition::getValues, lexicographical(String.CASE_INSENSITIVE_ORDER));

    private final GlueMetastoreStats stats = new GlueMetastoreStats();
    private final HdfsEnvironment hdfsEnvironment;
    private final HdfsContext hdfsContext;
    private final GlueAsyncClient glueClient;
    private final Optional<String> defaultDir;
    private final String catalogId;
    private final int partitionSegments;
    private final Executor executor;

    @Inject
    public GlueHiveMetastore(
            HdfsEnvironment hdfsEnvironment,
            GlueHiveMetastoreConfig glueConfig,
            @ForGlueHiveMetastore Executor executor)
    {
        this.hdfsEnvironment = requireNonNull(hdfsEnvironment, "hdfsEnvironment is null");
        this.hdfsContext = new HdfsContext(new ConnectorIdentity(DEFAULT_METASTORE_USER, Optional.empty(), Optional.empty()));
        this.glueClient = createAsyncGlueClient(requireNonNull(glueConfig, "glueConfig is null"), stats.newRequestMetricsCollector());
        this.defaultDir = glueConfig.getDefaultWarehouseDir();
        this.catalogId = glueConfig.getCatalogId().orElse(null);
        this.partitionSegments = glueConfig.getPartitionSegments();
        this.executor = requireNonNull(executor, "executor is null");
    }

    private static GlueAsyncClient createAsyncGlueClient(GlueHiveMetastoreConfig config, GlueMetastoreStats.GlueSdkClientMetricsCollector metricsPublisher)
    {
        NettyNioAsyncHttpClient.Builder nettyBuilder = NettyNioAsyncHttpClient.builder().maxConcurrency(config.getMaxGlueConnections());
        RetryPolicy.Builder retryPolicy = RetryPolicy.builder().numRetries(config.getMaxGlueErrorRetries());
        List<MetricPublisher> metricPublishers = new ArrayList<>();
        metricPublishers.add(metricsPublisher);
        ClientOverrideConfiguration.Builder clientOverrideConfiguration = ClientOverrideConfiguration.builder().retryPolicy(retryPolicy.build()).metricPublishers(metricPublishers);
        GlueAsyncClientBuilder asyncGlueClientBuilder = GlueAsyncClient.builder().httpClientBuilder(nettyBuilder).overrideConfiguration(clientOverrideConfiguration.build());

        if (config.getGlueEndpointUrl().isPresent()) {
            checkArgument(config.getGlueRegion().isPresent(), "Glue region must be set when Glue endpoint URL is set");
            asyncGlueClientBuilder.endpointOverride(URI.create(config.getGlueEndpointUrl().get())).region(Region.of(config.getGlueRegion().get()));
        }
        else if (config.getGlueRegion().isPresent()) {
            asyncGlueClientBuilder.region(Region.of(config.getGlueRegion().get()));
        }
        else if (config.getPinGlueClientToCurrentRegion()) {
            String currentRegion = EC2MetadataUtils.getEC2InstanceRegion();
            if (currentRegion != null) {
                asyncGlueClientBuilder.region(Region.of(currentRegion));
            }
        }

        if (config.getAwsAccessKey().isPresent() && config.getAwsSecretKey().isPresent()) {
            AwsCredentialsProvider credentialsProvider = StaticCredentialsProvider.create(AwsBasicCredentials.create(config.getAwsAccessKey().get(), config.getAwsSecretKey().get()));
            asyncGlueClientBuilder.credentialsProvider(credentialsProvider);
        }
        else if (config.getIamRole().isPresent()) {
            AwsCredentialsProvider credentialsProvider = StsAssumeRoleCredentialsProvider.builder()
                            .refreshRequest(() -> AssumeRoleRequest.builder().roleArn(config.getIamRole().get())
                            .roleSessionName("roleSessionName").build()).build();
            asyncGlueClientBuilder.credentialsProvider(credentialsProvider);
        }

        return asyncGlueClientBuilder.build();
    }

    @Managed
    @Flatten
    public GlueMetastoreStats getStats()
    {
        return stats;
    }

    // For Glue metastore there's an upper bound limit on 100 partitions per batch.
    // Here's the reference: https://docs.aws.amazon.com/glue/latest/webapi/API_BatchCreatePartition.html
    @Override
    public int getPartitionCommitBatchSize()
    {
        return BATCH_PARTITION_COMMIT_TOTAL_SIZE;
    }

    @Override
    public Optional<Database> getDatabase(MetastoreContext metastoreContext, String databaseName)
    {
        try {
            GetDatabaseResponse result = awsSyncRequest(glueClient::getDatabase, GetDatabaseRequest.builder().catalogId(catalogId).name(databaseName).build(), stats.getGetDatabase());
            return Optional.of(GlueToPrestoConverter.convertDatabase(result.database()));
        }
        catch (EntityNotFoundException e) {
            return Optional.empty();
        }
        catch (AwsServiceException e) {
            throw new PrestoException(HIVE_METASTORE_ERROR, e);
        }
    }

    @Override
    public List<String> getAllDatabases(MetastoreContext metastoreContext)
    {
        try {
            List<String> databaseNames = new ArrayList<>();
            GetDatabasesRequest request = GetDatabasesRequest.builder().catalogId(catalogId).build();
            do {
                GetDatabasesResponse result = awsSyncRequest(glueClient::getDatabases, request, stats.getGetDatabases());
                request = request.toBuilder().nextToken(result.nextToken()).build();
                result.databaseList().forEach(database -> databaseNames.add(database.name()));
            }
            while (request.nextToken() != null);

            return databaseNames;
        }
        catch (AwsServiceException e) {
            throw new PrestoException(HIVE_METASTORE_ERROR, e);
        }
    }

    @Override
    public Optional<Table> getTable(MetastoreContext metastoreContext, String databaseName, String tableName)
    {
        return getGlueTable(databaseName, tableName).map(table -> GlueToPrestoConverter.convertTable(table, databaseName));
    }

    private software.amazon.awssdk.services.glue.model.Table getGlueTableOrElseThrow(String databaseName, String tableName)
    {
        return getGlueTable(databaseName, tableName).orElseThrow(() -> new TableNotFoundException(new SchemaTableName(databaseName, tableName)));
    }

    private Optional<software.amazon.awssdk.services.glue.model.Table> getGlueTable(String databaseName, String tableName)
    {
        GetTableRequest request = GetTableRequest.builder()
                .catalogId(catalogId)
                .databaseName(databaseName)
                .name(tableName).build();

        try {
            GetTableResponse result = awsSyncRequest(glueClient::getTable, request, stats.getGetTable());
            return Optional.of(result.table());
        }
        catch (EntityNotFoundException e) {
            return Optional.empty();
        }
        catch (AwsServiceException e) {
            throw new PrestoException(HIVE_METASTORE_ERROR, e);
        }
    }

    @Override
    public Set<ColumnStatisticType> getSupportedColumnStatistics(MetastoreContext metastoreContext, Type type)
    {
        return ImmutableSet.of();
    }

    private Table getTableOrElseThrow(MetastoreContext metastoreContext, String databaseName, String tableName)
    {
        return getTable(metastoreContext, databaseName, tableName)
                .orElseThrow(() -> new TableNotFoundException(new SchemaTableName(databaseName, tableName)));
    }

    @Override
    public PartitionStatistics getTableStatistics(MetastoreContext metastoreContext, String databaseName, String tableName)
    {
        Table table = getTable(metastoreContext, databaseName, tableName)
                .orElseThrow(() -> new TableNotFoundException(new SchemaTableName(databaseName, tableName)));
        return new PartitionStatistics(getHiveBasicStatistics(table.getParameters()), ImmutableMap.of());
    }

    @Override
    public Map<String, PartitionStatistics> getPartitionStatistics(MetastoreContext metastoreContext, String databaseName, String tableName, Set<String> partitionNames)
    {
        ImmutableMap.Builder<String, PartitionStatistics> result = ImmutableMap.builder();
        getPartitionsByNames(metastoreContext, databaseName, tableName, ImmutableList.copyOf(partitionNames)).forEach((partitionName, optionalPartition) -> {
            Partition partition = optionalPartition.orElseThrow(() ->
                    new PartitionNotFoundException(new SchemaTableName(databaseName, tableName), toPartitionValues(partitionName)));
            PartitionStatistics partitionStatistics = new PartitionStatistics(getHiveBasicStatistics(partition.getParameters()), ImmutableMap.of());
            result.put(partitionName, partitionStatistics);
        });
        return result.build();
    }

    @Override
    public void updateTableStatistics(MetastoreContext metastoreContext, String databaseName, String tableName, Function<PartitionStatistics, PartitionStatistics> update)
    {
        PartitionStatistics currentStatistics = getTableStatistics(metastoreContext, databaseName, tableName);
        PartitionStatistics updatedStatistics = update.apply(currentStatistics);
        if (!updatedStatistics.getColumnStatistics().isEmpty()) {
            throw new PrestoException(NOT_SUPPORTED, "Glue metastore does not support column level statistics");
        }

        Table table = getTableOrElseThrow(metastoreContext, databaseName, tableName);

        try {
            TableInput tableInput = GlueInputConverter.convertTable(table);
            tableInput = tableInput.toBuilder().parameters(updateStatisticsParameters(table.getParameters(), updatedStatistics.getBasicStatistics())).build();
            UpdateTableRequest request = UpdateTableRequest.builder()
                    .catalogId(catalogId)
                    .databaseName(databaseName)
                    .tableInput(tableInput).build();
            awsSyncRequest(glueClient::updateTable, request, stats.getUpdateTable());
        }
        catch (EntityNotFoundException e) {
            throw new TableNotFoundException(new SchemaTableName(databaseName, tableName));
        }
        catch (AwsServiceException e) {
            throw new PrestoException(HIVE_METASTORE_ERROR, e);
        }
    }

    @Override
    public void updatePartitionStatistics(MetastoreContext metastoreContext, String databaseName, String tableName, String partitionName, Function<PartitionStatistics, PartitionStatistics> update)
    {
        PartitionStatistics currentStatistics = getPartitionStatistics(metastoreContext, databaseName, tableName, ImmutableSet.of(partitionName)).get(partitionName);
        if (currentStatistics == null) {
            throw new PrestoException(HIVE_PARTITION_DROPPED_DURING_QUERY, "Statistics result does not contain entry for partition: " + partitionName);
        }
        PartitionStatistics updatedStatistics = update.apply(currentStatistics);
        if (!updatedStatistics.getColumnStatistics().isEmpty()) {
            throw new PrestoException(NOT_SUPPORTED, "Glue metastore does not support column level statistics");
        }

        List<String> partitionValues = toPartitionValues(partitionName);
        Partition partition = getPartition(metastoreContext, databaseName, tableName, partitionValues).orElseThrow(() -> new PartitionNotFoundException(new SchemaTableName(databaseName, tableName), partitionValues));
        try {
            PartitionInput partitionInput = GlueInputConverter.convertPartition(partition);
            partitionInput = partitionInput.toBuilder().parameters(updateStatisticsParameters(partition.getParameters(), updatedStatistics.getBasicStatistics())).build();
            PartitionInput finalPartitionInput = partitionInput;
            UpdatePartitionRequest request = UpdatePartitionRequest.builder().catalogId(catalogId)
                    .databaseName(databaseName)
                    .tableName(tableName)
                    .partitionValueList(partition.getValues())
                    .partitionInput(finalPartitionInput).build();
            awsSyncRequest(glueClient::updatePartition, request, stats.getUpdatePartition());
        }
        catch (EntityNotFoundException e) {
            throw new PartitionNotFoundException(new SchemaTableName(databaseName, tableName), partitionValues);
        }
        catch (AwsServiceException e) {
            throw new PrestoException(HIVE_METASTORE_ERROR, e);
        }
    }

    @Override
    public Optional<List<String>> getAllTables(MetastoreContext metastoreContext, String databaseName)
    {
        try {
            List<String> tableNames = new ArrayList<>();
            GetTablesRequest request = GetTablesRequest.builder().catalogId(catalogId).databaseName(databaseName).build();
            do {
                GetTablesResponse result = awsSyncRequest(glueClient::getTables, request, stats.getGetTables());
                request = request.toBuilder().nextToken(result.nextToken()).build();
                result.tableList().forEach(table -> tableNames.add(table.name()));
            }
            while (request.nextToken() != null);

            return Optional.of(tableNames);
        }
        catch (EntityNotFoundException e) {
            // database does not exist
            return Optional.empty();
        }
        catch (AwsServiceException e) {
            throw new PrestoException(HIVE_METASTORE_ERROR, e);
        }
    }

    @Override
    public Optional<List<String>> getAllViews(MetastoreContext metastoreContext, String databaseName)
    {
        try {
            List<String> views = new ArrayList<>();
            GetTablesRequest request = GetTablesRequest.builder().catalogId(catalogId).databaseName(databaseName).build();

            do {
                GetTablesResponse result = awsSyncRequest(glueClient::getTables, request, stats.getGetTables());
                request = request.toBuilder().nextToken(result.nextToken()).build();
                result.tableList().stream()
                        .filter(table -> VIRTUAL_VIEW.name().equals(table.tableType()))
                        .forEach(table -> views.add(table.name()));
            }
            while (request.nextToken() != null);

            return Optional.of(views);
        }
        catch (EntityNotFoundException e) {
            // database does not exist
            return Optional.empty();
        }
        catch (AwsServiceException e) {
            throw new PrestoException(HIVE_METASTORE_ERROR, e);
        }
    }

    @Override
    public void createDatabase(MetastoreContext metastoreContext, Database database)
    {
        if (!database.getLocation().isPresent() && defaultDir.isPresent()) {
            String databaseLocation = new Path(defaultDir.get(), database.getDatabaseName()).toString();
            database = Database.builder(database)
                    .setLocation(Optional.of(databaseLocation))
                    .build();
        }

        try {
            DatabaseInput databaseInput = GlueInputConverter.convertDatabase(database);
            CreateDatabaseRequest request = CreateDatabaseRequest.builder()
                    .catalogId(catalogId)
                    .databaseInput(databaseInput)
                    .build();
            awsSyncRequest(glueClient::createDatabase, request, stats.getCreateDatabase());
        }
        catch (AlreadyExistsException e) {
            throw new SchemaAlreadyExistsException(database.getDatabaseName());
        }
        catch (AwsServiceException e) {
            throw new PrestoException(HIVE_METASTORE_ERROR, e);
        }

        if (database.getLocation().isPresent()) {
            createDirectory(hdfsContext, hdfsEnvironment, new Path(database.getLocation().get()));
        }
    }

    @Override
    public void dropDatabase(MetastoreContext metastoreContext, String databaseName)
    {
        try {
            DeleteDatabaseRequest request = DeleteDatabaseRequest.builder()
                    .catalogId(catalogId)
                    .name(databaseName).build();
            awsSyncRequest(glueClient::deleteDatabase, request, stats.getDeleteDatabase());
        }
        catch (EntityNotFoundException e) {
            throw new SchemaNotFoundException(databaseName);
        }
        catch (AwsServiceException e) {
            throw new PrestoException(HIVE_METASTORE_ERROR, e);
        }
    }

    @Override
    public void renameDatabase(MetastoreContext metastoreContext, String databaseName, String newDatabaseName)
    {
        try {
            Database database = getDatabase(metastoreContext, databaseName).orElseThrow(() -> new SchemaNotFoundException(databaseName));
            DatabaseInput renamedDatabase = GlueInputConverter.convertDatabase(database).toBuilder().name(newDatabaseName).build();
            UpdateDatabaseRequest request = UpdateDatabaseRequest.builder()
                    .catalogId(catalogId)
                    .name(databaseName)
                    .databaseInput(renamedDatabase).build();
            awsSyncRequest(glueClient::updateDatabase, request, stats.getUpdateDatabase());
        }
        catch (AwsServiceException e) {
            throw new PrestoException(HIVE_METASTORE_ERROR, e);
        }
    }

    @Override
    public MetastoreOperationResult createTable(MetastoreContext metastoreContext, Table table, PrincipalPrivileges principalPrivileges)
    {
        try {
            TableInput input = GlueInputConverter.convertTable(table);
            CreateTableRequest request = CreateTableRequest.builder()
                    .catalogId(catalogId)
                    .databaseName(table.getDatabaseName())
                    .tableInput(input).build();
            awsSyncRequest(glueClient::createTable, request, stats.getCreateTable());
            return EMPTY_RESULT;
        }
        catch (AlreadyExistsException e) {
            throw new TableAlreadyExistsException(new SchemaTableName(table.getDatabaseName(), table.getTableName()));
        }
        catch (EntityNotFoundException e) {
            throw new SchemaNotFoundException(table.getDatabaseName());
        }
        catch (AwsServiceException e) {
            throw new PrestoException(HIVE_METASTORE_ERROR, e);
        }
    }

    @Override
    public void dropTable(MetastoreContext metastoreContext, String databaseName, String tableName, boolean deleteData)
    {
        Table table = getTableOrElseThrow(metastoreContext, databaseName, tableName);

        try {
            DeleteTableRequest request = DeleteTableRequest.builder()
                    .catalogId(catalogId)
                    .databaseName(databaseName)
                    .name(tableName).build();
            awsSyncRequest(glueClient::deleteTable, request, stats.getDeleteTable());
        }
        catch (AwsServiceException e) {
            throw new PrestoException(HIVE_METASTORE_ERROR, e);
        }

        String tableLocation = table.getStorage().getLocation();
        if (deleteData && isManagedTable(table.getTableType().name()) && !isNullOrEmpty(tableLocation)) {
            deleteDirectoryRecursively(hdfsContext, hdfsEnvironment, new Path(tableLocation), true);
        }
    }

    @Override
    public MetastoreOperationResult replaceTable(MetastoreContext metastoreContext, String databaseName, String tableName, Table newTable, PrincipalPrivileges principalPrivileges)
    {
        try {
            TableInput newTableInput = GlueInputConverter.convertTable(newTable);
            UpdateTableRequest request = UpdateTableRequest.builder()
                    .catalogId(catalogId)
                    .databaseName(databaseName)
                    .tableInput(newTableInput).build();
            awsSyncRequest(glueClient::updateTable, request, stats.getUpdateTable());
            return EMPTY_RESULT;
        }
        catch (EntityNotFoundException e) {
            throw new TableNotFoundException(new SchemaTableName(databaseName, tableName));
        }
        catch (AwsServiceException e) {
            throw new PrestoException(HIVE_METASTORE_ERROR, e);
        }
    }

    @Override
    public MetastoreOperationResult renameTable(MetastoreContext metastoreContext, String databaseName, String tableName, String newDatabaseName, String newTableName)
    {
        throw new PrestoException(NOT_SUPPORTED, "Table rename is not yet supported by Glue service");
    }

    @Override
    public MetastoreOperationResult addColumn(MetastoreContext metastoreContext, String databaseName, String tableName, String columnName, HiveType columnType, String columnComment)
    {
        software.amazon.awssdk.services.glue.model.Table table = getGlueTableOrElseThrow(databaseName, tableName);
        ImmutableList.Builder<software.amazon.awssdk.services.glue.model.Column> newDataColumns = ImmutableList.builder();
        newDataColumns.addAll(table.storageDescriptor().columns());
        newDataColumns.add(convertColumn(new Column(columnName, columnType, Optional.ofNullable(columnComment), Optional.empty())));
        StorageDescriptor storageDescriptor = table.storageDescriptor().toBuilder().columns(newDataColumns.build()).build();
        table = table.toBuilder().storageDescriptor(storageDescriptor).build();
        replaceGlueTable(databaseName, tableName, table);
        return EMPTY_RESULT;
    }

    @Override
    public MetastoreOperationResult renameColumn(MetastoreContext metastoreContext, String databaseName, String tableName, String oldColumnName, String newColumnName)
    {
        software.amazon.awssdk.services.glue.model.Table table = getGlueTableOrElseThrow(databaseName, tableName);
        if (table.partitionKeys() != null && table.partitionKeys().stream().anyMatch(c -> c.name().equals(oldColumnName))) {
            throw new PrestoException(NOT_SUPPORTED, "Renaming partition columns is not supported");
        }
        ImmutableList.Builder<software.amazon.awssdk.services.glue.model.Column> newDataColumns = ImmutableList.builder();
        for (software.amazon.awssdk.services.glue.model.Column column : table.storageDescriptor().columns()) {
            if (column.name().equals(oldColumnName)) {
                newDataColumns.add(software.amazon.awssdk.services.glue.model.Column.builder()
                        .name(newColumnName)
                        .type(column.type())
                        .comment(column.comment()).build());
            }
            else {
                newDataColumns.add(column);
            }
        }
        StorageDescriptor storageDescriptor = table.storageDescriptor().toBuilder().columns(newDataColumns.build()).build();
        table = table.toBuilder().storageDescriptor(storageDescriptor).build();
        replaceGlueTable(databaseName, tableName, table);
        return EMPTY_RESULT;
    }

    @Override
    public MetastoreOperationResult dropColumn(MetastoreContext metastoreContext, String databaseName, String tableName, String columnName)
    {
        verifyCanDropColumn(this, metastoreContext, databaseName, tableName, columnName);
        software.amazon.awssdk.services.glue.model.Table table = getGlueTableOrElseThrow(databaseName, tableName);

        ImmutableList.Builder<software.amazon.awssdk.services.glue.model.Column> newDataColumns = ImmutableList.builder();
        boolean found = false;
        for (software.amazon.awssdk.services.glue.model.Column column : table.storageDescriptor().columns()) {
            if (column.name().equals(columnName)) {
                found = true;
            }
            else {
                newDataColumns.add(column);
            }
        }

        if (!found) {
            SchemaTableName name = new SchemaTableName(databaseName, tableName);
            throw new ColumnNotFoundException(name, columnName);
        }
        StorageDescriptor storageDescriptor = table.storageDescriptor().toBuilder().columns(newDataColumns.build()).build();
        table = table.toBuilder().storageDescriptor(storageDescriptor).build();
        replaceGlueTable(databaseName, tableName, table);

        return EMPTY_RESULT;
    }

    private void replaceGlueTable(String databaseName, String tableName, software.amazon.awssdk.services.glue.model.Table newTable)
    {
        try {
            UpdateTableRequest request = UpdateTableRequest.builder()
                    .catalogId(catalogId)
                    .databaseName(databaseName)
                    .tableInput(toTableInput(newTable)).build();
            awsSyncRequest(glueClient::updateTable, request, stats.getUpdateTable());
        }
        catch (EntityNotFoundException e) {
            throw new TableNotFoundException(new SchemaTableName(databaseName, tableName));
        }
        catch (AwsServiceException e) {
            throw new PrestoException(HIVE_METASTORE_ERROR, e);
        }
    }

    @Override
    public Optional<Partition> getPartition(MetastoreContext metastoreContext, String databaseName, String tableName, List<String> partitionValues)
    {
        try {
            GetPartitionRequest request = GetPartitionRequest.builder()
                    .catalogId(catalogId)
                    .databaseName(databaseName)
                    .tableName(tableName)
                    .partitionValues(partitionValues).build();
            GetPartitionResponse result = awsSyncRequest(glueClient::getPartition, request, stats.getGetPartition());
            return Optional.of(new GluePartitionConverter(databaseName, tableName).apply(result.partition()));
        }
        catch (EntityNotFoundException e) {
            return Optional.empty();
        }
        catch (AwsServiceException e) {
            throw new PrestoException(HIVE_METASTORE_ERROR, e);
        }
    }

    @Override
    public Optional<List<String>> getPartitionNames(MetastoreContext metastoreContext, String databaseName, String tableName)
    {
        Table table = getTableOrElseThrow(metastoreContext, databaseName, tableName);
        List<Partition> partitions = getPartitions(databaseName, tableName, WILDCARD_EXPRESSION);
        return Optional.of(buildPartitionNames(table.getPartitionColumns(), partitions));
    }

    /**
     * <pre>
     * Ex: Partition keys = ['a', 'b', 'c']
     *     Valid partition values:
     *     ['1','2','3'] or
     *     ['', '2', '']
     * </pre>
     *
     * @param partitionPredicates Full or partial list of partition values to filter on. Keys without filter will be empty strings.
     * @return a list of partition names.
     */
    @Override
    public List<String> getPartitionNamesByFilter(
            MetastoreContext metastoreContext,
            String databaseName,
            String tableName,
            Map<Column, Domain> partitionPredicates)
    {
        Table table = getTableOrElseThrow(metastoreContext, databaseName, tableName);
        String expression = buildGlueExpression(partitionPredicates);
        List<Partition> partitions = getPartitions(databaseName, tableName, expression);
        return buildPartitionNames(table.getPartitionColumns(), partitions);
    }

    @Override
    public List<PartitionNameWithVersion> getPartitionNamesWithVersionByFilter(
            MetastoreContext metastoreContext,
            String databaseName,
            String tableName,
            Map<Column, Domain> partitionPredicates)
    {
        throw new UnsupportedOperationException();
    }

    private List<Partition> getPartitions(String databaseName, String tableName, String expression)
    {
        if (partitionSegments == 1) {
            return getPartitions(databaseName, tableName, expression, null);
        }

        // Do parallel partition fetch.
        CompletionService<List<Partition>> completionService = new ExecutorCompletionService<>(executor);
        for (int i = 0; i < partitionSegments; i++) {
            Segment segment = Segment.builder().segmentNumber(i).totalSegments(partitionSegments).build();
            completionService.submit(() -> getPartitions(databaseName, tableName, expression, segment));
        }

        List<Partition> partitions = new ArrayList<>();
        try {
            for (int i = 0; i < partitionSegments; i++) {
                Future<List<Partition>> futurePartitions = completionService.take();
                partitions.addAll(futurePartitions.get());
            }
        }
        catch (ExecutionException | InterruptedException e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw new PrestoException(HIVE_METASTORE_ERROR, "Failed to fetch partitions from Glue Data Catalog", e);
        }

        partitions.sort(PARTITION_COMPARATOR);
        return partitions;
    }

    private List<Partition> getPartitions(String databaseName, String tableName, String expression, @Nullable Segment segment)
    {
        try {
            GluePartitionConverter converter = new GluePartitionConverter(databaseName, tableName);
            ArrayList<Partition> partitions = new ArrayList<>();
            GetPartitionsRequest request = GetPartitionsRequest.builder()
                    .catalogId(catalogId)
                    .databaseName(databaseName)
                    .tableName(tableName)
                    .expression(expression)
                    .segment(segment)
                    .maxResults(AWS_GLUE_GET_PARTITIONS_MAX_RESULTS).build();

            do {
                GetPartitionsResponse result = awsSyncRequest(glueClient::getPartitions, request, stats.getGetPartitions());
                request = request.toBuilder().nextToken(result.nextToken()).build();
                partitions.ensureCapacity(partitions.size() + result.partitions().size());
                result.partitions().stream().map(converter).forEach(partitions::add);
            }
            while (request.nextToken() != null);

            return partitions;
        }
        catch (AwsServiceException e) {
            throw new PrestoException(HIVE_METASTORE_ERROR, e);
        }
    }

    private static List<String> buildPartitionNames(List<Column> partitionColumns, List<Partition> partitions)
    {
        return mappedCopy(partitions, partition -> makePartName(partitionColumns, partition.getValues()));
    }

    /**
     * <pre>
     * Ex: Partition keys = ['a', 'b']
     *     Partition names = ['a=1/b=2', 'a=2/b=2']
     * </pre>
     *
     * @param partitionNames List of full partition names
     * @return Mapping of partition name to partition object
     */
    @Override
    public Map<String, Optional<Partition>> getPartitionsByNames(MetastoreContext metastoreContext, String databaseName, String tableName, List<String> partitionNames)
    {
        requireNonNull(partitionNames, "partitionNames is null");
        if (partitionNames.isEmpty()) {
            return ImmutableMap.of();
        }

        List<Partition> partitions = batchGetPartition(databaseName, tableName, partitionNames);

        Map<String, List<String>> partitionNameToPartitionValuesMap = partitionNames.stream()
                .collect(toMap(identity(), MetastoreUtil::toPartitionValues));
        Map<List<String>, Partition> partitionValuesToPartitionMap = partitions.stream()
                .collect(toMap(Partition::getValues, identity()));

        ImmutableMap.Builder<String, Optional<Partition>> resultBuilder = ImmutableMap.builder();
        for (Entry<String, List<String>> entry : partitionNameToPartitionValuesMap.entrySet()) {
            Partition partition = partitionValuesToPartitionMap.get(entry.getValue());
            resultBuilder.put(entry.getKey(), Optional.ofNullable(partition));
        }
        return resultBuilder.build();
    }

    private List<Partition> batchGetPartition(String databaseName, String tableName, List<String> partitionNames)
    {
        GlueCatalogApiStats.MetricsAsyncHandler metricsAsyncHandler = stats.getBatchGetPartitions().metricsAsyncHandler();
        try {
            List<Future<BatchGetPartitionResponse>> batchGetPartitionFutures = new ArrayList<>();

            for (List<String> partitionNamesBatch : Lists.partition(partitionNames, BATCH_GET_PARTITION_MAX_PAGE_SIZE)) {
                List<PartitionValueList> partitionValuesBatch = mappedCopy(partitionNamesBatch, partitionName -> PartitionValueList.builder().values(toPartitionValues(partitionName)).build());
                batchGetPartitionFutures.add(glueClient.batchGetPartition(BatchGetPartitionRequest.builder()
                        .catalogId(catalogId)
                        .databaseName(databaseName)
                        .tableName(tableName)
                        .partitionsToGet(partitionValuesBatch).build())
                        .whenCompleteAsync((result, error) -> {
                            if (result != null) {
                                metricsAsyncHandler.onSuccess();
                            }
                            else {
                                metricsAsyncHandler.onError(error);
                            }
                        }));
            }

            GluePartitionConverter converter = new GluePartitionConverter(databaseName, tableName);
            ImmutableList.Builder<Partition> resultsBuilder = ImmutableList.builderWithExpectedSize(partitionNames.size());
            for (Future<BatchGetPartitionResponse> future : batchGetPartitionFutures) {
                future.get().partitions().stream()
                        .map(converter)
                        .forEach(resultsBuilder::add);
            }

            return resultsBuilder.build();
        }
        catch (AwsServiceException | InterruptedException | ExecutionException e) {
            metricsAsyncHandler.onError(e);
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw new PrestoException(HIVE_METASTORE_ERROR, e);
        }
    }

    @Override
    public MetastoreOperationResult addPartitions(MetastoreContext metastoreContext, String databaseName, String tableName, List<PartitionWithStatistics> partitions)
    {
        GlueCatalogApiStats.MetricsAsyncHandler metricsAsyncHandler = stats.getBatchCreatePartitions().metricsAsyncHandler();
        try {
            List<Future<BatchCreatePartitionResponse>> futures = new ArrayList<>();

            for (List<PartitionWithStatistics> partitionBatch : Lists.partition(partitions, BATCH_CREATE_PARTITION_MAX_PAGE_SIZE)) {
                List<PartitionInput> partitionInputs = mappedCopy(partitionBatch, GlueInputConverter::convertPartition);
                futures.add(glueClient.batchCreatePartition(BatchCreatePartitionRequest.builder()
                        .catalogId(catalogId)
                        .databaseName(databaseName)
                        .tableName(tableName)
                        .partitionInputList(partitionInputs).build())
                        .whenCompleteAsync((result, error) -> {
                            if (result != null) {
                                metricsAsyncHandler.onSuccess();
                            }
                            else {
                                metricsAsyncHandler.onError(error);
                            }
                        }));
            }

            for (Future<BatchCreatePartitionResponse> future : futures) {
                BatchCreatePartitionResponse result = future.get();
                propagatePartitionErrorToPrestoException(databaseName, tableName, result.errors());
            }

            return EMPTY_RESULT;
        }
        catch (AwsServiceException | InterruptedException | ExecutionException e) {
            metricsAsyncHandler.onError(e);
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw new PrestoException(HIVE_METASTORE_ERROR, e);
        }
    }

    private static void propagatePartitionErrorToPrestoException(String databaseName, String tableName, List<PartitionError> partitionErrors)
    {
        if (partitionErrors != null && !partitionErrors.isEmpty()) {
            ErrorDetail errorDetail = partitionErrors.get(0).errorDetail();
            String glueExceptionCode = errorDetail.errorCode();

            switch (glueExceptionCode) {
                case "AlreadyExistsException":
                    throw new PrestoException(ALREADY_EXISTS, errorDetail.errorMessage());
                case "EntityNotFoundException":
                    throw new TableNotFoundException(new SchemaTableName(databaseName, tableName), errorDetail.errorMessage());
                default:
                    throw new PrestoException(HIVE_METASTORE_ERROR, errorDetail.errorCode() + ": " + errorDetail.errorMessage());
            }
        }
    }

    @Override
    public void dropPartition(MetastoreContext metastoreContext, String databaseName, String tableName, List<String> parts, boolean deleteData)
    {
        Table table = getTableOrElseThrow(metastoreContext, databaseName, tableName);
        Partition partition = getPartition(metastoreContext, databaseName, tableName, parts)
                .orElseThrow(() -> new PartitionNotFoundException(new SchemaTableName(databaseName, tableName), parts));

        try {
            DeletePartitionRequest request = DeletePartitionRequest.builder()
                    .catalogId(catalogId)
                    .databaseName(databaseName)
                    .tableName(tableName)
                    .partitionValues(parts).build();
            awsSyncRequest(glueClient::deletePartition, request, stats.getDeletePartition());
        }
        catch (AwsServiceException e) {
            throw new PrestoException(HIVE_METASTORE_ERROR, e);
        }
        String partLocation = partition.getStorage().getLocation();
        if (deleteData && isManagedTable(table.getTableType().name()) && !isNullOrEmpty(partLocation)) {
            deleteDirectoryRecursively(hdfsContext, hdfsEnvironment, new Path(partLocation), true);
        }
    }

    @Override
    public MetastoreOperationResult alterPartition(MetastoreContext metastoreContext, String databaseName, String tableName, PartitionWithStatistics partition)
    {
        try {
            PartitionInput newPartition = GlueInputConverter.convertPartition(partition);
            UpdatePartitionRequest request = UpdatePartitionRequest.builder()
                    .catalogId(catalogId)
                    .databaseName(databaseName)
                    .tableName(tableName)
                    .partitionInput(newPartition)
                    .partitionValueList(partition.getPartition().getValues()).build();
            awsSyncRequest(glueClient::updatePartition, request, stats.getUpdatePartition());
            return EMPTY_RESULT;
        }
        catch (EntityNotFoundException e) {
            throw new PartitionNotFoundException(new SchemaTableName(databaseName, tableName), partition.getPartition().getValues());
        }
        catch (AwsServiceException e) {
            throw new PrestoException(HIVE_METASTORE_ERROR, e);
        }
    }

    @Override
    public void createRole(MetastoreContext metastoreContext, String role, String grantor)
    {
        throw new PrestoException(NOT_SUPPORTED, "createRole is not supported by Glue");
    }

    @Override
    public void dropRole(MetastoreContext metastoreContext, String role)
    {
        throw new PrestoException(NOT_SUPPORTED, "dropRole is not supported by Glue");
    }

    @Override
    public Set<String> listRoles(MetastoreContext metastoreContext)
    {
        return ImmutableSet.of(PUBLIC_ROLE_NAME);
    }

    @Override
    public void grantRoles(MetastoreContext metastoreContext, Set<String> roles, Set<PrestoPrincipal> grantees, boolean withAdminOption, PrestoPrincipal grantor)
    {
        throw new PrestoException(NOT_SUPPORTED, "grantRoles is not supported by Glue");
    }

    @Override
    public void revokeRoles(MetastoreContext metastoreContext, Set<String> roles, Set<PrestoPrincipal> grantees, boolean adminOptionFor, PrestoPrincipal grantor)
    {
        throw new PrestoException(NOT_SUPPORTED, "revokeRoles is not supported by Glue");
    }

    @Override
    public Set<RoleGrant> listRoleGrants(MetastoreContext metastoreContext, PrestoPrincipal principal)
    {
        if (principal.getType() == USER) {
            return ImmutableSet.of(new RoleGrant(principal, PUBLIC_ROLE_NAME, false));
        }
        return ImmutableSet.of();
    }

    @Override
    public void grantTablePrivileges(MetastoreContext metastoreContext, String databaseName, String tableName, PrestoPrincipal grantee, Set<HivePrivilegeInfo> privileges)
    {
        throw new PrestoException(NOT_SUPPORTED, "grantTablePrivileges is not supported by Glue");
    }

    @Override
    public void revokeTablePrivileges(MetastoreContext metastoreContext, String databaseName, String tableName, PrestoPrincipal grantee, Set<HivePrivilegeInfo> privileges)
    {
        throw new PrestoException(NOT_SUPPORTED, "revokeTablePrivileges is not supported by Glue");
    }

    @Override
    public Set<HivePrivilegeInfo> listTablePrivileges(MetastoreContext metastoreContext, String databaseName, String tableName, PrestoPrincipal principal)
    {
        throw new PrestoException(NOT_SUPPORTED, "listTablePrivileges is not supported by Glue");
    }

    @Override
    public void setPartitionLeases(MetastoreContext metastoreContext, String databaseName, String tableName, Map<String, String> partitionNameToLocation, Duration leaseDuration)
    {
        throw new PrestoException(NOT_SUPPORTED, "setPartitionLeases is not supported by Glue");
    }
}
