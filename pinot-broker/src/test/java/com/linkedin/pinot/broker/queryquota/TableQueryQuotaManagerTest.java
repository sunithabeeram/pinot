/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.broker.queryquota;

import com.linkedin.pinot.common.config.QuotaConfig;
import com.linkedin.pinot.common.config.TableConfig;
import com.linkedin.pinot.common.config.TableNameBuilder;
import com.linkedin.pinot.common.metadata.ZKMetadataProvider;
import com.linkedin.pinot.common.utils.StringUtil;
import com.linkedin.pinot.common.utils.ZkStarter;
import java.io.IOException;
import org.apache.commons.lang.StringUtils;
import org.apache.helix.HelixManager;
import org.apache.helix.InstanceType;
import org.apache.helix.ZNRecord;
import org.apache.helix.manager.zk.ZKHelixManager;
import org.apache.helix.manager.zk.ZNRecordSerializer;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.manager.zk.ZkClient;
import org.apache.helix.model.ExternalView;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.json.JSONException;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import static com.linkedin.pinot.common.utils.CommonConstants.Helix.*;


public class TableQueryQuotaManagerTest {
  private ZkHelixPropertyStore<ZNRecord> _propertyStore;
  private HelixManager _helixManager;
  private TableQueryQuotaManager _tableQueryQuotaManager;
  private ZkStarter.ZookeeperInstance _zookeeperInstance;
  private static String OFFLINE_TABLE_NAME = "testTable_OFFLINE";
  private static String REALTIME_TABLE_NAME = "testTable_REALTIME";


  @BeforeTest
  public void beforeTest() {
    _zookeeperInstance = ZkStarter.startLocalZkServer();
    String helixClusterName = "TestTableQueryQuotaManagerService";

    _helixManager = initHelixManager(helixClusterName);
    _propertyStore = _helixManager.getHelixPropertyStore();

    _tableQueryQuotaManager = new TableQueryQuotaManager(_helixManager);
  }

  private HelixManager initHelixManager(String helixClusterName) {
    return new FakeHelixManager(helixClusterName, helixClusterName + "_1", InstanceType.CONTROLLER, ZkStarter.DEFAULT_ZK_STR);
  }

  public class FakeHelixManager extends ZKHelixManager {
    private ZkHelixPropertyStore<ZNRecord> _propertyStore;

    FakeHelixManager(String clusterName, String instanceName, InstanceType instanceType, String zkAddress) {
      super(clusterName, instanceName, instanceType, zkAddress);
      super._zkclient = new ZkClient(StringUtil.join("/", StringUtils.chomp(ZkStarter.DEFAULT_ZK_STR, "/")),
          ZkClient.DEFAULT_SESSION_TIMEOUT, ZkClient.DEFAULT_CONNECTION_TIMEOUT, new ZNRecordSerializer());
      _zkclient.deleteRecursive("/" + clusterName + "/PROPERTYSTORE");
      _zkclient.createPersistent("/" + clusterName + "/PROPERTYSTORE", true);
      setPropertyStore(clusterName);
    }

    void setPropertyStore(String clusterName) {
      _propertyStore = new ZkHelixPropertyStore<>(new ZkBaseDataAccessor<ZNRecord>(_zkclient),
          "/" + clusterName + "/PROPERTYSTORE", null);
    }

    public void resetPropertyStore() {
      _propertyStore.reset();
    }

    public void closeZkClient() {
      _zkclient.close();
    }
  }

  @AfterMethod
  public void afterMethod() {
    if (_helixManager instanceof FakeHelixManager) {
      ((FakeHelixManager) _helixManager).resetPropertyStore();
    }
    _tableQueryQuotaManager.cleanUpRateLimiterMap();
  }

  @AfterTest
  public void afterTest() {
    if (_helixManager instanceof FakeHelixManager) {
      ((FakeHelixManager) _helixManager).closeZkClient();
    }
    ZkStarter.stopLocalZkServer(_zookeeperInstance);
  }

  @Test
  public void testOfflineTableNotnullQuota() throws Exception {
    ExternalView brokerResource = generateBrokerResource(OFFLINE_TABLE_NAME);
    TableConfig tableConfig = generateDefaultTableConfig(OFFLINE_TABLE_NAME);
    setQps(tableConfig);
    _tableQueryQuotaManager.initTableQueryQuota(tableConfig, brokerResource);
    Assert.assertEquals(_tableQueryQuotaManager.getRateLimiterMapSize(), 1);
  }

  @Test
  public void testOfflineTableWithNullQuotaAndNoRealtimeTableConfig() throws Exception {
    ExternalView brokerResource = generateBrokerResource(OFFLINE_TABLE_NAME);
    TableConfig tableConfig = generateDefaultTableConfig(OFFLINE_TABLE_NAME);
    _tableQueryQuotaManager.initTableQueryQuota(tableConfig, brokerResource);
    Assert.assertEquals(_tableQueryQuotaManager.getRateLimiterMapSize(), 0);
  }

  @Test
  public void testOfflineTableWithNullQuotaButWithRealtimeTableConfigNullQpsConfig() throws Exception {
    ZNRecord znRecord = new ZNRecord("broker_instance_1");
    znRecord.setSimpleField("tableName", "testTable");
    znRecord.setSimpleField("tableType", "REALTIME");
    znRecord.setSimpleField("quota","{\"storage\":\"6G\"}");
    znRecord.setSimpleField("segmentsConfig", "{\"retentionTimeUnit\":\"DAYS\",\"retentionTimeValue\":\"1\",\"segmentPushFrequency\":\"daily\",\"segmentPushType\":\"APPEND\",\"replication\":\"3\",\"schemaName\":\"testSchema\",\"timeColumnName\":\"Date\",\"timeType\":\"MILLISECONDS\",\"segmentAssignmentStrategy\":\"BalanceNumSegmentAssignmentStrategy\",\"replicaGroupStrategyConfig\":null,\"hllConfig\":null,\"replicasPerPartition\":null}");
    znRecord.setSimpleField("tenants", "{\"broker\":\"testBroker\",\"server\":\"mt1\"}");
    znRecord.setSimpleField("metadata", "{\"customConfigs\":{\"d2Name\":\"CapReportingPinot\"}}");
    znRecord.setSimpleField("tableIndexConfig", "{\"segmentPartitionConfig\":null,\"sortedColumn\":[],\"invertedIndexColumns\":[\"jobsPosted\"],\"autoGeneratedInvertedIndex\":false,\"loadMode\":\"MMAP\",\"streamConfigs\":{},\"streamConsumptionConfig\":null,\"segmentFormatVersion\":\"v3\",\"columnMinMaxValueGeneratorMode\":null,\"noDictionaryColumns\":null,\"onHeapDictionaryColumns\":null,\"starTreeIndexSpec\":null,\"aggregateMetrics\":false}");
    ZKMetadataProvider.setRealtimeTableConfig(_propertyStore, REALTIME_TABLE_NAME, znRecord);

    ExternalView brokerResource = generateBrokerResource(OFFLINE_TABLE_NAME);
    TableConfig tableConfig = generateDefaultTableConfig(OFFLINE_TABLE_NAME);
    _tableQueryQuotaManager.initTableQueryQuota(tableConfig, brokerResource);
    Assert.assertEquals(_tableQueryQuotaManager.getRateLimiterMapSize(), 0);
  }

  @Test
  public void testOfflineTableWithNullQuotaButWithRealtimeTableConfigNotNullQpsConfig() throws Exception {
    ZNRecord znRecord = new ZNRecord("broker_instance_1");
    znRecord.setSimpleField("tableName", "testTable_REALTIME");
    znRecord.setSimpleField("tableType", "REALTIME");
    znRecord.setSimpleField("quota","{\"storage\":\"6G\", \"maxQueriesPerSecond\":\"100.00\"}");
    znRecord.setSimpleField("segmentsConfig", "{\"retentionTimeUnit\":\"DAYS\",\"retentionTimeValue\":\"1\",\"segmentPushFrequency\":\"daily\",\"segmentPushType\":\"APPEND\",\"replication\":\"3\",\"schemaName\":\"testSchema\",\"timeColumnName\":\"Date\",\"timeType\":\"MILLISECONDS\",\"segmentAssignmentStrategy\":\"BalanceNumSegmentAssignmentStrategy\",\"replicaGroupStrategyConfig\":null,\"hllConfig\":null,\"replicasPerPartition\":null}");
    znRecord.setSimpleField("tenants", "{\"broker\":\"testBroker\",\"server\":\"mt1\"}");
    znRecord.setSimpleField("metadata", "{\"customConfigs\":{\"d2Name\":\"CapReportingPinot\"}}");
    znRecord.setSimpleField("tableIndexConfig", "{\"segmentPartitionConfig\":null,\"sortedColumn\":[],\"invertedIndexColumns\":[\"jobsPosted\"],\"autoGeneratedInvertedIndex\":false,\"loadMode\":\"MMAP\",\"streamConfigs\":{},\"streamConsumptionConfig\":null,\"segmentFormatVersion\":\"v3\",\"columnMinMaxValueGeneratorMode\":null,\"noDictionaryColumns\":null,\"onHeapDictionaryColumns\":null,\"starTreeIndexSpec\":null,\"aggregateMetrics\":false}");
    ZKMetadataProvider.setRealtimeTableConfig(_propertyStore, REALTIME_TABLE_NAME, znRecord);

    ExternalView brokerResource = generateBrokerResource(REALTIME_TABLE_NAME);
    TableConfig tableConfig = generateDefaultTableConfig(OFFLINE_TABLE_NAME);
    _tableQueryQuotaManager.initTableQueryQuota(tableConfig, brokerResource);
    Assert.assertEquals(_tableQueryQuotaManager.getRateLimiterMapSize(), 1);
  }

  @Test
  public void testRealtimeTableNotnullQuota() throws Exception {
    ExternalView brokerResource = generateBrokerResource(REALTIME_TABLE_NAME);
    TableConfig tableConfig = generateDefaultTableConfig(REALTIME_TABLE_NAME);
    setQps(tableConfig);
    _tableQueryQuotaManager.initTableQueryQuota(tableConfig, brokerResource);
    Assert.assertEquals(_tableQueryQuotaManager.getRateLimiterMapSize(), 1);
  }

  @Test
  public void testRealtimeTableWithNullQuotaAndNoOfflineTableConfig() throws Exception {
    ExternalView brokerResource = generateBrokerResource(REALTIME_TABLE_NAME);
    TableConfig tableConfig = generateDefaultTableConfig(REALTIME_TABLE_NAME);
    _tableQueryQuotaManager.initTableQueryQuota(tableConfig, brokerResource);
    Assert.assertEquals(_tableQueryQuotaManager.getRateLimiterMapSize(), 0);
  }

  @Test
  public void testRealtimeTableWithNullQuotaButWithOfflineTableConfigNullQpsConfig() throws Exception {
    ZNRecord znRecord = new ZNRecord("broker_instance_1");
    znRecord.setSimpleField("tableName", "testTable");
    znRecord.setSimpleField("tableType", "OFFLINE");
    znRecord.setSimpleField("quota","{\"storage\":\"6G\"}");
    znRecord.setSimpleField("segmentsConfig", "{\"retentionTimeUnit\":\"DAYS\",\"retentionTimeValue\":\"1\",\"segmentPushFrequency\":\"daily\",\"segmentPushType\":\"APPEND\",\"replication\":\"3\",\"schemaName\":\"testSchema\",\"timeColumnName\":\"Date\",\"timeType\":\"MILLISECONDS\",\"segmentAssignmentStrategy\":\"BalanceNumSegmentAssignmentStrategy\",\"replicaGroupStrategyConfig\":null,\"hllConfig\":null,\"replicasPerPartition\":null}");
    znRecord.setSimpleField("tenants", "{\"broker\":\"testBroker\",\"server\":\"mt1\"}");
    znRecord.setSimpleField("metadata", "{\"customConfigs\":{\"d2Name\":\"CapReportingPinot\"}}");
    znRecord.setSimpleField("tableIndexConfig", "{\"segmentPartitionConfig\":null,\"sortedColumn\":[],\"invertedIndexColumns\":[\"jobsPosted\"],\"autoGeneratedInvertedIndex\":false,\"loadMode\":\"MMAP\",\"streamConfigs\":{},\"streamConsumptionConfig\":null,\"segmentFormatVersion\":\"v3\",\"columnMinMaxValueGeneratorMode\":null,\"noDictionaryColumns\":null,\"onHeapDictionaryColumns\":null,\"starTreeIndexSpec\":null,\"aggregateMetrics\":false}");
    ZKMetadataProvider.setOfflineTableConfig(_propertyStore, OFFLINE_TABLE_NAME, znRecord);

    ExternalView brokerResource = generateBrokerResource(REALTIME_TABLE_NAME);
    TableConfig tableConfig = generateDefaultTableConfig(REALTIME_TABLE_NAME);
    _tableQueryQuotaManager.initTableQueryQuota(tableConfig, brokerResource);
    Assert.assertEquals(_tableQueryQuotaManager.getRateLimiterMapSize(), 0);
  }

  @Test
  public void testRealtimeTableWithNullQuotaButWithOfflineTableConfigNotNullQpsConfig() throws Exception {
    ZNRecord znRecord = new ZNRecord("broker_instance_1");
    znRecord.setSimpleField("tableName", "testTable");
    znRecord.setSimpleField("tableType", "OFFLINE");
    znRecord.setSimpleField("quota","{\"storage\":\"6G\", \"maxQueriesPerSecond\":\"100.00\"}");
    znRecord.setSimpleField("segmentsConfig", "{\"retentionTimeUnit\":\"DAYS\",\"retentionTimeValue\":\"1\",\"segmentPushFrequency\":\"daily\",\"segmentPushType\":\"APPEND\",\"replication\":\"3\",\"schemaName\":\"testSchema\",\"timeColumnName\":\"Date\",\"timeType\":\"MILLISECONDS\",\"segmentAssignmentStrategy\":\"BalanceNumSegmentAssignmentStrategy\",\"replicaGroupStrategyConfig\":null,\"hllConfig\":null,\"replicasPerPartition\":null}");
    znRecord.setSimpleField("tenants", "{\"broker\":\"testBroker\",\"server\":\"mt1\"}");
    znRecord.setSimpleField("metadata", "{\"customConfigs\":{\"d2Name\":\"CapReportingPinot\"}}");
    znRecord.setSimpleField("tableIndexConfig", "{\"segmentPartitionConfig\":null,\"sortedColumn\":[],\"invertedIndexColumns\":[\"jobsPosted\"],\"autoGeneratedInvertedIndex\":false,\"loadMode\":\"MMAP\",\"streamConfigs\":{},\"streamConsumptionConfig\":null,\"segmentFormatVersion\":\"v3\",\"columnMinMaxValueGeneratorMode\":null,\"noDictionaryColumns\":null,\"onHeapDictionaryColumns\":null,\"starTreeIndexSpec\":null,\"aggregateMetrics\":false}");
    ZKMetadataProvider.setOfflineTableConfig(_propertyStore, OFFLINE_TABLE_NAME, znRecord);

    ExternalView brokerResource = generateBrokerResource(OFFLINE_TABLE_NAME);
    TableConfig tableConfig = generateDefaultTableConfig(REALTIME_TABLE_NAME);
    _tableQueryQuotaManager.initTableQueryQuota(tableConfig, brokerResource);
    Assert.assertEquals(_tableQueryQuotaManager.getRateLimiterMapSize(), 1);
  }

  @Test
  public void testInvalidQpsQuota() throws Exception {
    ExternalView brokerResource = generateBrokerResource(OFFLINE_TABLE_NAME);
    TableConfig tableConfig = generateDefaultTableConfig(OFFLINE_TABLE_NAME);
    // Set invalid qps quota
    QuotaConfig quotaConfig = new QuotaConfig();
    quotaConfig.setMaxQueriesPerSecond("InvalidQpsQuota");
    tableConfig.setQuotaConfig(quotaConfig);
    _tableQueryQuotaManager.initTableQueryQuota(tableConfig, brokerResource);
    Assert.assertEquals(_tableQueryQuotaManager.getRateLimiterMapSize(), 0);
  }

  @Test
  public void testInvalidNegativeQpsQuota() throws Exception {
    ExternalView brokerResource = generateBrokerResource(OFFLINE_TABLE_NAME);
    TableConfig tableConfig = generateDefaultTableConfig(OFFLINE_TABLE_NAME);
    // Set invalid negative qps quota
    QuotaConfig quotaConfig = new QuotaConfig();
    quotaConfig.setMaxQueriesPerSecond("-1.0");
    tableConfig.setQuotaConfig(quotaConfig);
    _tableQueryQuotaManager.initTableQueryQuota(tableConfig, brokerResource);
    Assert.assertEquals(_tableQueryQuotaManager.getRateLimiterMapSize(), 0);
  }

  @Test
  public void testNoBrokerResource() throws Exception {
    TableConfig tableConfig = generateDefaultTableConfig(OFFLINE_TABLE_NAME);
    setQps(tableConfig);
    _tableQueryQuotaManager.initTableQueryQuota(tableConfig, null);
    Assert.assertEquals(_tableQueryQuotaManager.getRateLimiterMapSize(), 0);
  }

  @Test
  public void testNoBrokerServiceOnBrokerResource() throws Exception {
    ExternalView brokerResource = new ExternalView(BROKER_RESOURCE_INSTANCE);
    TableConfig tableConfig = generateDefaultTableConfig(OFFLINE_TABLE_NAME);
    setQps(tableConfig);
    _tableQueryQuotaManager.initTableQueryQuota(tableConfig, brokerResource);
    Assert.assertEquals(_tableQueryQuotaManager.getRateLimiterMapSize(), 0);
  }

  @Test
  public void testNoOnlineBrokerServiceOnBrokerResource() throws Exception {
    ExternalView brokerResource = new ExternalView(BROKER_RESOURCE_INSTANCE);
    brokerResource.setState(OFFLINE_TABLE_NAME, "broker_instance_2", "OFFLINE");
    TableConfig tableConfig = generateDefaultTableConfig(OFFLINE_TABLE_NAME);
    setQps(tableConfig);
    _tableQueryQuotaManager.initTableQueryQuota(tableConfig, brokerResource);
    Assert.assertEquals(_tableQueryQuotaManager.getRateLimiterMapSize(), 0);
  }

  private TableConfig generateDefaultTableConfig(String tableName) throws IOException, JSONException {
    TableType tableType = TableNameBuilder.getTableTypeFromTableName(tableName);
    TableConfig.Builder builder = new TableConfig.Builder(tableType);
    builder.setTableName(tableName);
    return builder.build();
  }

  private void setQps(TableConfig tableConfig) {
    QuotaConfig quotaConfig = new QuotaConfig();
    quotaConfig.setMaxQueriesPerSecond("100.00");
    tableConfig.setQuotaConfig(quotaConfig);
  }

  private ExternalView generateBrokerResource(String tableName) {
    ExternalView brokerResource = new ExternalView(BROKER_RESOURCE_INSTANCE);
    brokerResource.setState(tableName, "broker_instance_1", "ONLINE");
    brokerResource.setState(tableName, "broker_instance_2", "OFFLINE");
    return brokerResource;
  }
}
