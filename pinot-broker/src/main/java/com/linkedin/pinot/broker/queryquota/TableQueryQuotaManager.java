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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.util.concurrent.RateLimiter;
import com.linkedin.pinot.common.config.QuotaConfig;
import com.linkedin.pinot.common.config.TableConfig;
import com.linkedin.pinot.common.config.TableNameBuilder;
import com.linkedin.pinot.common.metadata.ZKMetadataProvider;
import com.linkedin.pinot.common.utils.CommonConstants;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.Nonnull;
import org.apache.helix.HelixManager;
import org.apache.helix.ZNRecord;
import org.apache.helix.model.ExternalView;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class TableQueryQuotaManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(TableQueryQuotaManager.class);

  private final HelixManager _helixManager;
  private final Map<String, RateLimiter> _rateLimiterMap;

  public TableQueryQuotaManager(HelixManager helixManager) {
    _helixManager = helixManager;
    _rateLimiterMap  = new ConcurrentHashMap<>();
  }

  /**
   * Initialize dynamic rate limiter with table query quota.
   * @param tableConfig table config.
   * @param brokerResource broker resource which stores all the broker states of each table.
   * */
  public void initTableQueryQuota(TableConfig tableConfig, ExternalView brokerResource) {
    String tableName = tableConfig.getTableName();
    String rawTableName = TableNameBuilder.extractRawTableName(tableName);
    LOGGER.info("Initializing rate limiter for table {}", rawTableName);

    ZkHelixPropertyStore<ZNRecord> propertyStore = _helixManager.getHelixPropertyStore();

    CommonConstants.Helix.TableType tableType = TableNameBuilder.getTableTypeFromTableName(tableName);
    if (tableType == CommonConstants.Helix.TableType.OFFLINE) {
      QuotaConfig quotaConfig = tableConfig.getQuotaConfig();
      if (quotaConfig != null && !Strings.isNullOrEmpty(quotaConfig.getMaxQueriesPerSecond())) {
        createRateLimiter(tableName, rawTableName, brokerResource, quotaConfig);
      } else {
        String realtimeTableName = TableNameBuilder.REALTIME.tableNameWithType(rawTableName);
        TableConfig realTimeTableConfig = ZKMetadataProvider.getTableConfig(propertyStore, realtimeTableName);
        if (realTimeTableConfig == null) {
          return;
        }
        QuotaConfig realTimeTableQuotaConfig = realTimeTableConfig.getQuotaConfig();
        if (realTimeTableQuotaConfig != null && !Strings.isNullOrEmpty(realTimeTableQuotaConfig.getMaxQueriesPerSecond())) {
          createRateLimiter(realtimeTableName, rawTableName, brokerResource, realTimeTableQuotaConfig);
        }
      }
    } else {
      QuotaConfig quotaConfig = tableConfig.getQuotaConfig();
      if (quotaConfig != null && !Strings.isNullOrEmpty(quotaConfig.getMaxQueriesPerSecond())) {
        createRateLimiter(tableName, rawTableName, brokerResource, quotaConfig);
      } else {
        String offlineTableName = TableNameBuilder.OFFLINE.tableNameWithType(rawTableName);
        TableConfig offlineTableConfig = ZKMetadataProvider.getTableConfig(propertyStore, offlineTableName);
        if (offlineTableConfig == null) {
          return;
        }
        QuotaConfig offlineTableQuotaConfig = offlineTableConfig.getQuotaConfig();
        if (offlineTableQuotaConfig != null && !Strings.isNullOrEmpty(offlineTableQuotaConfig.getMaxQueriesPerSecond())) {
          createRateLimiter(offlineTableName, rawTableName, brokerResource, offlineTableQuotaConfig);
        }
      }
    }
  }

  /**
   * Create a rate limiter for a table.
   * @param tableName table name with table type.
   * @param rawTableName raw table name.
   * @param brokerResource broker resource which stores all the broker states of each table.
   * @param quotaConfig quota config of the table.
   * */
  private void createRateLimiter(String tableName, String rawTableName, ExternalView brokerResource, @Nonnull QuotaConfig quotaConfig) {
    if (brokerResource == null || quotaConfig.getMaxQueriesPerSecond() == null) {
      return;
    }
    Map<String, String> stateMap = brokerResource.getStateMap(tableName);
    if (stateMap == null) {
      LOGGER.warn("Failed to init qps quota: table {} is not in the broker resource!", tableName);
      return;
    }
    int onlineCount = 0;
    for (String state : stateMap.values()) {
      if (state.equals(CommonConstants.Helix.StateModel.SegmentOnlineOfflineStateModel.ONLINE)) {
        onlineCount++;
      }
    }
    // assert onlineCount > 0;
    if (onlineCount == 0) {
      LOGGER.warn("Failed to init qps quota: there's no online broker services for table {}!", tableName);
      return;
    }
    // Get the dynamic rate
    double overallRate;
    if (quotaConfig.isMaxQueriesPerSecondValid()) {
      overallRate = Double.parseDouble(quotaConfig.getMaxQueriesPerSecond());
    } else {
      LOGGER.error("Failed to init qps quota: error when parsing qps quota: {} for table: {}",
          quotaConfig.getMaxQueriesPerSecond(), tableName);
      return;
    }

    double perBrokerRate =  overallRate / onlineCount;
    RateLimiter rateLimiter = RateLimiter.create(perBrokerRate);
    _rateLimiterMap.put(rawTableName, rateLimiter);
    LOGGER.info("Rate limiter for table: {} has been initialized. Overall rate: {}. Per-broker rate: {}. Number of online broker instances: {}",
        rawTableName, overallRate, perBrokerRate, onlineCount);
  }

  /**
   * Acquire a token from rate limiter based on the table name.
   * @param tableName raw table name
   * @return true if there is no query quota specified for the table or a token can be acquired, otherwise return false.
   * */
  public boolean acquire(String tableName) {
    RateLimiter rateLimiter = _rateLimiterMap.get(tableName);
    if (rateLimiter == null) {
      // No qps quota is specified for this table.
      LOGGER.info("No qps quota is specified for table: {}", tableName);
      return true;
    } else if (rateLimiter.tryAcquire()) {
      LOGGER.info("Token is successfully acquired for table: {}. Per-broker rate: {}", tableName, rateLimiter.getRate());
      return true;
    } else {
      LOGGER.error("Quota is exceeded for table: {}. Per-broker rate: {}", tableName, rateLimiter.getRate());
      return false;
    }
  }

  @VisibleForTesting
  public int getRateLimiterMapSize() {
    return _rateLimiterMap.size();
  }

  @VisibleForTesting
  public void cleanUpRateLimiterMap() {
    _rateLimiterMap.clear();
  }

  public void processQueryQuotaChange() {
    // TODO: update rate
  }
}
