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
package com.linkedin.pinot.core.indexsegment.mutable;

import com.linkedin.pinot.common.data.DimensionFieldSpec;
import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.common.data.MetricFieldSpec;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.common.metadata.segment.RealtimeSegmentZKMetadata;
import com.linkedin.pinot.core.data.GenericRow;
import com.linkedin.pinot.core.io.writer.impl.MmapMemoryManager;
import com.linkedin.pinot.core.realtime.impl.RealtimeSegmentConfig;
import com.linkedin.pinot.core.realtime.impl.RealtimeSegmentStatsHistory;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.RandomStringUtils;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.mockito.Mockito.*;


public class MutableSegmentImplTest {
  private static final String DIMENSION_1 = "dim1";
  private static final String DIMENSION_2 = "dim2";
  private static final String METRIC_COLUMN = "metric";
  private static final String SEGMENT_NAME = "testSegment";
  private static final String KEY_SEPARATOR = "\t\t";
  private static final int NUM_ROWS = 10001;
  private static final Random RANDOM = new Random();

  private RealtimeSegmentConfig.Builder _configBuilder;
  private MutableSegment _mutableSegment;

  @BeforeClass
  public void setUp() {
    RealtimeSegmentStatsHistory statsHistory = mock(RealtimeSegmentStatsHistory.class);
    when(statsHistory.getEstimatedAvgColSize(anyString())).thenReturn(32);
    when(statsHistory.getEstimatedCardinality(anyString())).thenReturn(200);

    _configBuilder = new RealtimeSegmentConfig.Builder().setCapacity(1000000)
        .setMemoryManager(new MmapMemoryManager(FileUtils.getTempDirectoryPath(), SEGMENT_NAME))
        .setNoDictionaryColumns(new HashSet<>(Collections.singletonList(METRIC_COLUMN)))
        .setOffHeap(true)
        .setSchema(buildSchema())
        .setSegmentName(SEGMENT_NAME)
        .setStatsHistory(statsHistory)
        .setInvertedIndexColumns(new HashSet<>(Collections.singletonList(DIMENSION_1)))
        .setRealtimeSegmentZKMetadata(new RealtimeSegmentZKMetadata())
        .setAggregateMetrics(true);
  }

  @Test
  public void testAggregateMetrics() {
    _mutableSegment = new MutableSegmentImpl(_configBuilder.build());
    String[] stringValues = new String[10]; // 10 unique strings.
    for (int i = 0; i < stringValues.length; i++) {
      stringValues[i] = RandomStringUtils.random(10);
    }

    Map<String, Integer> expectedMap = new HashMap<>();
    for (int i = 0; i < NUM_ROWS; i++) {
      GenericRow row = new GenericRow();

      row.putField(DIMENSION_1, (long) RANDOM.nextInt(10)); // 10 unique values
      row.putField(DIMENSION_2, stringValues[RANDOM.nextInt(stringValues.length)]); // 10 unique values

      int metricValue = RANDOM.nextInt();
      row.putField(METRIC_COLUMN, metricValue);
      _mutableSegment.index(row);

      // Collect expected results.
      String key = buildKey(row);
      expectedMap.put(key, expectedMap.getOrDefault(key, 0) + metricValue);
    }

    int numDocsIndexed = _mutableSegment.getNumDocsIndexed();
    Assert.assertEquals(numDocsIndexed, expectedMap.size());

    GenericRow row = new GenericRow();
    for (int docId = 0; docId < numDocsIndexed; docId++) {
      _mutableSegment.getRecord(docId, row);
      String key = buildKey(row);
      Assert.assertEquals(row.getValue(METRIC_COLUMN), expectedMap.get(key));
    }
  }

  /**
   * Helper method to build key containing dimension column values.
   *
   * @param row Generic row to be used for building key.
   * @return String key for the given row.
   */
  private String buildKey(GenericRow row) {
    return String.valueOf(row.getValue(DIMENSION_1)) + KEY_SEPARATOR + row.getValue(DIMENSION_2);
  }

  /**
   * Helper method to build schema for test segment, containing two dimension columns and one metric column.
   *
   * @return Schema for test segment.
   */
  private Schema buildSchema() {
    Schema schema = new Schema();
    schema.addField(new DimensionFieldSpec(DIMENSION_1, FieldSpec.DataType.LONG, true));
    schema.addField(new DimensionFieldSpec(DIMENSION_2, FieldSpec.DataType.STRING, true));
    schema.addField(new MetricFieldSpec(METRIC_COLUMN, FieldSpec.DataType.INT));
    return schema;
  }

  @AfterClass
  public void tearDown() {
    _mutableSegment.destroy();
  }
}