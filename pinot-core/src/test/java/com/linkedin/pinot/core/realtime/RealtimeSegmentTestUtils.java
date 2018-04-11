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
package com.linkedin.pinot.core.realtime;

import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.common.metadata.segment.RealtimeSegmentZKMetadata;
import com.linkedin.pinot.core.indexsegment.mutable.MutableSegmentImpl;
import com.linkedin.pinot.core.io.writer.impl.DirectMemoryManager;
import com.linkedin.pinot.core.realtime.impl.RealtimeSegmentConfig;
import com.linkedin.pinot.core.realtime.impl.RealtimeSegmentStatsHistory;
import java.util.Collections;

import static org.mockito.Mockito.*;


public class RealtimeSegmentTestUtils {
  private RealtimeSegmentTestUtils() {
  }

  public static MutableSegmentImpl createRealtimeSegmentImpl(Schema schema, int sizeThresholdToFlushSegment,
      String segmentName, String streamName) {
    RealtimeSegmentStatsHistory statsHistory = mock(RealtimeSegmentStatsHistory.class);
    when(statsHistory.getEstimatedCardinality(anyString())).thenReturn(200);
    when(statsHistory.getEstimatedAvgColSize(anyString())).thenReturn(32);
    RealtimeSegmentConfig realtimeSegmentConfig = new RealtimeSegmentConfig.Builder().setSegmentName(segmentName)
        .setStreamName(streamName)
        .setSchema(schema)
        .setCapacity(sizeThresholdToFlushSegment)
        .setAvgNumMultiValues(2)
        .setNoDictionaryColumns(Collections.emptySet())
        .setInvertedIndexColumns(Collections.emptySet())
        .setRealtimeSegmentZKMetadata(new RealtimeSegmentZKMetadata())
        .setMemoryManager(new DirectMemoryManager(segmentName))
        .setStatsHistory(statsHistory)
        .build();
    return new MutableSegmentImpl(realtimeSegmentConfig);
  }
}
