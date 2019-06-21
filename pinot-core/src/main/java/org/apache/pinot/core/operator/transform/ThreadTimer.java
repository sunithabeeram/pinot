/**
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

package org.apache.pinot.core.operator.transform;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ThreadTimer {


  private static final Logger LOGGER = LoggerFactory.getLogger(ThreadTimer.class);

  private static ThreadMXBean _mxBean;
  private static boolean _threadCpuSupported;
  private static long NANOS_IN_MS = 1000_000L;
  private long startTimeMs;
  private long endTimeMs;

  static {
    _mxBean = ManagementFactory.getThreadMXBean();
    _threadCpuSupported = _mxBean.isThreadCpuTimeSupported();
    LOGGER.info("thread-cpu-supported: {}", _threadCpuSupported);
  }

  public ThreadTimer() {
    startTimeMs = -1;
    endTimeMs = -1;
  }

  public void start() {
    if (_threadCpuSupported) {
      startTimeMs = (_mxBean.getThreadCpuTime(Thread.currentThread().getId()))/NANOS_IN_MS;
    } else {
      startTimeMs = System.currentTimeMillis();
    }
  }

  public void end() {
    if (_threadCpuSupported) {
      endTimeMs = (_mxBean.getThreadCpuTime(Thread.currentThread().getId()))/NANOS_IN_MS;
    } else {
      endTimeMs = System.currentTimeMillis();
    }
  }

  public long getThreadCpuTime() {
    if (startTimeMs == -1 || endTimeMs == -1) {
      // invalid usage
      return -1;
    }

    return (endTimeMs - startTimeMs);
  }
}
