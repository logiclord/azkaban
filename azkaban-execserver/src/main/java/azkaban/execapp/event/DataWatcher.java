/*
 * Copyright 2015 LinkedIn Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package azkaban.execapp.event;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;

import azkaban.executor.ExecutableNode;
import azkaban.executor.Status;

public abstract class DataWatcher {
  private Logger logger;

  private ExecutableNode node;
  protected Map<String, DataSetStatus> map =
    new ConcurrentHashMap<String, DataSetStatus>();
  private boolean cancelWatch = false;

  public DataWatcher(ExecutableNode node) {
    this.node = node;
  }

  public void setLogger(Logger logger) {
    this.logger = logger;
  }

  protected Logger getLogger() {
    return this.logger;
  }

  public synchronized DataSetStatus getBlockingStatus(String dataSetId) {
    if (cancelWatch) {
      return null;
    }

    DataSetStatus blockingStatus = map.get(dataSetId);
    if (blockingStatus == null) {
      blockingStatus = new DataSetStatus(dataSetId, false);
      map.put(dataSetId, blockingStatus);
    }

    return blockingStatus;
  }

  public synchronized void unblockAllWatches() {
    logger.info("Unblock all watches on " + node.getNestedId());
    cancelWatch = true;

    for (DataSetStatus status : map.values()) {
      logger.info("Unblocking " + status.getDataSetId());
      status.changeStatus(true);
      status.unblock();
    }

    logger.info("Successfully unblocked all watches on " + node.getNestedId());
  }

  public boolean isWatchCancelled() {
    return cancelWatch;
  }

  public abstract void stopWatcher();
}
