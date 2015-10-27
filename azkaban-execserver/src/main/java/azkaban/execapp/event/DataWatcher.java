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

/**
 * Abstract class to implement data set watcher
 */
public abstract class DataWatcher {
  protected Logger logger;

  private ExecutableNode node;
  protected Map<String, DataSetBlockingStatus> dataSetMapper =
    new ConcurrentHashMap<String, DataSetBlockingStatus>();
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

  /**
   * Return DataSetBlockingStatus for a Dataset if available, otherwise create
   * and assign a DataSetBlockingStatus and then return
   *
   * @param dataSetId
   * @return
   */
  public synchronized DataSetBlockingStatus getBlockingStatus(String dataSetId) {
    if (cancelWatch) {
      return null;
    }

    logger.info(String.format("Job %s is waiting on dataset %s",
      node.getNestedId(), dataSetId));
    DataSetBlockingStatus blockingStatus = dataSetMapper.get(dataSetId);
    if (blockingStatus == null) {
      blockingStatus = new DataSetBlockingStatus(dataSetId, false);
      dataSetMapper.put(dataSetId, blockingStatus);
    }

    return blockingStatus;
  }

  /**
   * Helper method to unblock all waiting dataset watchers
   */
  public synchronized void unblockAllWatches() {
    logger.debug("Unblocking all watches on " + node.getNestedId());
    cancelWatch = true;

    for (DataSetBlockingStatus status : dataSetMapper.values()) {
      logger.info("Unblocking " + status.getDataSetId());
      status.changeStatus(true);
      status.unblock();
    }

    logger.debug("Successfully unblocked all watches on " + node.getNestedId());
  }

  public boolean isWatchCancelled() {
    return cancelWatch;
  }

  public abstract void stopWatcher();
}
