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

/**
 * Class to represent dataset status
 */
public class DataSetBlockingStatus {
  private static final long WAIT_TIME = 5 * 60 * 1000;
  private final String dataSetId;
  private Boolean isReady;

  public DataSetBlockingStatus(String dataSetId, Boolean isReady) {
    this.dataSetId = dataSetId;
    this.isReady = isReady;
  }

  /**
   * Block current thread till we have data set ready
   *
   * @return
   */
  public Boolean blockOnReadyStatus() {
    while (!isReady) {
      synchronized (this) {
        try {
          this.wait(WAIT_TIME);
        } catch (InterruptedException e) {
        }
      }
    }

    return isReady;
  }

  /**
   * Unblock all threads waiting on this dataset
   */
  public void unblock() {
    synchronized (this) {
      this.notifyAll();
    }
  }

  /**
   * Update status of this dataset
   *
   * @param status
   */
  public void changeStatus(Boolean status) {
    synchronized (this) {
      this.isReady = status;
      if (isReady) {
        unblock();
      }
    }
  }

  public String getDataSetId() {
    return dataSetId;
  }

  public Boolean viewStatus() {
    return this.isReady;
  }
}
