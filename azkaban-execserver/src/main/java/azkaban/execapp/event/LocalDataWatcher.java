package azkaban.execapp.event;

import azkaban.execapp.JobRunner;

import java.io.File;
import java.util.Map.Entry;
import java.util.Timer;
import java.util.TimerTask;

/**
 * A very basic implementation to serve as POC for zsl data trigger from local
 * files without wild cards
 */
public class LocalDataWatcher extends DataWatcher {
  private static final int WAIT_INTERVAL = 5000; // 5 sec
  private Timer timer;

  public LocalDataWatcher(JobRunner runner) {
    super(runner.getNode());
    timer = new Timer();
    timer.schedule(getTimerTask(), WAIT_INTERVAL, WAIT_INTERVAL);
    // TODO: We need to have an expression resolver which maps wild card in
    // dataset path using variables like dates etc
  }

  @Override
  public void stopWatcher() {
    timer.cancel();
    super.unblockAllWatches();
  }

  /**
   * Get a TimerTask to reschedule Timer
   *
   * @return An anonymous TimerTask class
   */
  private TimerTask getTimerTask() {
    // anonymous class to timely iterate over all tracked file set
    return new TimerTask() {
      @Override
      public void run() {
        // TODO: Each job checking individually is not optimal. There should be
        // a central thread to check files in local/hdfs as there may be
        // multiple jobs waiting on same file and
        for (Entry<String, DataSetStatus> entry : map.entrySet()) {
          File file = new File(entry.getKey());
          entry.getValue().changeStatus(file.exists());
        }
      }
    };
  }
}
