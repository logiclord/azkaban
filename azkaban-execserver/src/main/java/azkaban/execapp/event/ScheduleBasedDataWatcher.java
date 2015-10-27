package azkaban.execapp.event;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import azkaban.execapp.JobRunner;

/**
 * Abstract class to have a timer based polling to check data watcher
 */
public abstract class ScheduleBasedDataWatcher extends DataWatcher {
  private static final int DEFAULT_WAIT_INTERVAL = 30000; // 30 sec
  public static final String DATA_WAIT_INTERVAL = "dataWaitInterval";
  private ScheduledExecutorService timedService;
  private long waitInterval;

  public ScheduleBasedDataWatcher(JobRunner runner) {
    super(runner.getNode());
    timedService = Executors.newScheduledThreadPool(1);

    // user specified interval cannot be less then DEFAULT_WAIT_INTERVAL
    waitInterval =
      Math.max(DEFAULT_WAIT_INTERVAL,
        runner.getProps().getLong(DATA_WAIT_INTERVAL, DEFAULT_WAIT_INTERVAL));

    timedService.scheduleAtFixedRate(getScheduledTask(), DEFAULT_WAIT_INTERVAL,
      waitInterval, TimeUnit.MILLISECONDS);

    // TODO: We need to have an expression resolver which maps wild card in
    // dataset path using variables like dates etc
  }

  @Override
  public void stopWatcher() {
    timedService.shutdown();
    super.unblockAllWatches();
  }

  /**
   * This method must be implement by child class to process dataset mapper
   */
  protected abstract void processDataSets();

  /**
   * Get a TimerTask to reschedule Timer
   *
   * @return An anonymous TimerTask class
   */
  private Runnable getScheduledTask() {
    // anonymous class to timely iterate over all tracked file set
    return new Runnable() {
      @Override
      public void run() {
        try {
          processDataSets();
        } catch (Throwable th) {
          logger.error("Exception while processing dataset in DataWatcher", th);
        }
      }
    };
  }
}
