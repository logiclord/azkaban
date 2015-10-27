package azkaban.execapp.event;

import java.io.File;
import java.util.Map.Entry;

import org.apache.commons.io.FileExistsException;

import azkaban.execapp.JobRunner;

/**
 * A very basic implementation to serve as POC for zsl data trigger from local
 * files without wild cards
 */
public class LocalDataWatcher extends ScheduleBasedDataWatcher {

  public LocalDataWatcher(JobRunner runner) {
    super(runner);
  }

  @Override
  protected void processDataSets() {
    // TODO: Each job checking individually is not optimal. There should be
    // a central thread to check files in local/hdfs as there may be
    // multiple jobs waiting on same file and
    for (Entry<String, DataSetBlockingStatus> entry : dataSetMapper.entrySet()) {
      File file = new File(entry.getKey());
      boolean isExists = file.exists();
      if (isExists) {
        logger.info(String.format(
          "Dataset %s is now available on local file system", entry.getKey()));
      }
      entry.getValue().changeStatus(isExists);
    }
  }
}
