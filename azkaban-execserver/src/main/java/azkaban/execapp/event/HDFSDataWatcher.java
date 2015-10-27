package azkaban.execapp.event;

import java.io.IOException;
import java.security.PrivilegedAction;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;

import azkaban.execapp.JobRunner;
import azkaban.jobExecutor.ProcessJob;

/**
 * A very basic implementation to serve as POC for zsl data trigger from hdfs
 * files without wild cards
 */
public class HDFSDataWatcher extends ScheduleBasedDataWatcher {
  // username to be used to fetch hdfs filesystem
  private String username;

  public HDFSDataWatcher(JobRunner runner) {
    super(runner);
    username = runner.getProps().getString(ProcessJob.USER_TO_PROXY, "azkaban");
  }

  /**
   * Fetch filesystem as "user.to.proxy" user, if available otherwise "azkaban"
   * user
   *
   * @param config
   * @return
   * @throws IOException
   */
  private FileSystem getFileSystem(final Configuration config)
    throws IOException {
    FileSystem hdfs;
    UserGroupInformation ugi = getProxiedUser(username);

    if (ugi != null) {
      hdfs = ugi.doAs(new PrivilegedAction<FileSystem>() {

        @Override
        public FileSystem run() {
          try {
            return FileSystem.get(config);
          } catch (IOException e) {
            logger.error("Failed to fetch filesystem", e);
            throw new RuntimeException(e);
          }
        }
      });
    } else {
      hdfs = FileSystem.get(config);
    }
    return hdfs;
  }

  /**
   * Get UserGroupInformation while proxying as another user Assuming that
   * Azkaban user have permission to proxy as any user
   *
   * @param userToProxy
   * @return
   * @throws IOException
   */
  private synchronized UserGroupInformation getProxiedUser(String userToProxy)
    throws IOException {
    return UserGroupInformation.createProxyUser(userToProxy,
      UserGroupInformation.getLoginUser());
  }

  @Override
  protected void processDataSets() {
    final Configuration config = new Configuration();
    FileSystem hdfs;
    try {
      // TODO: we assume that each job will get hadoop tokens for itself
      // Which means this implementation do not work for default "command"
      // jobtype. Please use "shell" for running commands with data
      // triggers
      hdfs = getFileSystem(config);
      logger.debug("Waiting for file on hdfs: " + hdfs.getUri());

      // TODO: Each job checking individually is not optimal. There should
      // be a central thread to check files in hdfs as there may be
      // multiple jobs waiting on same file and
      for (Entry<String, DataSetBlockingStatus> entry : dataSetMapper
        .entrySet()) {
        Path path = new Path(entry.getKey());
        boolean isExists = false;

        try {
          isExists = hdfs.exists(path);
        } catch (IOException e) {
          logger.error("Failed to check hdfs path " + entry.getKey(), e);
        }

        if (isExists) {
          logger.info(String.format("Dataset %s is now available on hdfs",
            entry.getKey()));
        }

        entry.getValue().changeStatus(isExists);
      }

      if (hdfs != null) {
        hdfs.close();
      }

    } catch (IOException ex) {
      logger.error("Failed to access hdfs filesystem ", ex);
    }

  }
}
