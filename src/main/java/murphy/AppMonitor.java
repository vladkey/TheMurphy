package murphy;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationResourceUsageReport;
import org.apache.hadoop.yarn.api.records.ContainerReport;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class AppMonitor {

	private static final Logger logger = LoggerFactory.getLogger(AppMonitor.class);

	private static final int MONITOR_HEARTBEAT_INTERVAL_MS = 5000;

	/**
	 * Checks application status every N seconds till app master is finished
	 * 
	 * @param appId application id
	 * @throws YarnException
	 * @throws IOException
	 */
	public static boolean monitorApplication(YarnClient yarnClient, ApplicationId appId)
		throws YarnException, IOException
	{
		boolean printedApp = false;
		ApplicationAttemptId prevAttemptId = null;
		int containersPoll = 0;
		while (true) {
			// Get application report for the appId we are interested in
			ApplicationReport report = yarnClient.getApplicationReport(appId);

			// Check application state
			YarnApplicationState state = report.getYarnApplicationState();
			FinalApplicationStatus dsStatus = report.getFinalApplicationStatus();
			if (YarnApplicationState.FINISHED == state) {
				if (FinalApplicationStatus.SUCCEEDED == dsStatus) {
					logger.info("Application {} has FINISHED successfully. Breaking monitoring loop", appId);
					logger.info("Execution time: started {}, finished {}", report.getStartTime(), report.getFinishTime());
				} else {
					logger.error("Application {} has finished but final status is {}", appId, dsStatus);
					logger.error("Diagnostics: {}", report.getDiagnostics());
				}
				return true;
			} else if (YarnApplicationState.KILLED == state || YarnApplicationState.FAILED == state) {
				logger.info("Application {} did not finish. DSFinalStatus={}", appId, dsStatus);
				logger.error("Diagnostics: {}", report.getDiagnostics());
				return false;
			}

			try {
				Thread.sleep(MONITOR_HEARTBEAT_INTERVAL_MS);
			} catch (InterruptedException e) {
			}

			if (!printedApp) {
				logger.info("App id: {} {} ({}) runs on {}. Progress: {} Tracking URL: ",
					appId, state, report.getName(), report.getHost(), report.getProgress(), report.getTrackingUrl());
				printedApp = true;
			} else {
				logger.info("State: {} on {}. Progress: {}", state, report.getHost(), report.getProgress());
			}

			ApplicationResourceUsageReport usage = report.getApplicationResourceUsageReport();
			logger.debug("Resources usage: {} containers, {} vcores*s, {} memory*s",
					usage.getNumUsedContainers(), usage.getVcoreSeconds(), usage.getMemorySeconds());

			ApplicationAttemptId attemptId = report.getCurrentApplicationAttemptId();
			boolean needPrint = ! attemptId.equals(prevAttemptId);
			if (needPrint || containersPoll >= 7) { // every 30s
				printContainers(yarnClient, attemptId, prevAttemptId);
				prevAttemptId = attemptId;
				containersPoll = 0;
			}
			containersPoll++;
		}
	}

	private static void printContainers(YarnClient yarnClient, ApplicationAttemptId attemptId, ApplicationAttemptId prevAttemptId)
			throws YarnException, IOException
	{
		List<ContainerReport> containers = yarnClient.getContainers(attemptId);
		logger.info("Containers for attempt {} (prev: {}):", attemptId, prevAttemptId);
		for (ContainerReport r : containers) {
			logger.info("C: {} at {} state={}, log={}",
					r.getContainerId(), r.getAssignedNode(), r.getContainerState(), r.getLogUrl());
		}
	}
}
