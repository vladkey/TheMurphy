package murphy.appmaster;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.client.api.async.NMClientAsync;
import org.apache.hadoop.yarn.client.api.async.impl.NMClientAsyncImpl;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.*;

import murphy.AppSubmitter.SubmitterParams;
import murphy.ContainerLauncher;


public class AppMaster {

	private static final Logger logger = LoggerFactory.getLogger(AppMaster.class);

	private static final String APPLICATION_MASTER_LOGDIR = System.getProperty("app.am.logdir");

	public static final int MEMORY_MB = 512;
	public static final int MEMORY_RESERVED_MB = 32;
	public static final int VCORES = 1;
	public static final int PRIORITY = 0;

	public static final String CONTAINER_JAR = "TheMurphy.jar";

	private static final int AM_RM_HEARTBEAT_INTERVAL_MS = 1000;
	private static final int COMPLETION_CHECK_INTERVAL_MS = 5000;

	private final ContainerLauncher launcher;
	private final AppState appState;

	private AMRMClientAsync<AMRMClient.ContainerRequest> rmClient;
	private NMClientAsync nmClient;
	private RMCallbackHandler rmCallbackHandler;

	private static int numContainers = 0;
	private static long containerMemSize = 1073741824;


	public static List<String> getAMCommand(ApplicationId appId, SubmitterParams ap) {
		String logDir;
		if (APPLICATION_MASTER_LOGDIR != null) {
			logDir = APPLICATION_MASTER_LOGDIR;
		} else {
			logDir = ApplicationConstants.LOG_DIR_EXPANSION_VAR;
		}

		// Setting Command
		List<String> command = Arrays.asList(
			Environment.JAVA_HOME.$() + "/bin/java", "-Xmx" + (MEMORY_MB - MEMORY_RESERVED_MB) + "m",
			AppMaster.class.getName(),
			appId + "",
			"\"" + ap.numContainers + "\"",
			"\"" + ap.containerMemSize + "\"",
			"1> " + logDir + "/AppMaster.stdout",
			"2> " + logDir + "/AppMaster.stderr");
		return command;
	}

	public static void main(String[] args) {
		logger.info("Application master is running with params:" + String.join(" ", args));

		try {
			String appId = args[0];
			numContainers = Integer.valueOf(args[1]);
			containerMemSize = Long.valueOf(args[2]);

			Configuration conf = new YarnConfiguration();
			AppMaster appMaster = new AppMaster(conf, appId);
			appMaster.startYarnClients();
			appMaster.registerAppMaster();
			appMaster.runTasks();
			appMaster.waitForTasks();
			appMaster.stopAppMaster();
		} catch (Throwable t) {
			logger.error("Error running ApplicationMaster", t);
			System.exit(1);
		}
	}

	public AppMaster(Configuration conf, String appId) throws IOException {
		launcher = new ContainerLauncher(conf, appId);
		launcher.collectTokensForContainers();

		appState = new AppState();
		rmCallbackHandler = new RMCallbackHandler(this, appState);
	}

	private void startYarnClients() {
		Configuration conf = launcher.getConf();
		// Resource Manager Client setup
		rmClient = AMRMClientAsync.createAMRMClientAsync(AM_RM_HEARTBEAT_INTERVAL_MS, rmCallbackHandler);
		rmClient.init(conf);
		rmClient.start();

		// Node Manager Client setup
		nmClient = new NMClientAsyncImpl(new NMCallbackHandler());
		nmClient.init(conf);
		nmClient.start();
	}

	private void registerAppMaster() throws YarnException, IOException {
		String thisHost = InetAddress.getLocalHost().getHostName();
		InetSocketAddress listen = new InetSocketAddress(thisHost, 4444);
		new WebUI(appState).start(listen);

		String appTrackingUrl = "http://" + listen.getHostString() + ":" + listen.getPort() + "/murphy/index";
		RegisterApplicationMasterResponse regInfo = rmClient.registerApplicationMaster(
				listen.getHostString(), listen.getPort(), appTrackingUrl);
		logger.info("Registered AM on {}, trackingURL: {}", thisHost, appTrackingUrl);

		logger.info("ContainersFromPreviousAttempts: {}", regInfo.getContainersFromPreviousAttempts());
	}

	private void runTasks() throws IOException, InterruptedException {
		logger.info("Requesting {} containers", numContainers);
		for (int i = 0; i < numContainers; i++) {
			EatMemoryTask task = new EatMemoryTask(containerMemSize);
			requestContainer(task);
		}
	}

	/**
	 * Send request for container to Resource Manager Add task to
	 * RMCallbackHandler's queue
	 */
	public void requestContainer(EatMemoryTask task) {
		// record the request before asking for container
		rmCallbackHandler.registerPlanItem(task);

		String[] nodes = null;
		Priority priority = task.getPriority();
		Resource capability = task.getCapability();
		AMRMClient.ContainerRequest containerRequest = new AMRMClient.ContainerRequest(capability, nodes, null, priority);
		rmClient.addContainerRequest(containerRequest);

		logger.info("Container was requested for {}", task);
	}

	public void launchEatMemoryTask(Container container, EatMemoryTask task) {
		logger.info("Container allocated for task, node: {}, id: {} ({})",
				container.getNodeId().getHost(), container.getId(), container.getNodeHttpAddress());

		List<String> command = task.getCommand();
		System.out.println(command);
		logger.info("Launch command in containerId {}", container.getId());
		ContainerLaunchContext containerContext = launcher.createContainerLaunchContext(CONTAINER_JAR, command);
		nmClient.startContainerAsync(container, containerContext);
	}

	private void waitForTasks() {
		logger.info("Waiting for containers completion...");

		while (appState.completedTasksCount < numContainers) {
			try {
				Thread.sleep(COMPLETION_CHECK_INTERVAL_MS);
			} catch (InterruptedException e) {
				logger.info("Interrupted wait for tasks, ignoring...");
			}
			logger.info("Containers completed: {} of {}", appState.completedTasksCount, numContainers);
		}
	}

	private void stopAppMaster() throws IOException, YarnException {
		logger.info("AppMaster stop initiated:");

		int failedTasksCount = appState.failedTasksCount;
		String appMessage = (failedTasksCount == 0) ? "OK" : String.format("Task failures: %d.", failedTasksCount);
		rmClient.unregisterApplicationMaster(FinalApplicationStatus.SUCCEEDED, appMessage, null);
		rmClient.stop();
		logger.info("RMClient stopped");

		nmClient.stop();
		logger.info("NMClient stopped");
	}

}
