package murphy;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;

import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.Records;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;

import java.util.*;

import murphy.appmaster.AppMaster;

/**
 * Runs application via YARN Usage: hadoop jar ./murphy-jar-with-dependencies.jar murphy.AppSubmitter
 * ./murphy-jar-with-dependencies.jar
 */
public class AppSubmitter {

	private static final Logger logger = LoggerFactory.getLogger(AppSubmitter.class);


	private static Configuration conf;
	private static String appJar;


	public static class SubmitterParams {
		public String numContainers;
		public String containerMemSize;

		public SubmitterParams(String[] args) throws Exception {
			numContainers = args[1];
			containerMemSize = args[2];
		}
	}


	public static void main(String[] args) throws Exception {
		logger.info("AppSubmitter args: " + String.join(" ", args));
		appJar = args[0];
		conf = new YarnConfiguration();

		YarnClient yarnClient = YarnClient.createYarnClient();
		yarnClient.init(conf);
		yarnClient.start();

		printNodes(yarnClient);

		String queueName = System.getProperty("yarn.queue", "bd_power");

		SubmitterParams ap = new SubmitterParams(args);
		String appName = ContainerLauncher.APPLICATION_NAME + ":" + ap.numContainers + "x" + ap.containerMemSize;
		logger.info("Starting {} of {} cont * {} MB", appName, ap.numContainers, ap.containerMemSize);
		ApplicationId appId = submitAppMaster(yarnClient, appName, ap, queueName);

		AppMonitor.monitorApplication(yarnClient, appId);
	}

	public static ApplicationId submitAppMaster(YarnClient yarnClient, String appName, SubmitterParams ap, String queueName)
			throws IOException, YarnException
	{
		// Create Yarn application
		YarnClientApplication app = yarnClient.createApplication();
		GetNewApplicationResponse appResponse = app.getNewApplicationResponse();
		printCaps(appResponse);

		ApplicationSubmissionContext appContext = app.getApplicationSubmissionContext();
		ApplicationId appId = appContext.getApplicationId();

		// Set the application name
		appContext.setApplicationName(appName);

		// Set up resource type requirements
		Resource capability = Resource.newInstance(AppMaster.MEMORY_MB, AppMaster.VCORES);
		appContext.setResource(capability);
		Priority priority = Priority.newInstance(AppMaster.PRIORITY);
		appContext.setPriority(priority);

		// Set up the container launch context for the application master
		ContainerLaunchContext amContainer =  configureAMContainer(appId, ap);
		appContext.setAMContainerSpec(amContainer);
		appContext.setQueue(queueName);

		logger.info("Submitting application {} to RM, queue: {}", appContext.getApplicationName(), queueName);
		yarnClient.submitApplication(appContext);
		return appId;
	}

	public static void printCaps(GetNewApplicationResponse appResponse) {
		int maxMem = appResponse.getMaximumResourceCapability().getMemory();
		logger.debug("Max mem capabililty of resources in this cluster: {}", maxMem);

		int maxVCores = appResponse.getMaximumResourceCapability().getVirtualCores();
		logger.debug("Max virtual cores capabililty of resources in this cluster: {} ", maxVCores);
	}

	public static void printNodes(YarnClient yarnClient) throws YarnException, IOException {
		YarnClusterMetrics clusterMetrics = yarnClient.getYarnClusterMetrics();
		logger.debug("Got Cluster metric info from ASM, numNodeManagers={}", clusterMetrics.getNumNodeManagers());

		List<NodeReport> clusterNodeReports = yarnClient.getNodeReports(NodeState.RUNNING);
		logger.debug("Got Cluster node info from ASM");
		for (NodeReport node : clusterNodeReports) {
			logger.debug("NodeId: {}, nodeAddress: {}, nodeRackName: {}, nodeNumContainers: {}",
					node.getNodeId(), node.getHttpAddress(), node.getRackName(), node.getNumContainers());
		}
	}

	public static ContainerLaunchContext configureAMContainer(ApplicationId appId, SubmitterParams ap)
			throws IOException
	{
		ContainerLaunchContext amContainer = Records.newRecord(ContainerLaunchContext.class);

		ContainerLauncher launcher = new ContainerLauncher(conf, appId.toString());

		List<String> command = AppMaster.getAMCommand(appId, ap);
		System.out.println(command);
		amContainer.setCommands(Arrays.asList(String.join(" ", command)));

		// Copy the application master jar to the filesystem
		launcher.addToLocalResources(appJar, AppMaster.CONTAINER_JAR);
		amContainer.setLocalResources(launcher.getLocalResources());

		// Set the env variables to be setup in the env where the application master will be run
		logger.info("Set the environment for the AM container");
		amContainer.setEnvironment(ContainerLauncher.createContainerEnv(conf));

		// Setup security tokens
		if (UserGroupInformation.isSecurityEnabled()) {
			ByteBuffer fsTokens = launcher.collectSecurityTokens();
			amContainer.setTokens(fsTokens);
			logger.info("Set tokens for the AM container");
		} else {
			logger.info("Security is NOT enabled");
		}
		return amContainer;
	}

}
