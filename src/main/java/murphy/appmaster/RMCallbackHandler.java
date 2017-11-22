package murphy.appmaster;

import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.client.api.async.NMClientAsync;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.atomic.*;

/**
 * Processes communication of AM with Resource Manager
 */
public class RMCallbackHandler implements AMRMClientAsync.CallbackHandler {

	private static final Logger logger = LoggerFactory
			.getLogger(RMCallbackHandler.class);

	private final AppMaster appMaster;
	private final AppState appState;

	private final List<EatMemoryTask> requests = new ArrayList<>();

	public RMCallbackHandler(AppMaster appMaster, AppState appState) {
		this.appMaster = appMaster;
		this.appState = appState;
	}

	/**
	 * Add task with resources of requested container to queue
	 * 
	 * @param task
	 */
	public synchronized void registerPlanItem(EatMemoryTask task) {
		requests.add(task);
	}

	@Override
	public synchronized void onContainersAllocated(List<Container> allocatedContainers) {
		logger.info("Got response from RM for container ask, allocatedCount={}", allocatedContainers.size());
		for (Container container : allocatedContainers) {
			EatMemoryTask task = requests.remove(0);
			appState.recordContainerState(container.getId().getContainerId(), task, "ALLOCATED");
			appMaster.launchEatMemoryTask(container, task);
		}
	}

	@Override
	public synchronized void onContainersCompleted(List<ContainerStatus> completedContainers) {
		for (ContainerStatus s : completedContainers) {
			appState.onContainerFinished(s);
		}
	}

	// Called when error comes from RM communications as well as from errors in the callback itself from the app.
	@Override
	public void onError(Throwable e) {
		logger.error("Error was caught in RMCallbackHandler", e);
		// TODO: maybe wait some time for temporary network condition but eventually kill containers and exit with -13
	}

	@Override
	public void onShutdownRequest() {
		logger.error("Shutdown was requested in RMCallbackHandler");
		// TODO: implement graceful stop, maybe cleanup
	}

	@Override
	public void onNodesUpdated(List<NodeReport> list) {
		// TODO: need to track
		// Called when nodes tracked by the ResourceManager have changed in health, availability etc.
		logger.info("Nodes were updated: {}", list);
	}

	@Override
	public float getProgress() {
		return appState.getProgress();
	}

}
