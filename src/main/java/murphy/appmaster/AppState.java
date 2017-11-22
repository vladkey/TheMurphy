package murphy.appmaster;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AppState {

	private static final Logger logger = LoggerFactory.getLogger(AppState.class);

	public volatile int runningContainers = 0;
	public volatile int completedTasksCount = 0;
	public volatile int failedTasksCount = 0;

	/** Map[containerIds: tasks] */
	private final Map<Long, EatMemoryTask> container2task = new HashMap<>();

	// TODO: wire NMCallbackHandler for containers tracking

	public void recordContainerState(long containerId, EatMemoryTask task, String state) {
		// put task and container id into running tasks map
		container2task.put(containerId, task);
	}

	public void onContainerFinished(ContainerStatus s) {
		logger.info("Container {} completed with exit status  {} ", s.getContainerId(), s.getExitStatus());
		EatMemoryTask task = container2task.remove(s.getContainerId());
		completedTasksCount++;
		if (s.getExitStatus() != 0) {
			failedTasksCount++;
			logger.error("Container {} exited {}, diag: {}, state: {}", s.getContainerId(), s.getExitStatus(),
					s.getDiagnostics(), s.getState());
		}
	}

	public float getProgress() {
		return 0.50f;
	}

}
