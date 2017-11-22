package murphy.appmaster;


import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;

import java.util.*;


public class EatMemoryTask {

	private static final int CONTAINER_VCORES = 1;
	private static final int CONTAINER_PRIORITY = 0;

	private static final String EXECUTOR_CLASS = murphy.EatMemoryWorker.class.getName();

	private final long memSizeParam;
	private final int memSizeMB;

	public EatMemoryTask(long memSizeParam) {
		this.memSizeParam = memSizeParam;
		this.memSizeMB = (int) (memSizeParam * 12 / 1048576 / 10);
	}

	public List<String> getCommand() {
		String logDir = ApplicationConstants.LOG_DIR_EXPANSION_VAR;
		List<String> command = Arrays.asList(
			Environment.JAVA_HOME.$() + "/bin/java",
			"-Xmx" + memSizeMB + "m",
			EXECUTOR_CLASS,
			Long.toString(memSizeParam),
			"this_is_worker",
			"1> " + logDir + "/stdout",
			"2> " + logDir + "/stderr"
		);
		return command;
	}

	public Resource getCapability() {
		Resource capability = Resource.newInstance(memSizeMB, CONTAINER_VCORES);
		return capability;
	}

	public Priority getPriority() {
		Priority priority = Priority.newInstance(CONTAINER_PRIORITY);
		return priority;
	}
}
