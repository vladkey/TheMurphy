package murphy.appmaster;

import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.client.api.async.NMClientAsync;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.Map;

/**
 * Process communication with Node Managers
 */
public class NMCallbackHandler implements NMClientAsync.CallbackHandler {

	private static final Logger logger = LoggerFactory.getLogger(NMCallbackHandler.class);

	@Override
	public void onContainerStopped(ContainerId containerId) {
		logger.debug("Succeeded to stop Container {}", containerId);
	}

	@Override
	public void onContainerStarted(ContainerId containerId, Map<String, ByteBuffer> allServiceResponse) {
		logger.info("Succeeded to start Container {}", containerId);
		logger.info("ServicesData: {}", allServiceResponse.keySet());
	}

	@Override
	public void onStartContainerError(ContainerId containerId, Throwable t) {
		logger.error("Failed to start Container {} ", containerId, t);
	}

	@Override
	public void onGetContainerStatusError(ContainerId containerId, Throwable t) {
		logger.error("Failed to query the status of Container {} ", containerId, t);
	}

	@Override
	public void onStopContainerError(ContainerId containerId, Throwable t) {
		logger.error("Failed to stop Container {}", containerId, t);
	}

	@Override
	public void onContainerStatusReceived(ContainerId containerId, ContainerStatus s) {
		logger.info("Container {} status {} (exit: {} diag: {})", containerId, s.getState(),
				s.getExitStatus(), s.getDiagnostics());
	}
}
