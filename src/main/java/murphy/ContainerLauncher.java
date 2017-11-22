package murphy;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.URL;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;
import org.apache.hadoop.yarn.security.AMRMTokenIdentifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

/**
 * Set of common methods for ContainerLaunchers
 */
public class ContainerLauncher {

	public static final String APPLICATION_NAME = "Memory.Stress";

	public static final Logger logger = LoggerFactory.getLogger(ContainerLauncher.class);

	protected final FileSystem fs;
	protected final Configuration conf;
	protected final String appId;
	protected final Map<String, LocalResource> localResources = new HashMap<String, LocalResource>();

	protected ByteBuffer allTokens;
	protected UserGroupInformation appSubmitterUgi;


	public ContainerLauncher(Configuration conf, String appId) throws IOException {
		this.conf = conf;
		fs = FileSystem.get(conf);
		this.appId = appId;
	}

	public FileSystem getFileSystem() {
		return fs;
	}

	public Configuration getConf() {
		return conf;
	}

	public Map<String, LocalResource> getLocalResources() {
		return localResources;
	}

	/**
	 * addToLocalResources() writes resource to the Hadoop File System and adds
	 * it to localResources dictionary, that will be sent with Container Request
	 * 
	 * @param fs Hadoop FileSystem instance
	 * @param fileSrcPath path to resource file
	 * @param fileDstPath output file name on HDFS
	 * @param appId application id
	 * @param localResources localResources dictionary
	 * @throws IOException
	 */
	public void addToLocalResources(String fileSrcPath, String fileDstPath)
			throws IOException
	{
		if (localResources.containsKey(fileDstPath)) {
			// file is already copied, no need to overwrite
			return;
		}

		String suffix = APPLICATION_NAME + File.separator + fileDstPath;
		Path jarDir = fs.getHomeDirectory();
		Path dst = new Path("hdfs://analytics2dev/tmp", suffix);

		fs.copyFromLocalFile(new Path(fileSrcPath), dst);
		logger.debug("Copied {} into {}", fileSrcPath, dst);

		FileStatus fileStatus = fs.getFileStatus(dst);
		URL yarnUrl = ConverterUtils.getYarnUrlFromURI(dst.toUri());
		logger.info("Remapped {} into {}", dst, yarnUrl);
		LocalResource resource = LocalResource.newInstance(yarnUrl,
				LocalResourceType.FILE, LocalResourceVisibility.PUBLIC, // APPLICATION,
				fileStatus.getLen(), fileStatus.getModificationTime());
		localResources.put(fileDstPath, resource);
	}

	public ContainerLaunchContext createContainerLaunchContext(String containerJar, List<String> command) {
		ContainerLaunchContext containerContext = Records.newRecord(ContainerLaunchContext.class);
		// ContainerLaunchContext.newInstance(localResources, shellEnv, commands, null, allTokens.duplicate(), null);

		containerContext.setCommands(Arrays.asList(String.join(" ", command)));

		// Copy worker jar to the filesystem
		// Create a local resource to point to the destination jar path
		try {
			addToLocalResources(containerJar, containerJar);
			containerContext.setLocalResources(localResources);
		} catch (IOException e) {
			logger.error("FileSystem {} is not accessible.", fs, e);
			// throw new RuntimeException(e);
		}

		// Setup environment
		containerContext.setEnvironment(createContainerEnv(conf));
		containerContext.setTokens(allTokens.duplicate());
		return containerContext;
	}

	public ByteBuffer collectSecurityTokens() throws IOException {
		// Note: Credentials class is marked as LimitedPrivate for HDFS and MapReduce
		Credentials credentials = new Credentials();
		String tokenRenewer = conf.get(YarnConfiguration.RM_PRINCIPAL);
		if (tokenRenewer == null || tokenRenewer.length() == 0) {
			throw new IOException("Can't get Master Kerberos principal for the RM to use as renewer");
		}

		// For now, only getting tokens for the default file-system
		Token<?> tokens[] = fs.addDelegationTokens(tokenRenewer, credentials);
		if (tokens != null) {
			for (Token<?> token : tokens) {
				logger.info("Got DelegationToken for " + fs.getUri() + ": " + token);
			}
		}
		DataOutputBuffer dob = new DataOutputBuffer();
		credentials.writeTokenStorageToStream(dob);
		ByteBuffer fsTokens = ByteBuffer.wrap(dob.getData(), 0, dob.getLength());
		return fsTokens;
	}

	public void collectTokensForContainers() throws IOException {
		Credentials credentials = UserGroupInformation.getCurrentUser().getCredentials();

		// Now remove the AM->RM token so that containers cannot access it.
		Iterator<Token<?>> iter = credentials.getAllTokens().iterator();
		logger.info("Collecting tokens to pass to offsprings");
		while (iter.hasNext()) {
			Token<?> token = iter.next();
			if (token.getKind().equals(AMRMTokenIdentifier.KIND_NAME)) {
				iter.remove();
			}
		}

		DataOutputBuffer dob = new DataOutputBuffer();
		credentials.writeTokenStorageToStream(dob);
		allTokens = ByteBuffer.wrap(dob.getData(), 0, dob.getLength());

		// Create appSubmitterUgi and add original tokens to it
		String appSubmitterUserName = System.getenv(ApplicationConstants.Environment.USER.name());
		appSubmitterUgi = UserGroupInformation.createRemoteUser(appSubmitterUserName);
		appSubmitterUgi.addCredentials(credentials);
	}

	/**
	 * Adds environment variables (currently just CLASSPATH) for a container to context.
	 *
	 * @param conf Yarn Config
	 */
	public static Map<String, String> createContainerEnv(Configuration conf) {
		StringBuilder classPathEnv = new StringBuilder(ApplicationConstants.Environment.CLASSPATH.$$())
			.append(ApplicationConstants.CLASS_PATH_SEPARATOR)
			.append("./*");
		for (String c : conf.getStrings(
					YarnConfiguration.YARN_APPLICATION_CLASSPATH,
					YarnConfiguration.DEFAULT_YARN_CROSS_PLATFORM_APPLICATION_CLASSPATH)) {
			classPathEnv.append(ApplicationConstants.CLASS_PATH_SEPARATOR);
			classPathEnv.append(c.trim());
		}
		Map<String, String> env = new HashMap<>();
		env.put("CLASSPATH", classPathEnv.toString());
		String hadoopHomeDir = System.getenv("HADOOP_HOME");
		if (hadoopHomeDir != null) {
			env.put("HADOOP_HOME", hadoopHomeDir);
			logger.info("Passing HADOOP_HOME: " + hadoopHomeDir);
		} else {
			logger.warn("NOT passing HADOOP_HOME, null value");
		}
		String pathEnvVar = System.getenv("PATH");
		env.put("PATH", pathEnvVar);
		logger.info("Passing PATH: " + pathEnvVar);
		return env;
	}

}
