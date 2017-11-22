package murphy;

import org.apache.hadoop.conf.Configuration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.*;

public class EatMemoryWorker {
	private static final Logger logger = LoggerFactory.getLogger(EatMemoryWorker.class);

	private Configuration conf;


	public static void main(String[] args) throws Exception {
		logger.info("EatMemoryWorker started with parameters: " + String.join(" ", args));
		long memorySize = Long.valueOf(args[0]);

		boolean valid = run(memorySize);

		logger.info("EatMemoryWorker finished");

		if (!valid) {
			System.exit(1);
		}
	}

	public static boolean run(long memorySize) throws Exception {
		printMemory("Start");
		int chunkSize = (100 * 1024 * 1024);
		int chunks = (int) ((memorySize + chunkSize - 1) / chunkSize);
		List<byte[]> occupy = new ArrayList<>();
		for (int i = 0; i < chunks; i++) {
			occupy.add(new byte[chunkSize]);
		}

		logger.info("Allocated " + chunks + " x " + chunkSize + " bytes; sleeping for 120 s");

		for (int i = 0; i < chunks; i++) {
			byte[] bytes = occupy.get(i);
			for (int j = 0; j < bytes.length; j++) {
				bytes[j] = (byte) ((i + j) % 255 - 128);
			}
		}

		Thread.sleep(120 * 1000);
		printMemory("END");

		return occupy.size() == chunks && occupy.get(0).length == chunkSize; // Attempt to defeat HotSpot optimizer
	}

	private static void printMemory(String marker) {
		Runtime r = Runtime.getRuntime();
		logger.info("Memory at {}: {} Mb total, {} Mb max, {} Mb free", marker,
				mb(r.totalMemory()), mb(r.maxMemory()), mb(r.freeMemory()));
	}

	private static long mb(long bytes) {
		return bytes / (1024 * 1024);
	}

}
