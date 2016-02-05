package org.logger.event.cassandra.loader;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EsMappingUtil {

	private static final Logger LOG = LoggerFactory.getLogger(EsMappingUtil.class);

	public static String getMappingConfig(String indexType) {
		return getConfig(indexType, "mappings");
	}

	public static String getSettingConfig(String indexType) {
		return getConfig(indexType, "settings");
	}

	public static String getConfig(String indexType, String configFile) {
		String content = null;
		String settingsPath = "/insights-data-logger/config/index/" + indexType + "/_" + configFile + ".json";

		Path currentRelativePath = Paths.get("");
		String s = currentRelativePath.toAbsolutePath().toString();
		String filePath = s + settingsPath;
		LOG.info("Current Location : " + s);
		File file = new File(filePath);
		try {
			LOG.info("File Locations : " + file.getAbsolutePath());
			content = FileUtils.readFileToString(file);
		} catch (FileNotFoundException e) {
			InputStream resourceStream = EsMappingUtil.class.getClassLoader().getResourceAsStream(settingsPath);
			content = readFileAsString(resourceStream);
		} catch (Exception exception) {
			LOG.error("Exception: Unable to get indexing configuration." + exception);
		}

		return content;
	}

	private static String readFileAsString(InputStream is) {

		BufferedReader br = null;
		StringBuilder sb = new StringBuilder();

		String line;
		try {
			br = new BufferedReader(new InputStreamReader(is));
			while ((line = br.readLine()) != null) {
				sb.append(line);
			}

		} catch (IOException e) {
			LOG.error("Exception:" + e);
		} finally {
			if (br != null) {
				try {
					br.close();
				} catch (IOException e) {
					LOG.error("Exception:" + e);
				}
			}
		}

		return sb.toString();

	}

}
