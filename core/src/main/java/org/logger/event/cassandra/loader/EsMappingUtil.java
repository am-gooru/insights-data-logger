package org.logger.event.cassandra.loader;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import org.apache.commons.io.FileUtils;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;

public class EsMappingUtil {

	public static String getMappingConfig(String indexType) {
		return getConfig(indexType, "mappings");
	}

	public static String getSettingConfig(String indexType) {
		return getConfig(indexType, "settings");
	}

	public static String getConfig(String indexType,
			String configFile) {
		String content = null;
		String settingsPath = "config/index/" + indexType + "/_" + configFile + ".json";
		try {
			Resource resource = new ClassPathResource(settingsPath);
			System.out.print("File Location : " + resource.getURI());
			System.out.print("File Locations : " + resource.getFile());
			content = FileUtils.readFileToString(resource.getFile());
			
		} catch (FileNotFoundException e) {
			InputStream resourceStream = EsMappingUtil.class.getClassLoader().getResourceAsStream(settingsPath);
			content = readFileAsString(resourceStream);
		} catch (Exception exception) {
			exception.printStackTrace();
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
			e.printStackTrace();
		} finally {
			if (br != null) {
				try {
					br.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}

		return sb.toString();

	}

}
