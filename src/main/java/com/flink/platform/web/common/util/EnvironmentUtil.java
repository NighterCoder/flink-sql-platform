package com.flink.platform.web.common.util;

import com.flink.platform.core.config.Environment;
import com.flink.platform.core.exception.SqlPlatformException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URL;

/**
 * Utility class for reading environment file.
 */
public class EnvironmentUtil {
    private static final Logger LOG = LoggerFactory.getLogger(EnvironmentUtil.class);

    public static Environment readEnvironment(URL envUrl){
        // use an empty environment by default
        if (envUrl == null) {
            System.out.println("No session environment specified.");
            return new Environment();
        }
        System.out.println("Reading configuration from: " + envUrl);
        LOG.info("Using configuration file: {}", envUrl);

        try {
            return Environment.parse(envUrl);
        } catch (IOException e) {
            throw new SqlPlatformException("Could not read configuration file at: " + envUrl, e);
        }

    }

}
