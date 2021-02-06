package com.flink.platform.core.context;

import com.flink.platform.core.config.Environment;
import com.flink.platform.core.exception.SqlPlatformException;
import org.apache.commons.cli.Options;
import org.apache.flink.client.cli.CliFrontend;
import org.apache.flink.client.cli.CliFrontendParser;
import org.apache.flink.client.cli.CustomCommandLine;
import org.apache.flink.client.deployment.ClusterClientServiceLoader;
import org.apache.flink.client.deployment.DefaultClusterClientServiceLoader;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.plugin.PluginUtils;

import java.net.URL;
import java.util.List;
import java.util.Objects;

/**
 * Context describing default environment, dependencies, flink config, etc.
 */
public class DefaultContext {

    private final Environment defaultEnv;
    private final List<URL> dependencies;
    private final Configuration flinkConfig;
    private final List<CustomCommandLine> commandLines;
    private final Options commandLineOptions;
    private final ClusterClientServiceLoader clusterClientServiceLoader;

    public DefaultContext(Environment defaultEnv, List<URL> dependencies) {
        this.defaultEnv = defaultEnv;
        this.dependencies = dependencies;

        // 加载Flink Conf
        final String flinkConfigDir;
        try {
            // 查找Flink Configuration Directory
            // 设置环境变量FLINK_CONF_DIR
            flinkConfigDir = CliFrontend.getConfigurationDirectoryFromEnv();
            // 加载全局配置
            this.flinkConfig = GlobalConfiguration.loadConfiguration(flinkConfigDir);

            // initialize default file system
            FileSystem.initialize(flinkConfig, PluginUtils.createPluginManagerFromRootFolder(flinkConfig));

            // load command lines for deployment
            this.commandLines = CliFrontend.loadCustomCommandLines(flinkConfig, flinkConfigDir);
            this.commandLineOptions = collectCommandLineOptions(commandLines);

        } catch (Exception e) {
            throw new SqlPlatformException("Could not load Flink configuration.", e);
        }

        this.clusterClientServiceLoader = new DefaultClusterClientServiceLoader();
    }

    public Configuration getFlinkConfig() {
        return flinkConfig;
    }

    public Environment getDefaultEnv() {
        return defaultEnv;
    }

    public List<URL> getDependencies() {
        return dependencies;
    }

    public List<CustomCommandLine> getCommandLines() {
        return commandLines;
    }

    public Options getCommandLineOptions() {
        return commandLineOptions;
    }

    public ClusterClientServiceLoader getClusterClientServiceLoader() {
        return clusterClientServiceLoader;
    }

    private Options collectCommandLineOptions(List<CustomCommandLine> commandLines) {
        final Options customOptions = new Options();
        for (CustomCommandLine customCommandLine : commandLines) {
            customCommandLine.addGeneralOptions(customOptions);
            customCommandLine.addRunOptions(customOptions);
        }
        return CliFrontendParser.mergeOptions(
                CliFrontendParser.getRunCommandOptions(),
                customOptions);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof DefaultContext)) {
            return false;
        }
        DefaultContext context = (DefaultContext) o;
        return Objects.equals(defaultEnv, context.defaultEnv) &&
                Objects.equals(dependencies, context.dependencies) &&
                Objects.equals(flinkConfig, context.flinkConfig) &&
                Objects.equals(commandLines, context.commandLines) &&
                Objects.equals(commandLineOptions, context.commandLineOptions) &&
                Objects.equals(clusterClientServiceLoader, context.clusterClientServiceLoader);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                defaultEnv, dependencies, flinkConfig, commandLines, commandLineOptions, clusterClientServiceLoader);
    }


}
