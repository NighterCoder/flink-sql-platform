package com.flink.platform.core.config;

import com.flink.platform.core.config.entries.*;
import com.flink.platform.core.exception.SqlPlatformException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonMappingException;
import org.apache.flink.table.descriptors.DescriptorProperties;

import java.io.IOException;
import java.net.URL;
import java.util.*;

/**
 * Environment configuration that represents the content of an environment file. Environment files
 * define server, session, catalogs, tables, execution, and deployment behavior.
 * An environment might be defined by default or as part of a session. Environments can be merged or enriched with properties.
 */
public class Environment {
    public static final String SERVER_ENTRY = "server";

    public static final String SESSION_ENTRY = "session";

    public static final String EXECUTION_ENTRY = "execution";

    public static final String CONFIGURATION_ENTRY = "table";

    public static final String DEPLOYMENT_ENTRY = "deployment";

    public static final String DEER_ENTRY = "deer";

    private ServerEntry server;

    private SessionEntry session;

    private DeerEntry deer;

    private Map<String, ModuleEntry> modules;

    private Map<String, CatalogEntry> catalogs;

    private Map<String, TableEntry> tables;

    private Map<String, FunctionEntry> functions;

    private ExecutionEntry execution;

    private ConfigurationEntry configuration;

    private DeploymentEntry deployment;

    public Environment() {
        this.server = ServerEntry.DEFAULT_INSTANCE;
        this.session = SessionEntry.DEFAULT_INSTANCE;
        this.modules = new LinkedHashMap<>();
        this.catalogs = Collections.emptyMap();
        this.tables = Collections.emptyMap();
        this.functions = Collections.emptyMap();
        this.execution = ExecutionEntry.DEFAULT_INSTANCE;
        this.configuration = ConfigurationEntry.DEFAULT_INSTANCE;
        this.deployment = DeploymentEntry.DEFAULT_INSTANCE;
        // 添加 DeerEntry 信息
        this.deer = DeerEntry.DEFAULT_INSTANCE;
    }

    public DeerEntry getDeer() {
        return deer;
    }

    public void setDeer(Map<String, Object> config) {
        this.deer = DeerEntry.create(config);
    }

    public void setSession(Map<String, Object> config) {
        this.session = SessionEntry.create(config);
    }

    public SessionEntry getSession() {
        return session;
    }

    public void setServer(Map<String, Object> config) {
        this.server = ServerEntry.create(config);
    }

    public ServerEntry getServer() {
        return server;
    }

    public Map<String, ModuleEntry> getModules() {
        return modules;
    }

    public void setModules(List<Map<String, Object>> modules) {
        this.modules = new LinkedHashMap<>(modules.size());

        modules.forEach(config -> {
            final ModuleEntry entry = ModuleEntry.create(config);
            if (this.modules.containsKey(entry.getName())) {
                throw new SqlPlatformException(
                        String.format("Cannot register module '%s' because a module with this name is already registered.",
                                entry.getName()));
            }
            this.modules.put(entry.getName(), entry);
        });
    }

    public Map<String, CatalogEntry> getCatalogs() {
        return catalogs;
    }

    public void setCatalogs(List<Map<String, Object>> catalogs) {
        this.catalogs = new HashMap<>(catalogs.size());

        catalogs.forEach(config -> {
            final CatalogEntry catalog = CatalogEntry.create(config);
            if (this.catalogs.containsKey(catalog.getName())) {
                throw new SqlPlatformException(
                        String.format("Cannot create catalog '%s' because a catalog with this name is already registered.",
                                catalog.getName()));
            }
            this.catalogs.put(catalog.getName(), catalog);
        });
    }

    public Map<String, TableEntry> getTables() {
        return tables;
    }

    public void setTables(List<Map<String, Object>> tables) {
        this.tables = new LinkedHashMap<>(tables.size());

        tables.forEach(config -> {
            final TableEntry table = TableEntry.create(config);
            if (this.tables.containsKey(table.getName())) {
                throw new SqlPlatformException(
                        "Cannot create table '" + table
                                .getName() + "' because a table with this name is already registered.");
            }
            this.tables.put(table.getName(), table);
        });
    }

    public Map<String, FunctionEntry> getFunctions() {
        return functions;
    }

    public void setFunctions(List<Map<String, Object>> functions) {
        this.functions = new HashMap<>(functions.size());

        functions.forEach(config -> {
            final FunctionEntry function = FunctionEntry.create(config);
            if (this.functions.containsKey(function.getName())) {
                throw new SqlPlatformException(
                        "Cannot create function '" + function
                                .getName() + "' because a function with this name is already registered.");
            }
            this.functions.put(function.getName(), function);
        });
    }

    public void setExecution(Map<String, Object> config) {
        this.execution = ExecutionEntry.create(config);
    }

    public ExecutionEntry getExecution() {
        return execution;
    }

    public void setConfiguration(Map<String, Object> config) {
        this.configuration = ConfigurationEntry.create(config);
    }

    public ConfigurationEntry getConfiguration() {
        return configuration;
    }

    public void setDeployment(Map<String, Object> config) {
        this.deployment = DeploymentEntry.create(config);
    }

    public DeploymentEntry getDeployment() {
        return deployment;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append("==================== Server =====================\n");
        server.asTopLevelMap().forEach((k, v) -> sb.append(k).append(": ").append(v).append('\n'));
        sb.append("==================== Session =====================\n");
        session.asTopLevelMap().forEach((k, v) -> sb.append(k).append(": ").append(v).append('\n'));
        sb.append("===================== Modules =====================\n");
        modules.forEach((name, module) -> {
            sb.append("- ").append(ModuleEntry.MODULE_NAME).append(": ").append(name).append("\n");
            module.asMap().forEach((k, v) -> sb.append("  ").append(k).append(": ").append(v).append('\n'));
        });
        sb.append("===================== Catalogs =====================\n");
        catalogs.forEach((name, catalog) -> {
            sb.append("- ").append(CatalogEntry.CATALOG_NAME).append(": ").append(name).append("\n");
            catalog.asMap().forEach((k, v) -> sb.append("  ").append(k).append(": ").append(v).append('\n'));
        });
        sb.append("===================== Tables =====================\n");
        tables.forEach((name, table) -> {
            sb.append("- ").append(TableEntry.TABLES_NAME).append(": ").append(name).append("\n");
            table.asMap().forEach((k, v) -> sb.append("  ").append(k).append(": ").append(v).append('\n'));
        });
        sb.append("=================== Functions ====================\n");
        functions.forEach((name, function) -> {
            sb.append("- ").append(FunctionEntry.FUNCTIONS_NAME).append(": ").append(name).append("\n");
            function.asMap().forEach((k, v) -> sb.append("  ").append(k).append(": ").append(v).append('\n'));
        });
        sb.append("=================== Execution ====================\n");
        execution.asTopLevelMap().forEach((k, v) -> sb.append(k).append(": ").append(v).append('\n'));
        sb.append("================== Configuration =================\n");
        configuration.asMap().forEach((k, v) -> sb.append(k).append(": ").append(v).append('\n'));
        sb.append("=================== Deployment ===================\n");
        deployment.asTopLevelMap().forEach((k, v) -> sb.append(k).append(": ").append(v).append('\n'));
        sb.append("=================== Deer ===================\n");
        deer.asTopLevelMap().forEach((k, v) -> sb.append(k).append(": ").append(v).append('\n'));
        return sb.toString();
    }

    // --------------------------------------------------------------------------------------------

    /**
     * Parses an environment file from an URL.
     */
    public static Environment parse(URL url) throws IOException {
        try {
            return new ConfigUtil.LowerCaseYamlMapper().readValue(url, Environment.class);
        } catch (JsonMappingException e) {
            throw new SqlPlatformException("Could not parse environment file. Cause: " + e.getMessage());
        }
    }

    /**
     * Parses an environment file from an String.
     */
    public static Environment parse(String content) throws IOException {
        try {
            return new ConfigUtil.LowerCaseYamlMapper().readValue(content, Environment.class);
        } catch (JsonMappingException e) {
            throw new SqlPlatformException("Could not parse environment file. Cause: " + e.getMessage());
        }
    }

    /**
     * Merges two environments. The properties of the first environment might be overwritten by the second one.
     */
    public static Environment merge(Environment env1, Environment env2) {
        final Environment mergedEnv = new Environment();

        // merge server properties
        mergedEnv.server = ServerEntry.merge(env1.getServer(), env2.getServer());

        // merge session properties
        mergedEnv.session = SessionEntry.merge(env1.getSession(), env2.getSession());

        // merge modules
        final Map<String, ModuleEntry> modules = new LinkedHashMap<>(env1.getModules());
        modules.putAll(env2.getModules());
        mergedEnv.modules = modules;

        // merge catalogs
        final Map<String, CatalogEntry> catalogs = new HashMap<>(env1.getCatalogs());
        catalogs.putAll(env2.getCatalogs());
        mergedEnv.catalogs = catalogs;

        // merge tables
        final Map<String, TableEntry> tables = new LinkedHashMap<>(env1.getTables());
        tables.putAll(env2.getTables());
        mergedEnv.tables = tables;

        // merge functions
        final Map<String, FunctionEntry> functions = new HashMap<>(env1.getFunctions());
        functions.putAll(env2.getFunctions());
        mergedEnv.functions = functions;

        // merge execution properties
        mergedEnv.execution = ExecutionEntry.merge(env1.getExecution(), env2.getExecution());

        // merge configuration properties
        mergedEnv.configuration = ConfigurationEntry.merge(env1.getConfiguration(), env2.getConfiguration());

        // merge deployment properties
        mergedEnv.deployment = DeploymentEntry.merge(env1.getDeployment(), env2.getDeployment());

        // merge deer properties
        mergedEnv.deer = DeerEntry.merge(env1.getDeer(),env2.getDeer());


        return mergedEnv;
    }

    public Environment clone() {
        return enrich(this, Collections.emptyMap(), Collections.emptyMap());
    }

    /**
     * Enriches an environment with new/modified properties or views and returns the new instance.
     */
    public static Environment enrich(
            Environment env,
            Map<String, String> properties,
            Map<String, ViewEntry> views) {
        final Environment enrichedEnv = new Environment();

        enrichedEnv.modules = new LinkedHashMap<>(env.getModules());

        // merge catalogs
        enrichedEnv.catalogs = new LinkedHashMap<>(env.getCatalogs());

        // merge tables
        enrichedEnv.tables = new LinkedHashMap<>(env.getTables());
        enrichedEnv.tables.putAll(views);

        // merge functions
        enrichedEnv.functions = new HashMap<>(env.getFunctions());

        // enrich execution properties
        enrichedEnv.execution = ExecutionEntry.enrich(env.execution, properties);

        // enrich configuration properties
        enrichedEnv.configuration = ConfigurationEntry.enrich(env.configuration, properties);

        // enrich deployment properties
        enrichedEnv.deployment = DeploymentEntry.enrich(env.deployment, properties);

        // does not change session properties
        enrichedEnv.session = env.getSession();

        // does not change server properties
        enrichedEnv.server = env.getServer();

        enrichedEnv.deer = DeerEntry.enrich(env.deer,properties);

        return enrichedEnv;
    }
}
