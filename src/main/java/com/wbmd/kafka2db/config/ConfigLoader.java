package com.wbmd.kafka2db.config;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public final class ConfigLoader {
    public static final String CONFIG_SYSTEM_PROPERTY = "kafka2db.config";
    public static final String CONFIG_ENVIRONMENT_VARIABLE = "KAFKA2DB_CONFIG";

    private ConfigLoader() {
    }

    public static AppConfig load(String[] args) {
        Path path = resolvePath(args);
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, true);
        try {
            AppConfig config = mapper.readValue(Files.newInputStream(path), AppConfig.class);
            ConfigValidator.validate(config);
            return config;
        } catch (IOException e) {
            throw new ConfigException("Failed to load configuration: " + path.toAbsolutePath(), e);
        }
    }

    static Path resolvePath(String[] args) {
        String configured = null;
        if (args != null && args.length > 0 && !isBlank(args[0])) {
            configured = args[0];
        }
        if (isBlank(configured)) {
            configured = System.getProperty(CONFIG_SYSTEM_PROPERTY);
        }
        if (isBlank(configured)) {
            configured = System.getenv(CONFIG_ENVIRONMENT_VARIABLE);
        }
        if (isBlank(configured)) {
            configured = "application.yaml";
        }
        Path path = Paths.get(configured).toAbsolutePath().normalize();
        if (!Files.isRegularFile(path)) {
            throw new ConfigException("Configuration file does not exist: " + path);
        }
        return path;
    }

    static boolean isBlank(String value) {
        return value == null || value.trim().isEmpty();
    }
}

