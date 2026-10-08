package com.wbmd.kafka2db.writer;

public enum TargetType {
    TDENGINE("tdengine"),
    OCEANBASE("oceanbase");

    private final String configName;

    TargetType(String configName) {
        this.configName = configName;
    }

    public String getConfigName() {
        return configName;
    }
}

