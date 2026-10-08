package com.wbmd.kafka2db.writer;

import com.wbmd.kafka2db.config.TopicMapping;
import com.wbmd.kafka2db.parser.ParsedMessage;

import java.sql.SQLException;

public interface TargetWriter {
    TargetType getTargetType();

    void write(TopicMapping mapping, ParsedMessage message) throws SQLException;
}

