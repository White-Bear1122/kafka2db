package com.wbmd.kafka2db.parser;

import com.wbmd.kafka2db.config.AppConfig;
import com.wbmd.kafka2db.config.TopicMapping;

import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CodingErrorAction;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

public final class KafkaMessageParser {
    private static final String FIELD_DELIMITER = ";";
    private static final String RECORD_DELIMITER = "~";
    private static final String GROUP_DELIMITER = "||";

    public ParsedMessage parse(byte[] payload,
                               TopicMapping.SourceSchema schema,
                               AppConfig.RuntimeConfig runtime) throws MessageParseException {
        if (payload == null) {
            throw new MessageParseException("Kafka message value is null");
        }
        if (payload.length > runtime.getMaxMessageBytes()) {
            throw new MessageParseException("Message exceeds maxMessageBytes: " + payload.length);
        }

        String message = decode(payload, schema.getCharset());
        if (!message.isEmpty() && message.charAt(0) == '\uFEFF') {
            message = message.substring(1);
        }
        if (schema.isRemoveLineBreaks()) {
            message = message.replace("\r", "").replace("\n", "");
        }

        int headerEnd = message.indexOf(RECORD_DELIMITER);
        if (headerEnd < 0) {
            throw new MessageParseException("Message header is not terminated by '~'");
        }
        if (!message.endsWith(GROUP_DELIMITER)) {
            throw new MessageParseException("Message is not terminated by '||'");
        }

        String headerText = message.substring(0, headerEnd);
        Map<String, String> header = fields(
                headerText, schema.getHeaderFieldCount(), "header", "headerField");

        String bodyText = message.substring(headerEnd + 1, message.length() - GROUP_DELIMITER.length());
        if (bodyText.isEmpty()) {
            throw new MessageParseException("Message contains no data groups");
        }

        String[] groups = split(bodyText, GROUP_DELIMITER);
        List<ParsedRecord> records = new ArrayList<ParsedRecord>();
        for (int groupIndex = 0; groupIndex < groups.length; groupIndex++) {
            parseGroup(groups[groupIndex], groupIndex + 1, schema, runtime, records);
        }
        if (records.isEmpty()) {
            throw new MessageParseException("Message contains no body records");
        }
        return new ParsedMessage(header, records);
    }

    private void parseGroup(String group,
                            int groupIndex,
                            TopicMapping.SourceSchema schema,
                            AppConfig.RuntimeConfig runtime,
                            List<ParsedRecord> output) throws MessageParseException {
        if (group.isEmpty()) {
            throw new MessageParseException("Data group " + groupIndex + " is empty");
        }
        if (!group.endsWith(RECORD_DELIMITER)) {
            throw new MessageParseException("Data group " + groupIndex + " is not terminated by '~'");
        }

        String content = group.substring(0, group.length() - RECORD_DELIMITER.length());
        String[] rows = split(content, RECORD_DELIMITER);
        for (int rowIndex = 0; rowIndex < rows.length; rowIndex++) {
            if (rows[rowIndex].isEmpty()) {
                throw new MessageParseException(
                        "Data group " + groupIndex + " contains an empty record at index " + (rowIndex + 1));
            }
            if (output.size() >= runtime.getMaxRecordsPerMessage()) {
                throw new MessageParseException(
                        "Message exceeds maxRecordsPerMessage: " + runtime.getMaxRecordsPerMessage());
            }
            Map<String, String> values = fields(
                    rows[rowIndex], schema.getBodyFieldCount(),
                    "group " + groupIndex + " record " + (rowIndex + 1), "boreholeField");
            output.add(new ParsedRecord(groupIndex, rowIndex + 1, values));
        }
    }

    private Map<String, String> fields(String value,
                                       int expectedCount,
                                       String location,
                                       String prefix) throws MessageParseException {
        String[] values = split(value, FIELD_DELIMITER);
        if (values.length != expectedCount) {
            throw new MessageParseException(
                    location + " field count mismatch: expected " + expectedCount + ", actual " + values.length);
        }
        Map<String, String> result = new LinkedHashMap<String, String>();
        for (int index = 0; index < values.length; index++) {
            result.put(prefix + (index + 1), values[index]);
        }
        return result;
    }

    private String decode(byte[] payload, String charsetName) throws MessageParseException {
        try {
            Charset charset = Charset.forName(charsetName);
            return charset.newDecoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)
                    .decode(ByteBuffer.wrap(payload))
                    .toString();
        } catch (CharacterCodingException e) {
            throw new MessageParseException("Message is not valid " + charsetName, e);
        } catch (RuntimeException e) {
            throw new MessageParseException("Unsupported charset: " + charsetName, e);
        }
    }

    private String[] split(String value, String delimiter) {
        return value.split(Pattern.quote(delimiter), -1);
    }
}
