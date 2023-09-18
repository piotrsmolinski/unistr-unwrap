package dev.piotrsmolinski.kafka.unistr;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.Transformation;

import java.util.Map;

public class UnistrUnwrap<R extends ConnectRecord<R>> implements Transformation<R> {

  private static ConfigDef CONFIG_DEF = buildConfig();

  @Override
  public ConfigDef config() {
    return CONFIG_DEF;
  }

  @Override
  public void configure(Map<String, ?> map) {
    // do nothing
  }

  @Override
  public void close() {
    // do nothing
  }

  @Override
  public R apply(R record) {
    return record.newRecord(
            record.topic(),
            record.kafkaPartition(),
            record.keySchema(),
            record.key(),
            record.valueSchema(),
            convertStruct((Struct)record.value()),
            record.timestamp(),
            record.headers()
    );
  }

  protected Struct convertStruct(Struct struct) {
    Struct result = new Struct(struct.schema());
    for (Field field : struct.schema().fields()) {
      result.put(field, convertField(struct.get(field)));
    }
    return result;
  }

  protected Object convertField(Object fieldValue) {
    if (fieldValue instanceof String) {
      return unwrap((String)fieldValue);
    } else {
      return fieldValue;
    }
  }

  protected String unwrap(String text) {
    if (!text.startsWith("UNISTR('") || !text.endsWith("')")) {
      return text;
    }
    StringBuilder builder = new StringBuilder(text.length() - 10);
    int p0 = 8;
    int p1 = text.length() - 2;
    for (int i = p0; i < p1; i++) {
      char c = text.charAt(i);
      switch (c) {
        case '\'':
          if (i >= p1-1) {
            throw new IllegalArgumentException("Single quote expected");
          }
          if (text.charAt(i+1) != '\'') {
            throw new IllegalArgumentException("Single quote expected");
          }
          builder.append('\'');
          i += 1;
          break;
        case '\\':
          if (i >= p1-5) {
            throw new IllegalArgumentException("Unicode code expected");
          }
          try {
            builder.append((char) Integer.parseInt(text.substring(i + 1, i + 5), 16));
          } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Unicode code expected");
          }
          i += 4;
          break;
        default:
          builder.append(c);
      }
    }
    return builder.toString();
  }

  protected static ConfigDef buildConfig() {
    return new ConfigDef();
  }

}
