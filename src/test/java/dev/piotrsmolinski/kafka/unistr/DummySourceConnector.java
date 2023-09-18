package dev.piotrsmolinski.kafka.unistr;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class DummySourceConnector extends SourceConnector {

  @Override
  public void start(Map<String, String> props) {

  }

  @Override
  public Class<? extends Task> taskClass() {
    return null;
  }

  @Override
  public List<Map<String, String>> taskConfigs(int maxTasks) {
    return Collections.emptyList();
  }

  @Override
  public void stop() {

  }

  @Override
  public ConfigDef config() {
    return new ConfigDef();
  }

  @Override
  public String version() {
    return null;
  }

}
