package dev.piotrsmolinski.kafka.unistr;

import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;

import java.util.List;
import java.util.Map;

public class DummySourceTask extends SourceTask {

  @Override
  public String version() {
    return null;
  }

  @Override
  public void start(Map<String, String> props) {

  }

  @Override
  public List<SourceRecord> poll() throws InterruptedException {
    return null;
  }

  @Override
  public void stop() {

  }

}
