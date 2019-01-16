package io.tenjin.kafka.connect.redshift;

import io.tenjin.kafka.connect.redshift.utils.Version;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RedshiftSinkConnector extends SinkConnector {

  private Map<String, String> configProperties;
  private RedshiftSinkConnectorConfig config;

  @Override
  public void start(Map<String, String> properties) {
    configProperties = properties;
    config = new RedshiftSinkConnectorConfig(configProperties);
  }

  @Override
  public List<Map<String, String>> taskConfigs(int maxTasks) {
    List<Map<String, String>> taskConfigs = new ArrayList<>(maxTasks);
    for(int i = 0; i < maxTasks; i++) {
      taskConfigs.add(new HashMap<>(configProperties));
    }
    return taskConfigs;
  }

  @Override
  public void stop() {

  }

  @Override
  public ConfigDef config() {
    return RedshiftSinkConnectorConfig.config;
  }

  @Override
  public String version() {
    return Version.getVersion();
  }

  @Override
  public Class<? extends Task> taskClass() {
    return RedshiftSinkTask.class;
  }
}
