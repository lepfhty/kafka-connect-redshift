package io.tenjin.kafka.connect.redshift;

import org.apache.kafka.common.config.ConfigDef;

import java.util.Map;

public class RedshiftSinkTaskConfig extends RedshiftSinkConnectorConfig {

  static ConfigDef config = baseConfigDef();

  public RedshiftSinkTaskConfig(Map<String, String> props) {
    super(config, props);
  }

}
