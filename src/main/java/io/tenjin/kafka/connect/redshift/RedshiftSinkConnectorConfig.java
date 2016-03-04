package io.tenjin.kafka.connect.redshift;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.List;
import java.util.Map;

import static org.apache.kafka.common.config.ConfigDef.Importance;
import static org.apache.kafka.common.config.ConfigDef.Type;

public class RedshiftSinkConnectorConfig extends AbstractConfig {

//  public static final String TOPIC_CONFIG = "topic";
//  private static final String TOPIC_DOC = "Kafka topic to consume messages.";

  public static final String S3_BUCKET_CONFIG = "s3.bucket";
  private static final String S3_BUCKET_DOC = "S3 bucket to stage data for COPY.";

  public static final String CONNECTION_URL_CONFIG = "connection.url";
  private static final String CONNECTION_URL_DOC = "Redshift JDBC connection URL.";

  public static final String TABLE_CONFIG = "table";
  private static final String TABLE_DOC = "Redshift destination table name.";

  public static final String FIELDS_CONFIG = "fields";
  private static final String FIELDS_DOC = "Field names to send to Redshift. Use * for all fields.";
  public static final String FIELDS_DEFAULT = "*";

  public static final String TEMP_OUTPUT_DIR_CONFIG = "temp.output.dir";
  private static final String TEMP_OUTPUT_DIR_DOC = "Output directory to write to the local filesystem.";
  public static final String TEMP_OUTPUT_DIR_DEFAULT = "/tmp/kafka-connect-redshift";

  public static final String AWS_ACCESS_KEY_ID_CONFIG = "aws.access.key.id";
  public static final String AWS_ACCESS_KEY_ID_DOC = "AWS Access Key ID";
  public static final String AWS_SECRET_ACCESS_KEY_CONFIG = "aws.secret.access.key";
  public static final String AWS_SECRET_ACCESS_KEY_DOC = "AWS Secret Access Key";

  public static ConfigDef baseConfigDef() {
    return new ConfigDef()
        .define(RedshiftSinkConnector.TOPICS_CONFIG, Type.LIST, Importance.HIGH, "Topic list")
        .define(S3_BUCKET_CONFIG, Type.STRING, Importance.HIGH, S3_BUCKET_DOC)
        .define(CONNECTION_URL_CONFIG, Type.STRING, Importance.HIGH, CONNECTION_URL_DOC)
        .define(FIELDS_CONFIG, Type.LIST, FIELDS_DEFAULT, Importance.HIGH, FIELDS_DOC)
        .define(TABLE_CONFIG, Type.STRING, Importance.HIGH, TABLE_DOC)
        .define(TEMP_OUTPUT_DIR_CONFIG, Type.STRING, TEMP_OUTPUT_DIR_DEFAULT, Importance.HIGH, TEMP_OUTPUT_DIR_DOC)
        .define(AWS_ACCESS_KEY_ID_CONFIG, Type.STRING, Importance.HIGH, AWS_ACCESS_KEY_ID_DOC)
        .define(AWS_SECRET_ACCESS_KEY_CONFIG, Type.PASSWORD, Importance.HIGH, AWS_SECRET_ACCESS_KEY_DOC);
  }

  static ConfigDef config = baseConfigDef();

  public RedshiftSinkConnectorConfig(Map<String, String> originals) {
    super(config, originals);
  }

  protected RedshiftSinkConnectorConfig(ConfigDef def, Map<String, String> props) {
    super(def, props);
  }

  public String getTopic() {
    return getList(RedshiftSinkConnector.TOPICS_CONFIG).get(0);
  }

  public String getS3Bucket() {
    return getString(S3_BUCKET_CONFIG);
  }

  public String getConnectionUrl() {
    return getString(CONNECTION_URL_CONFIG);
  }

  public String getTable() {
    return getString(TABLE_CONFIG);
  }

  public List<String> getFields() {
    return getList(FIELDS_CONFIG);
  }

  public String getTempOutputDir() {
    return getString(TEMP_OUTPUT_DIR_CONFIG);
  }

  public AWSCredentials getAwsCredentials() {
    return new BasicAWSCredentials(getString(AWS_ACCESS_KEY_ID_CONFIG), getPassword(AWS_SECRET_ACCESS_KEY_CONFIG).value());
  }

}
