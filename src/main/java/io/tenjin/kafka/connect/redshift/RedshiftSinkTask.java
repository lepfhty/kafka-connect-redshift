package io.tenjin.kafka.connect.redshift;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import io.tenjin.kafka.connect.redshift.utils.Version;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.*;

public class RedshiftSinkTask extends SinkTask {

  private static final Logger LOGGER = LoggerFactory.getLogger(RedshiftSinkTask.class);

  private Map<String, String> configProperties;
  private RedshiftSinkTaskConfig config;

  private File tempDir;
  private Map<TopicPartition, File> tempFiles;
  private Map<TopicPartition, BufferedWriter> writers;

  private CopySerializer serializer;
  private AmazonS3 s3;

  @Override
  public void start(Map<String, String> properties) {
    configProperties = properties;
    config = new RedshiftSinkTaskConfig(configProperties);

    tempDir = new File(config.getTempOutputDir());
    tempDir.mkdirs();
    tempFiles = new HashMap<>();
    writers = new HashMap<>();

    List<String> fields = config.getFields();
    if (fields.size() == 1 && fields.get(0).equals("*")) {
      fields.clear();
    }
    serializer = new DefaultCopySerializer(fields);
    s3 = new AmazonS3Client(config.getAwsCredentials());
  }

  private BufferedWriter getWriter(SinkRecord record) {
    TopicPartition tp = new TopicPartition(record.topic(), record.kafkaPartition());
    BufferedWriter writer = writers.get(tp);
    if (writer == null) {
      try {
        File f = new File(tempDir, tp + ".dsv");
        tempFiles.put(tp, f);
        writer = new BufferedWriter(new FileWriter(f));
        writers.put(tp, writer);
      } catch (IOException e) {
        LOGGER.error("Couldn't create file for topic-partition {}.", tp, e);
        throw new ConnectException(e);
      }
    }
    return writer;
  }

 @Override
  public void put(Collection<SinkRecord> collection) {
    try {
      for (SinkRecord record : collection) {
        appendRecord(record);
      }
    } catch (IOException e) {
      LOGGER.warn("Failed to append records. Retrying batch.", e);
      throw new RetriableException(e);
    }
  }

  private void appendRecord(SinkRecord record) throws IOException {
    BufferedWriter writer = getWriter(record);
    String row = serializer.serializeRecord(record);
    writer.append(row);
    writer.newLine();
  }

  @Override
  public void flush(Map<TopicPartition, OffsetAndMetadata> map) {
    List<String> s3Urls = new ArrayList<>();
    for (TopicPartition tp : map.keySet()) {
      closeTempFiles(tp);
      if (shouldCopyToRedshift(tp)) {
        String url = putFileToS3(tp);
        if (url != null)
          s3Urls.add(url);
      }
    }

    if (!s3Urls.isEmpty()) {
      String manifestUrl = createManifest(s3Urls);
      if (manifestUrl != null)
        copyFromS3ToRedshift(manifestUrl);
    }
  }

  private void closeTempFiles(TopicPartition tp) {
    try {
      writers.get(tp).close();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  private boolean shouldCopyToRedshift(TopicPartition tp) {
    return true;
  }

  private String putFileToS3(TopicPartition tp) {
    File file = tempFiles.get(tp);
    if (file == null || !file.exists() || file.length() == 0)
      return null;

    SimpleDateFormat f = new SimpleDateFormat("yyyy/MM/dd");
    String key = String.format("%s/%s/%s", config.getTopic(), f.format(new Date()), file.getName());
    String url = "s3://" + config.getS3Bucket() + "/" + key;
    try {
      s3.putObject(config.getS3Bucket(), key, file);
      if (file.delete()) {
        tempFiles.remove(tp);
        writers.remove(tp);
      }
    } catch (AmazonClientException e) {
      LOGGER.warn("Couldn't put file {} to S3 {}.", file.getName(), url, e);
      LOGGER.warn("Attempting to reopen writer for appending.");
      try {
        writers.put(tp, new BufferedWriter(new FileWriter(file)));
      } catch (IOException e1) {
        LOGGER.error("Couldn't reopen writer for appending {}.", file.getName(), e1);
        throw new ConnectException(e1);
      }
    }
    return url;
  }

  private String createManifest(List<String> s3Urls) {
    File dir = new File(config.getTempOutputDir());
    File manifestFile = new File(dir, "manifest_" + System.currentTimeMillis() + ".json");
    BufferedWriter manifestWriter;
    try {
      manifestWriter = new BufferedWriter(new FileWriter(manifestFile));
      manifestWriter.append("{\"entries\":[");
      boolean first = true;
      for (String url : s3Urls) {
        if (!first) {
          manifestWriter.append(",");
        }
        manifestWriter.append("{\"url\":\"").append(url).append("\",\"mandatory\":true}");
        first = false;
      }
      manifestWriter.append("]}");
      manifestWriter.close();
    } catch (IOException e) {
      throw new ConnectException(e);
    }

    String key = config.getTopic() + "/" + manifestFile;
    String url = "s3://" + config.getS3Bucket() + "/" + key;
    try {
      s3.putObject(config.getS3Bucket(), key, manifestFile);
    } catch (AmazonClientException e) {
      LOGGER.warn("Couldn't put manifest file {} to S3 {}.", manifestFile.getName(), url, e);
      manifestFile.delete();
      return null;
    }

    return url;
  }

  private void copyFromS3ToRedshift(String manifestUrl) {
    try (Connection conn = DriverManager.getConnection(config.getConnectionUrl());
         Statement stmt = conn.createStatement()) {
      stmt.execute(copyStatement(manifestUrl));
    } catch (SQLException e) {
      LOGGER.warn("Failed to copy data into Redshift.", e);
    }
  }

  private String copyStatement(String manifestUrl) {
    AWSCredentials creds = config.getAwsCredentials();
    String copy = String.format("COPY %s FROM '%s' " +
        "CREDENTIALS 'aws_access_key_id=%s;aws_secret_access_key=%s' " +
        "MANIFEST %s",
        config.getTable(),
        manifestUrl,
        creds.getAWSAccessKeyId(),
        creds.getAWSSecretKey(),
        serializer.copyOptions());
    return copy;
  }

  @Override
  public void stop() {

  }

  @Override
  public String version() {
    return Version.getVersion();
  }

  @Override
  public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
    super.onPartitionsAssigned(partitions);
  }

  @Override
  public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
    super.onPartitionsRevoked(partitions);
  }
}
