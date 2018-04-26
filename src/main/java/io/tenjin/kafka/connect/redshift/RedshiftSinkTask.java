package io.tenjin.kafka.connect.redshift;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.PutObjectResult;
import io.tenjin.kafka.connect.redshift.utils.Version;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Struct;
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
    private Map<String, File> tempFiles;
    private Map<String, BufferedWriter> writers;

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
            fields = new ArrayList<>();
        }
        serializer = new DefaultCopySerializer(fields);
        s3 = new AmazonS3Client(config.getAwsCredentials());
    }

    private BufferedWriter getWriter(SinkRecord record, String tableName) {
        TopicPartition tp = new TopicPartition(record.topic(), record.kafkaPartition());
        BufferedWriter writer = writers.get(tp);
        if (writer == null) {
            try {
                File f = new File(tempDir, key(tp, tableName) +  ".dsv");
                tempFiles.put(key(tp, tableName), f);
                writer = new BufferedWriter(new FileWriter(f));
                writers.put(key(tp, tableName), writer);
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

        List<String> s3Urls = new ArrayList<>();
        Set<String> keys = tempFiles.keySet();
        for (String key : keys) {
            closeTempFiles(key);
            if (shouldCopyToRedshift(key)) {
                String url = putFileToS3(key);
                if (url != null)
                    s3Urls.add(url);
            }
        }

        if (!s3Urls.isEmpty()) {
            String manifestUrl = null;
            try {
                manifestUrl = createManifest(s3Urls);
                if (manifestUrl != null) {
                    copyFromS3ToRedshift(manifestUrl);
                }
            } catch (RedshiftCopyFailedException e) {
                LOGGER.error("Redshift copy failed. ", e);
                throw new ConnectException(e);
            }

        }
    }

    private void appendRecord(SinkRecord record) throws IOException {
        final Struct recordValue = (Struct) record.value();
        final String tableName = (String)recordValue.get("tableName");
        BufferedWriter writer = getWriter(record, tableName);
        String row = serializer.serializeRecord(record);
        writer.append(row);
        writer.newLine();
    }

    @Override
    public void flush(Map<TopicPartition, OffsetAndMetadata> map) {


    }


    private void closeTempFiles(String key) {
        try {
            BufferedWriter w = writers.get(key);
            if (w != null) {
                w.close();
                writers.remove(key);
            }
        } catch (IOException e) {
            LOGGER.warn("Couldn't close writer for topic-partition {}", key, e);
        }
    }

    private boolean shouldCopyToRedshift(String key) {
        return true;
    }

    private String putFileToS3(String fileName) {
        File file = tempFiles.get(fileName);
        if (file == null || !file.exists() || file.length() == 0)
            return null;

        SimpleDateFormat f = new SimpleDateFormat("yyyy/MM/dd");
        String key = String.format("%s/%s/%s/%s", config.getTopic(), f.format(new Date()), fileName.split("-")[2] , file.getName());
        String url = "s3://" + config.getS3Bucket() + "/" + key;
        try {
            s3.putObject(config.getS3Bucket(), key, file);
            if (file.delete()) {
                tempFiles.remove(fileName);
                writers.remove(fileName);
            }
        } catch (AmazonClientException e) {
            LOGGER.warn("Couldn't put file {} to S3 {}.", file.getName(), url, e);
            LOGGER.warn("Attempting to reopen writer for appending.");
            try {
                writers.put(fileName, new BufferedWriter(new FileWriter(file)));
            } catch (IOException e1) {
                LOGGER.error("Couldn't reopen writer for appending {}.", file.getName(), e1);
                throw new ConnectException(e1);
            }
        }
        return url;
    }

    private String createManifest(List<String> s3Urls) throws RedshiftCopyFailedException {
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

        String key = config.getTopic() + "/" + manifestFile.getName();
        String url = "s3://" + config.getS3Bucket() + "/" + key;
        try {
            final PutObjectResult putObjectResult = s3.putObject(config.getS3Bucket(), key, manifestFile);
            LOGGER.info("Manifest file MD5 : "+putObjectResult.getContentMd5());
        } catch (AmazonClientException e) {
            LOGGER.error("Couldn't put manifest file {} to S3 {}.", manifestFile.getName(), url, e);
            manifestFile.delete();
            throw new RedshiftCopyFailedException("Failed to write manifest to s3", e);
        }

        return url;
    }

    private void copyFromS3ToRedshift(String manifestUrl) throws RedshiftCopyFailedException {
        try (Connection conn = DriverManager.getConnection(config.getConnectionUrl());
             Statement stmt = conn.createStatement()) {
            String statement = copyStatement(manifestUrl);
            LOGGER.info("Executing statement :: " + statement);
            stmt.execute(statement);
        } catch (SQLException e) {
            throw new RedshiftCopyFailedException("Failed to copy data into Redshift.", e);
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

    private String key(TopicPartition tp, String tableName) {
        return new StringBuilder().append(tp.toString()).append("-").append(tableName).toString();
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
