package io.tenjin.kafka.connect.redshift;

import org.apache.kafka.connect.sink.SinkRecord;

public interface CopySerializer {
  String serializeRecord(SinkRecord record);
  String copyOptions();
}
