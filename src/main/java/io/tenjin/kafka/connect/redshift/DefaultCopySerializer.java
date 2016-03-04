package io.tenjin.kafka.connect.redshift;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;

public class DefaultCopySerializer implements CopySerializer {

  private static final DateFormat FORMAT;
  private List<String> fields;

  static {
    FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSSSSS");
    FORMAT.setTimeZone(TimeZone.getTimeZone("UTC"));
  }

  public DefaultCopySerializer(List<String> fields) {
    this.fields = new ArrayList<>(fields);
  }

  private static final String DELIMITER = "|";
  private static final String NULL = "\\N";
  private static final String ESCAPE = "\\";
  private static final String NEWLINE = "\n";

  private static final String ESCAPE_ESCAPE = ESCAPE + ESCAPE;
  private static final String ESCAPE_NEWLINE = ESCAPE + NEWLINE;
  private static final String ESCAPE_DELIMITER = ESCAPE + DELIMITER;

  private static final String COPY_OPTIONS = "ESCAPE DELIMITER '" + DELIMITER + "'";

  public String serializeRecord(SinkRecord record) {
    Struct struct = (Struct) record.value();
    StringBuilder b = new StringBuilder();
    boolean first = true;
    List<String> fieldsList = fields;
    if (fields.isEmpty()) {
      for (Field f : record.valueSchema().fields())
        fields.add(f.name());
    }
    for (String field : fieldsList) {
      if (!first)
        b.append(DELIMITER);
      Object value = struct.get(field);
      if (value == null)
        b.append(NULL);
      else if (value instanceof Date) {
        b.append(FORMAT.format(value));
      }
      else if (value instanceof String) {
        String str = value.toString()
            .replace(ESCAPE, ESCAPE_ESCAPE)
            .replace(DELIMITER, ESCAPE_DELIMITER)
            .replace(NEWLINE, ESCAPE_NEWLINE);
        b.append(str);
      }
      else {
        b.append(value);
      }
      first = false;
    }
    return b.toString();
  }

  public String copyOptions() {
    return COPY_OPTIONS;
  }
}
