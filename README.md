Kafka Connect Redshift
======================

## Build

* Install the Redshift driver into the local maven repository.
    * Download the [Redshift JDBC 4.1 Driver](http://docs.aws.amazon.com/redshift/latest/mgmt/configure-jdbc-connection.html).
    * Copy the jar into `repo/com/amazonaws/redshiftjdbc41/${redshift.version}/`
* `mvn package`
* Copy `target/kafka-connect-redshift-VERSION-jar-with-dependencies.jar` into your classpath.


## Configure

* `name` - Connector identifier.
* `connector.class` - io.tenjin.kafka.connect.redshift.RedshiftSinkConnector
* `tasks.max` - Max number of tasks
* `topics` - Topics to read from Kafka.

* `temp.output.dir` - Output directory to write to the local filesystem.
* `s3.bucket` - S3 bucket to stage data for COPY.
* `aws.access.key.id` - AWS access key ID.
* `aws.secret.access.key` - AWS secret access key.
* `connection.url` - Redshift JDBC connection URL. If you're using Amazon's Redshift JDBC Driver, you may want to set
`OpenSourceSubProtocolOverride=true` in case the PostgreSQL driver is on your classpath.
* `table` - Redshift destination table name.
* `fields` - Field names to send to Redshift. Use * for all fields.

## Run

On Confluent Platform:
```bash
bin/connect-standalone etc/connect-standalone.properties <path-to-your-connect-redshift.properties> <other-connector.properties> ...
```

With Kafka only download:
```bash
bin/connect-standalone.sh config/connect-standalone.properties <path-to-your-connect-redshift.properties> <other-connector.properties> ...
```


## Known Limitations

* Untested. => Use at your own risk!
* Uses the local filesystem. => Make sure you have enough disk space!
* No WAL, probably doesn't recover well. => Contribute!
* Only 1 destination table supported.

If you're running the Confluent Platform on Java 8, there is an [issue with Joda Time and hadoop-aws]
(https://github.com/JodaOrg/joda-time/issues/288).  The workaround is to exclude the `share/java/kafka-connect-hdfs/joda-time-1.6.2.jar`
from the classpath when running the connectors.  The newer Joda Time classes should be included in the built jar.

## License

Apache v2.0

## Contribute

Help me out, please.  Thanks!
