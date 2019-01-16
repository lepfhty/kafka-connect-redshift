package io.tenjin.kafka.connect.redshift;

/**
 * Created by somasundar.sekar on 4/18/2018.
 */
public class RedshiftCopyFailedException extends Exception {

    public RedshiftCopyFailedException() {
    }

    public RedshiftCopyFailedException(String message) {
        super(message);
    }

    public RedshiftCopyFailedException(String message, Throwable cause) {
        super(message, cause);
    }

    public RedshiftCopyFailedException(Throwable cause) {
        super(cause);
    }
}
