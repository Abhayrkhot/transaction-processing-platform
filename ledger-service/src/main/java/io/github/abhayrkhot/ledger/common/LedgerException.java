package io.github.abhayrkhot.ledger.common;

import org.springframework.http.HttpStatus;

/**
 * A request that can never succeed as sent. The API maps it to a 4xx response and the
 * Kafka consumer sends the message straight to the dead-letter topic without retrying.
 */
public class LedgerException extends RuntimeException {

    private final HttpStatus status;
    private final String code;

    public LedgerException(HttpStatus status, String code, String message) {
        super(message);
        this.status = status;
        this.code = code;
    }

    public HttpStatus status() {
        return status;
    }

    public String code() {
        return code;
    }

    public static LedgerException notFound(String what, Object id) {
        return new LedgerException(HttpStatus.NOT_FOUND, what.toUpperCase() + "_NOT_FOUND", what + " " + id + " not found");
    }

    public static LedgerException invalid(String code, String message) {
        return new LedgerException(HttpStatus.UNPROCESSABLE_CONTENT, code, message);
    }

    public static LedgerException badRequest(String code, String message) {
        return new LedgerException(HttpStatus.BAD_REQUEST, code, message);
    }

    public static LedgerException conflict(String code, String message) {
        return new LedgerException(HttpStatus.CONFLICT, code, message);
    }
}
