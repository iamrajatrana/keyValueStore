package io.cred.exception;

public class TableDoesNotExistsException extends RuntimeException{
    public TableDoesNotExistsException(String message) {
        super(message);
    }
}
