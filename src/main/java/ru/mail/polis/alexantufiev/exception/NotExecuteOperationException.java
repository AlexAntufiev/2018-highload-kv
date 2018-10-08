package ru.mail.polis.alexantufiev.exception;

/**
 * Error with not execute action.
 *
 * @author Aleksey Antufev
 * @version 1.0.0
 * @since 1.0.0 08.10.2018
 */
public class NotExecuteOperationException extends RuntimeException {

    public NotExecuteOperationException(String message) {
        super(message);
    }

    public NotExecuteOperationException(String message, Throwable cause) {
        super(message, cause);
    }
}
