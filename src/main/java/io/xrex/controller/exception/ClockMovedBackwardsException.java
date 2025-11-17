package io.xrex.controller.exception;

public class ClockMovedBackwardsException extends RuntimeException {
    public ClockMovedBackwardsException(String message) {
        super(message);
    }
}
