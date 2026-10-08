package com.wbmd.kafka2db.writer;

public final class TargetWriteResult {
    private final TargetType target;
    private final boolean success;
    private final int attempts;
    private final Throwable error;

    private TargetWriteResult(TargetType target, boolean success, int attempts, Throwable error) {
        this.target = target;
        this.success = success;
        this.attempts = attempts;
        this.error = error;
    }

    public static TargetWriteResult success(TargetType target, int attempts) {
        return new TargetWriteResult(target, true, attempts, null);
    }

    public static TargetWriteResult failure(TargetType target, int attempts, Throwable error) {
        return new TargetWriteResult(target, false, attempts, error);
    }

    public TargetType getTarget() {
        return target;
    }

    public boolean isSuccess() {
        return success;
    }

    public int getAttempts() {
        return attempts;
    }

    public Throwable getError() {
        return error;
    }
}

