package com.wbmd.kafka2db.failure;

public interface FailureStore {
    void record(FailureRecord failure) throws FailureStoreException;
}
