package io.zeebe.logstreams.processor;

import io.zeebe.db.ZeebeDb;

public interface StreamProcessorFactory {

  StreamProcessor createProcessor(ZeebeDb zeebeDb);
}
