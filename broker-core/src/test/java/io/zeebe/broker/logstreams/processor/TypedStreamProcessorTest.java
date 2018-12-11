/*
 * Zeebe Broker Core
 * Copyright © 2017 camunda services GmbH (info@camunda.com)
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package io.zeebe.broker.logstreams.processor;

import static io.zeebe.test.util.TestUtil.doRepeatedly;
import static io.zeebe.util.buffer.BufferUtil.wrapString;
import static org.assertj.core.api.Assertions.assertThat;

import io.zeebe.broker.logstreams.state.ZbColumnFamilies;
import io.zeebe.broker.util.Records;
import io.zeebe.broker.util.StreamProcessorControl;
import io.zeebe.broker.util.TestStreams;
import io.zeebe.db.impl.rocksdb.ZeebeRocksDbFactory;
import io.zeebe.logstreams.log.LogStream;
import io.zeebe.logstreams.log.LoggedEvent;
import io.zeebe.protocol.clientapi.RecordType;
import io.zeebe.protocol.clientapi.ValueType;
import io.zeebe.protocol.impl.record.value.deployment.DeploymentRecord;
import io.zeebe.protocol.impl.record.value.deployment.ResourceType;
import io.zeebe.protocol.intent.DeploymentIntent;
import io.zeebe.servicecontainer.testing.ServiceContainerRule;
import io.zeebe.test.util.AutoCloseableRule;
import io.zeebe.transport.ServerOutput;
import io.zeebe.util.sched.testing.ActorSchedulerRule;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class TypedStreamProcessorTest {
  public static final String STREAM_NAME = "foo";
  public static final int STREAM_PROCESSOR_ID = 144144;

  public TemporaryFolder tempFolder = new TemporaryFolder();
  public AutoCloseableRule closeables = new AutoCloseableRule();

  public ActorSchedulerRule actorSchedulerRule = new ActorSchedulerRule();
  public ServiceContainerRule serviceContainerRule = new ServiceContainerRule(actorSchedulerRule);

  @Rule
  public RuleChain ruleChain =
      RuleChain.outerRule(tempFolder)
          .around(actorSchedulerRule)
          .around(serviceContainerRule)
          .around(closeables);

  protected TestStreams streams;
  protected LogStream stream;

  @Mock protected ServerOutput output;

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);

    streams =
        new TestStreams(
            tempFolder.getRoot(), closeables, serviceContainerRule.get(), actorSchedulerRule.get());

    stream = streams.createLogStream(STREAM_NAME);
  }

  @Test
  public void shouldWriteSourceEventAndProducerOnBatch() {
    // given
    final TypedStreamEnvironment env =
        new TypedStreamEnvironment(streams.getLogStream(STREAM_NAME), output);

    final AtomicLong key = new AtomicLong();
    final TypedStreamProcessor streamProcessor =
        env.newStreamProcessor()
            .keyGenerator(() -> key.getAndIncrement())
            .onCommand(ValueType.DEPLOYMENT, DeploymentIntent.CREATE, new BatchProcessor())
            .build();

    final StreamProcessorControl streamProcessorControl =
        streams.initStreamProcessor(
            STREAM_NAME,
            STREAM_PROCESSOR_ID,
            ZeebeRocksDbFactory.newFactory(ZbColumnFamilies.class),
            (db) -> streamProcessor);
    streamProcessorControl.start();
    final long firstEventPosition =
        streams
            .newRecord(STREAM_NAME)
            .event(deployment("foo", ResourceType.BPMN_XML))
            .recordType(RecordType.COMMAND)
            .intent(DeploymentIntent.CREATE)
            .write();

    // when
    streamProcessorControl.unblock();

    final LoggedEvent writtenEvent =
        doRepeatedly(
                () ->
                    streams
                        .events(STREAM_NAME)
                        .filter(
                            e -> Records.isEvent(e, ValueType.DEPLOYMENT, DeploymentIntent.CREATED))
                        .findFirst())
            .until(o -> o.isPresent())
            .get();

    // then
    assertThat(writtenEvent.getProducerId()).isEqualTo(STREAM_PROCESSOR_ID);

    assertThat(writtenEvent.getSourceEventPosition()).isEqualTo(firstEventPosition);
  }

  protected DeploymentRecord deployment(final String name, final ResourceType resourceType) {
    final DeploymentRecord event = new DeploymentRecord();
    event
        .resources()
        .add()
        .setResourceType(resourceType)
        .setResource(wrapString("foo"))
        .setResourceName(wrapString(name));
    return event;
  }

  protected static class BatchProcessor implements TypedRecordProcessor<DeploymentRecord> {

    @Override
    public void processRecord(
        final TypedRecord<DeploymentRecord> record,
        final TypedResponseWriter responseWriter,
        final TypedStreamWriter streamWriter) {
      streamWriter.appendNewEvent(DeploymentIntent.CREATED, record.getValue());
      streamWriter.flush();
    }
  }
}
