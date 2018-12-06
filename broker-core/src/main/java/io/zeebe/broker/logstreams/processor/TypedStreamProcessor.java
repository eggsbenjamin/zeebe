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

import io.zeebe.logstreams.log.LogStream;
import io.zeebe.logstreams.log.LogStreamRecordWriter;
import io.zeebe.logstreams.log.LoggedEvent;
import io.zeebe.logstreams.processor.EventProcessor;
import io.zeebe.logstreams.processor.StreamProcessor;
import io.zeebe.logstreams.processor.StreamProcessorContext;
import io.zeebe.logstreams.state.StateController;
import io.zeebe.msgpack.UnpackedObject;
import io.zeebe.protocol.clientapi.ValueType;
import io.zeebe.protocol.impl.record.RecordMetadata;
import io.zeebe.transport.ServerOutput;
import io.zeebe.util.ReflectUtil;
import io.zeebe.util.sched.ActorControl;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;

@SuppressWarnings({"unchecked"})
public class TypedStreamProcessor implements StreamProcessor {

  protected final ServerOutput output;
  protected final RecordProcessorMap recordProcessors;
  protected final List<StreamProcessorLifecycleAware> lifecycleListeners = new ArrayList<>();
  private final KeyGenerator keyGenerator;

  protected final RecordMetadata metadata = new RecordMetadata();
  protected final EnumMap<ValueType, Class<? extends UnpackedObject>> eventRegistry;
  protected final EnumMap<ValueType, UnpackedObject> eventCache;

  protected final TypedEventImpl typedEvent = new TypedEventImpl();
  private final TypedStreamEnvironment environment;

  protected DelegatingEventProcessor eventProcessorWrapper;
  protected ActorControl actor;
  private StreamProcessorContext streamProcessorContext;
  private StateController stateController;

  public TypedStreamProcessor(
      final ServerOutput output,
      final RecordProcessorMap recordProcessors,
      final List<StreamProcessorLifecycleAware> lifecycleListeners,
      final EnumMap<ValueType, Class<? extends UnpackedObject>> eventRegistry,
      final KeyGenerator keyGenerator,
      final TypedStreamEnvironment environment,
      final StateController stateController) {
    this.output = output;
    this.recordProcessors = recordProcessors;
    this.keyGenerator = keyGenerator;
    recordProcessors.values().forEachRemaining(p -> this.lifecycleListeners.add(p));

    this.lifecycleListeners.addAll(lifecycleListeners);

    this.eventCache = new EnumMap<>(ValueType.class);

    eventRegistry.forEach((t, c) -> eventCache.put(t, ReflectUtil.newInstance(c)));
    this.eventRegistry = eventRegistry;
    this.environment = environment;
    this.stateController = stateController;
  }

  @Override
  public void onOpen(final StreamProcessorContext context) {
    this.eventProcessorWrapper =
        new DelegatingEventProcessor(
            context.getId(), output, context.getLogStream(), eventRegistry, keyGenerator);

    this.actor = context.getActorControl();
    this.streamProcessorContext = context;
    lifecycleListeners.forEach(e -> e.onOpen(this));
  }

  @Override
  public void onRecovered() {
    lifecycleListeners.forEach(e -> e.onRecovered(this));
  }

  @Override
  public void onClose() {
    lifecycleListeners.forEach(e -> e.onClose());
  }

  @Override
  public StateController getStateController() {
    return stateController;
  }

  @Override
  public EventProcessor onEvent(final LoggedEvent event) {
    final long position = event.getPosition();
    metadata.reset();
    event.readMetadata(metadata);

    final TypedRecordProcessor<?> currentProcessor =
        recordProcessors.get(
            metadata.getRecordType(), metadata.getValueType(), metadata.getIntent().value());

    if (currentProcessor != null) {
      final UnpackedObject value = eventCache.get(metadata.getValueType());
      value.reset();
      event.readValue(value);

      typedEvent.wrap(event, metadata, value);
      eventProcessorWrapper.wrap(currentProcessor, typedEvent, position);
      return eventProcessorWrapper;
    } else {
      return null;
    }
  }

  public MetadataFilter buildTypeFilter() {
    return m ->
        recordProcessors.containsKey(m.getRecordType(), m.getValueType(), m.getIntent().value());
  }

  protected static class DelegatingEventProcessor implements EventProcessor {

    protected final int streamProcessorId;
    protected final LogStream logStream;
    protected final TypedStreamWriterImpl writer;
    protected final TypedResponseWriterImpl responseWriter;

    protected TypedRecordProcessor<?> eventProcessor;
    protected TypedEventImpl event;
    private SideEffectProducer sideEffectProducer;
    private long position;

    public DelegatingEventProcessor(
        final int streamProcessorId,
        final ServerOutput output,
        final LogStream logStream,
        final EnumMap<ValueType, Class<? extends UnpackedObject>> eventRegistry,
        final KeyGenerator keyGenerator) {
      this.streamProcessorId = streamProcessorId;
      this.logStream = logStream;
      this.writer = new TypedStreamWriterImpl(logStream, eventRegistry, keyGenerator);
      this.responseWriter = new TypedResponseWriterImpl(output, logStream.getPartitionId());
    }

    public void wrap(
        final TypedRecordProcessor<?> eventProcessor,
        final TypedEventImpl event,
        final long position) {
      this.eventProcessor = eventProcessor;
      this.event = event;
      this.position = position;
    }

    @Override
    public void processEvent() {
      writer.reset();
      responseWriter.reset();

      this.writer.configureSourceContext(streamProcessorId, position);

      // default side effect is responses; can be changed by processor
      sideEffectProducer = responseWriter;

      eventProcessor.processRecord(
          position, event, responseWriter, writer, this::setSideEffectProducer);
    }

    public void setSideEffectProducer(final SideEffectProducer sideEffectProducer) {
      this.sideEffectProducer = sideEffectProducer;
    }

    @Override
    public boolean executeSideEffects() {
      return sideEffectProducer.flush();
    }

    @Override
    public long writeEvent(final LogStreamRecordWriter writer) {
      return this.writer.flush();
    }
  }

  public ActorControl getActor() {
    return actor;
  }

  public StreamProcessorContext getStreamProcessorContext() {
    return streamProcessorContext;
  }

  public TypedStreamEnvironment getEnvironment() {
    return environment;
  }

  public KeyGenerator getKeyGenerator() {
    return keyGenerator;
  }
}
