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
package io.zeebe.broker.workflow.processor.boundary;

import static io.zeebe.broker.logstreams.state.DefaultZeebeDbFactory.DEFAULT_DB_FACTORY;
import static io.zeebe.msgpack.value.DocumentValue.EMPTY_DOCUMENT;
import static io.zeebe.test.util.MsgPackUtil.asMsgPack;
import static io.zeebe.util.buffer.BufferUtil.wrapString;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.zeebe.broker.logstreams.processor.TypedStreamWriter;
import io.zeebe.broker.logstreams.state.ZbColumnFamilies;
import io.zeebe.broker.logstreams.state.ZeebeState;
import io.zeebe.broker.workflow.model.element.ExecutableWorkflow;
import io.zeebe.broker.workflow.model.transformation.BpmnTransformer;
import io.zeebe.broker.workflow.state.DeployedWorkflow;
import io.zeebe.broker.workflow.state.ElementInstance;
import io.zeebe.broker.workflow.state.WorkflowState;
import io.zeebe.db.ZeebeDb;
import io.zeebe.model.bpmn.Bpmn;
import io.zeebe.model.bpmn.BpmnModelInstance;
import io.zeebe.protocol.impl.record.value.workflowinstance.WorkflowInstanceRecord;
import io.zeebe.protocol.intent.WorkflowInstanceIntent;
import java.util.List;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

public class BoundaryEventHelperTest {
  @Rule public final TemporaryFolder folder = new TemporaryFolder();
  private final BoundaryEventHelper helper = new BoundaryEventHelper();
  private final BpmnTransformer bpmnTransformer = new BpmnTransformer();

  private ZeebeState state;
  private TypedStreamWriter writer;
  private WorkflowState workflowState;
  private ZeebeDb<ZbColumnFamilies> zeebeDb;

  @Before
  public void setUp() throws Exception {
    zeebeDb = DEFAULT_DB_FACTORY.createDb(folder.newFolder("state"));
    this.state = new ZeebeState(zeebeDb);
    workflowState = spy(this.state.getWorkflowState());
    writer = mock(TypedStreamWriter.class);
  }

  @After
  public void tearDown() throws Exception {
    zeebeDb.close();
  }

  @Test
  public void shouldNotTriggerBoundaryEventIfAttachedToActivityNotActivated() {
    // given
    final DirectBuffer handlerNodeId = wrapString("event");
    final WorkflowInstanceIntent[] states =
        new WorkflowInstanceIntent[] {
          WorkflowInstanceIntent.ELEMENT_READY,
          WorkflowInstanceIntent.ELEMENT_COMPLETING,
          WorkflowInstanceIntent.ELEMENT_TERMINATING,
          WorkflowInstanceIntent.ELEMENT_COMPLETED,
          WorkflowInstanceIntent.ELEMENT_TERMINATED
        };
    final WorkflowInstanceRecord record = new WorkflowInstanceRecord().setElementId("activity");

    // when
    for (final WorkflowInstanceIntent state : states) {
      final ElementInstance attachedTo =
          workflowState.getElementInstanceState().newInstance(1, record, state);

      // then
      assertThat(helper.shouldTriggerBoundaryEvent(attachedTo, handlerNodeId)).isFalse();
    }
  }

  @Test
  public void shouldNotTriggerBoundaryEventIfTriggerIsSameAsAttachedTo() {
    // given
    final DirectBuffer handlerNodeId = wrapString("activity");
    final WorkflowInstanceRecord record = new WorkflowInstanceRecord().setElementId("activity");

    // when
    final ElementInstance attachedTo =
        workflowState
            .getElementInstanceState()
            .newInstance(1, record, WorkflowInstanceIntent.ELEMENT_ACTIVATED);

    // then
    assertThat(helper.shouldTriggerBoundaryEvent(attachedTo, handlerNodeId)).isFalse();
  }

  @Test
  public void shouldTriggerBoundaryEvent() {
    // given
    final DirectBuffer handlerNodeId = wrapString("event");
    final WorkflowInstanceRecord record = new WorkflowInstanceRecord().setElementId("activity");

    // when
    final ElementInstance attachedTo =
        workflowState
            .getElementInstanceState()
            .newInstance(1, record, WorkflowInstanceIntent.ELEMENT_ACTIVATED);

    // then
    assertThat(helper.shouldTriggerBoundaryEvent(attachedTo, handlerNodeId)).isTrue();
  }

  @Test
  public void shouldNotTerminateActivityIfNotConfiguredTo() {
    // given
    final DirectBuffer handlerNodeId = wrapString("event");
    final WorkflowInstanceRecord record = new WorkflowInstanceRecord().setElementId("activity");
    final ElementInstance attachedTo =
        workflowState
            .getElementInstanceState()
            .newInstance(1, record, WorkflowInstanceIntent.ELEMENT_ACTIVATED);

    // when
    createWorkflowFor("activity", "event", false);
    helper.triggerBoundaryEvent(workflowState, attachedTo, handlerNodeId, EMPTY_DOCUMENT, writer);

    // then
    assertThat(attachedTo.getState()).isEqualTo(WorkflowInstanceIntent.ELEMENT_ACTIVATED);
    verify(writer, Mockito.never())
        .appendFollowUpEvent(anyLong(), any(WorkflowInstanceIntent.class), any());
  }

  @Test
  public void shouldTerminateActivityIfConfiguredTo() {
    // given
    final DirectBuffer handlerNodeId = wrapString("event");
    final WorkflowInstanceRecord record = new WorkflowInstanceRecord().setElementId("activity");
    final ElementInstance attachedTo =
        workflowState
            .getElementInstanceState()
            .newInstance(1, record, WorkflowInstanceIntent.ELEMENT_ACTIVATED);

    // when
    createWorkflowFor("activity", "event", true);
    helper.triggerBoundaryEvent(workflowState, attachedTo, handlerNodeId, EMPTY_DOCUMENT, writer);

    // then
    assertThat(attachedTo.getState()).isEqualTo(WorkflowInstanceIntent.ELEMENT_TERMINATING);
    verify(writer, Mockito.times(1))
        .appendFollowUpEvent(
            attachedTo.getKey(), WorkflowInstanceIntent.ELEMENT_TERMINATING, attachedTo.getValue());
  }

  @Test
  public void shouldMarkActivityAsInterruptedByEventTrigger() {
    // given
    final DirectBuffer handlerNodeId = wrapString("event");
    final WorkflowInstanceRecord record = new WorkflowInstanceRecord().setElementId("activity");
    final ElementInstance attachedTo =
        workflowState
            .getElementInstanceState()
            .newInstance(1, record, WorkflowInstanceIntent.ELEMENT_ACTIVATED);

    // when
    createWorkflowFor("activity", "event", true);
    helper.triggerBoundaryEvent(workflowState, attachedTo, handlerNodeId, EMPTY_DOCUMENT, writer);

    // then
    final ArgumentCaptor<WorkflowInstanceRecord> argRecord =
        ArgumentCaptor.forClass(WorkflowInstanceRecord.class);
    final ArgumentCaptor<WorkflowInstanceIntent> argIntent =
        ArgumentCaptor.forClass(WorkflowInstanceIntent.class);
    assertThat(attachedTo.getState()).isEqualTo(WorkflowInstanceIntent.ELEMENT_TERMINATING);
    verify(writer, Mockito.times(1))
        .appendFollowUpEvent(anyLong(), argIntent.capture(), argRecord.capture());
    assertThat(argIntent.getValue()).isEqualTo(WorkflowInstanceIntent.ELEMENT_TERMINATING);
    assertThat(argRecord.getValue())
        .isEqualToComparingOnlyGivenFields(
            attachedTo.getValue(),
            "bpmnProcessId",
            "workflowKey",
            "payload",
            "scopeInstanceKey",
            "version",
            "workflowInstanceKey",
            "elementId");
    assertThat(attachedTo.getInterruptingEventTrigger().getHandlerNodeId())
        .isEqualTo(handlerNodeId);
    assertThat(attachedTo.getInterruptingEventTrigger().getPayload()).isEqualTo(EMPTY_DOCUMENT);
  }

  @Test
  public void shouldTriggerCatchEvent() {
    // given
    final DirectBuffer handlerNodeId = wrapString("event");
    final WorkflowInstanceRecord record =
        new WorkflowInstanceRecord()
            .setPayload(new UnsafeBuffer(asMsgPack("{ \"foo\": 1 }")))
            .setScopeInstanceKey(1)
            .setWorkflowKey(1)
            .setWorkflowInstanceKey(1)
            .setVersion(1)
            .setBpmnProcessId("process")
            .setElementId("activity");

    // when
    helper.triggerCatchEvent(record, handlerNodeId, EMPTY_DOCUMENT, writer);

    // then
    final ArgumentCaptor<WorkflowInstanceRecord> argRecord =
        ArgumentCaptor.forClass(WorkflowInstanceRecord.class);
    final ArgumentCaptor<WorkflowInstanceIntent> argIntent =
        ArgumentCaptor.forClass(WorkflowInstanceIntent.class);
    verify(writer, Mockito.times(1)).appendNewEvent(argIntent.capture(), argRecord.capture());
    assertThat(argIntent.getValue()).isEqualTo(WorkflowInstanceIntent.CATCH_EVENT_TRIGGERING);
    assertThat(argRecord.getValue())
        .isEqualToComparingOnlyGivenFields(
            record,
            "bpmnProcessId",
            "workflowKey",
            "scopeInstanceKey",
            "version",
            "workflowInstanceKey");
    assertThat(argRecord.getValue().getElementId()).isEqualTo(handlerNodeId);
    assertThat(argRecord.getValue().getPayload()).isEqualTo(EMPTY_DOCUMENT);
  }

  private void createWorkflowFor(
      String activityId, String boundaryEventId, boolean cancelActivity) {
    final BpmnModelInstance model = createBpmnModelFor(activityId, boundaryEventId, cancelActivity);
    final List<ExecutableWorkflow> transformed = bpmnTransformer.transformDefinitions(model);

    persistWorkflow(transformed.get(0));
  }

  private void persistWorkflow(ExecutableWorkflow workflow) {
    final DeployedWorkflow deployedWorkflow = mock(DeployedWorkflow.class);

    when(deployedWorkflow.getWorkflow()).thenReturn(workflow);
    when(workflowState.getWorkflowByKey(anyLong())).thenReturn(deployedWorkflow);
  }

  private BpmnModelInstance createBpmnModelFor(
      String activityId, String boundaryEventId, boolean cancelActivity) {
    return Bpmn.createExecutableProcess()
        .startEvent()
        .serviceTask(activityId, b -> b.zeebeTaskType("type"))
        .boundaryEvent(boundaryEventId)
        .cancelActivity(cancelActivity)
        .timerWithDuration("PT1S")
        .endEvent()
        .moveToActivity(activityId)
        .endEvent()
        .done();
  }
}
