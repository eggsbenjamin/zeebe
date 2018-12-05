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
package io.zeebe.broker.workflow.state;

import io.zeebe.db.ColumnFamily;
import io.zeebe.db.ZeebeDb;
import io.zeebe.db.impl.ZbLong;
import io.zeebe.db.impl.ZbString;
import io.zeebe.db.impl.rocksdb.ZbColumnFamilies;
import io.zeebe.logstreams.state.StateController;
import io.zeebe.logstreams.state.StateLifecycleListener;
import io.zeebe.protocol.impl.record.value.deployment.DeploymentRecord;
import java.io.File;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.agrona.DirectBuffer;

public class WorkflowState implements StateLifecycleListener {

  private static final byte[] WORKFLOW_VERSION_FAMILY_NAME =
      "workflowStateWorkflowVersion".getBytes();
  public static final byte[][] COLUMN_FAMILY_NAMES = {WORKFLOW_VERSION_FAMILY_NAME};

  public static List<byte[]> getColumnFamilyNames() {
    return Stream.of(
            COLUMN_FAMILY_NAMES,
            WorkflowPersistenceCache.COLUMN_FAMILY_NAMES,
            ElementInstanceState.COLUMN_FAMILY_NAMES,
            TimerInstanceState.COLUMN_FAMILY_NAMES)
        .flatMap(Stream::of)
        .collect(Collectors.toList());
  }

  // version
  private final ColumnFamily<ZbString, ZbLong> workflowVersionColumnFamily;
  private final NextValueManager nextValueManager;

  //
  private WorkflowPersistenceCache workflowPersistenceCache;
  private TimerInstanceState timerInstanceState;
  private ElementInstanceState elementInstanceState;

  public WorkflowState(ZeebeDb zeebeDb) {
    workflowVersionColumnFamily =
        zeebeDb.createColumnFamily(ZbColumnFamilies.WORKFLOW_VERSION, ZbLong.class, ZbLong.class);
    nextValueManager = new NextValueManager();



  }

  @Override
  public void onOpened(StateController stateController) {

    workflowPersistenceCache = new WorkflowPersistenceCache(stateController);
    timerInstanceState = new TimerInstanceState(stateController);
    elementInstanceState = new ElementInstanceState(stateController);
  }

  public int getNextWorkflowVersion(String bpmnProcessId) {
    return (int) nextValueManager.getNextValue(workflowVersionColumnFamily, bpmnProcessId);
  }

  public boolean putDeployment(long deploymentKey, DeploymentRecord deploymentRecord) {
    return workflowPersistenceCache.putDeployment(deploymentKey, deploymentRecord);
  }

  public DeployedWorkflow getWorkflowByProcessIdAndVersion(
      DirectBuffer bpmnProcessId, int version) {
    return workflowPersistenceCache.getWorkflowByProcessIdAndVersion(bpmnProcessId, version);
  }

  public DeployedWorkflow getWorkflowByKey(long workflowKey) {
    return workflowPersistenceCache.getWorkflowByKey(workflowKey);
  }

  public DeployedWorkflow getLatestWorkflowVersionByProcessId(DirectBuffer bpmnProcessId) {
    return workflowPersistenceCache.getLatestWorkflowVersionByProcessId(bpmnProcessId);
  }

  public Collection<DeployedWorkflow> getWorkflows() {
    return workflowPersistenceCache.getWorkflows();
  }

  public Collection<DeployedWorkflow> getWorkflowsByBpmnProcessId(DirectBuffer processId) {
    return workflowPersistenceCache.getWorkflowsByBpmnProcessId(processId);
  }

  public TimerInstanceState getTimerState() {
    return timerInstanceState;
  }

  /**
   * @return only a meaningful value after {@link WorkflowState#open(File, boolean)} was called,
   *     i.e. during the lifetime of the owning stream processor.
   */
  public ElementInstanceState getElementInstanceState() {
    return elementInstanceState;
  }
}
