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
package io.zeebe.broker.workflow.processor.event;

import io.zeebe.broker.workflow.model.element.ExecutableCatchEventSupplier;
import io.zeebe.broker.workflow.processor.BpmnStepContext;
import io.zeebe.broker.workflow.processor.BpmnStepHandler;

public class SubscribeEventHandler implements BpmnStepHandler<ExecutableCatchEventSupplier> {

  @Override
  public void handle(final BpmnStepContext<ExecutableCatchEventSupplier> context) {
    final ExecutableCatchEventSupplier supplier = context.getElement();

    try {
      context.getCatchEventOutput().subscribeToCatchEvents(context, supplier.getEvents());

      context.getOutput().deferEvent(context.getRecord());
    } catch (Exception e) {
      // TODO (Phil): improve incident handling #1736
    }
  }
}
