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
package io.zeebe.broker.subscription.message.state;

import io.zeebe.broker.logstreams.state.ZbColumnFamilies;
import io.zeebe.db.ColumnFamily;
import io.zeebe.db.ZeebeDb;
import io.zeebe.db.impl.ZbCompositeKey;
import io.zeebe.db.impl.ZbLong;
import io.zeebe.db.impl.ZbNil;
import io.zeebe.db.impl.ZbString;
import org.agrona.DirectBuffer;

public class MessageState {

  /**
   * <pre>message key -> message
   */
  private final ColumnFamily<ZbLong, Message> messageColumnFamily;

  private final ZbLong messageKey;
  private final Message message;

  /**
   * <pre>name | correlation key | key -> []
   *
   * find message by name and correlation key - the message key ensures the queue ordering
   */
  private final ZbString messageName;

  private final ZbString correlationKey;
  private final ZbCompositeKey<ZbCompositeKey<ZbString, ZbString>, ZbLong>
      nameCorrelationMessageKey;
  private final ZbCompositeKey<ZbString, ZbString> nameAndCorrelationKey;
  private final ColumnFamily<ZbCompositeKey<ZbCompositeKey<ZbString, ZbString>, ZbLong>, ZbNil>
      nameCorrelationMessageColumnFamily;

  /**
   * <pre>deadline | key -> []
   *
   * find messages which are before a given timestamp */
  private final ZbLong deadline;

  private final ZbCompositeKey<ZbLong, ZbLong> deadlineMessageKey;
  private final ColumnFamily<ZbCompositeKey<ZbLong, ZbLong>, ZbNil> deadlineColumnFamily;

  /**
   * <pre>name | correlation key | message id -> []
   *
   * exist a message for a given message name, correlation key and message id */
  private final ZbString messageId;

  private final ZbCompositeKey<ZbCompositeKey<ZbString, ZbString>, ZbString>
      nameCorrelationMessageIdKey;
  private final ColumnFamily<ZbCompositeKey<ZbCompositeKey<ZbString, ZbString>, ZbString>, ZbNil>
      messageIdColumnFamily;

  /**
   * <pre>key | workflow instance key -> []
   *
   * check if a message is correlated to a workflow instance */
  private final ZbCompositeKey<ZbLong, ZbLong> messageWorkflowKey;

  private final ZbLong workflowInstanceKey;
  private final ColumnFamily<ZbCompositeKey<ZbLong, ZbLong>, ZbNil> correlatedMessageColumnFamily;

  private final ZeebeDb<ZbColumnFamilies> zeebeDb;

  public MessageState(ZeebeDb<ZbColumnFamilies> zeebeDb) {
    messageKey = new ZbLong();
    message = new Message();
    messageColumnFamily =
        zeebeDb.createColumnFamily(ZbColumnFamilies.MESSAGE_KEY, messageKey, message);

    messageName = new ZbString();
    correlationKey = new ZbString();
    nameAndCorrelationKey = new ZbCompositeKey<>();
    nameCorrelationMessageKey = new ZbCompositeKey<>();
    nameCorrelationMessageColumnFamily =
        zeebeDb.createColumnFamily(
            ZbColumnFamilies.MESSAGES, nameCorrelationMessageKey, ZbNil.INSTANCE);

    deadline = new ZbLong();
    deadlineMessageKey = new ZbCompositeKey<>();
    deadlineColumnFamily =
        zeebeDb.createColumnFamily(
            ZbColumnFamilies.MESSAGE_DEADLINES, deadlineMessageKey, ZbNil.INSTANCE);

    messageId = new ZbString();
    nameCorrelationMessageIdKey = new ZbCompositeKey<>();
    messageIdColumnFamily =
        zeebeDb.createColumnFamily(
            ZbColumnFamilies.MESSAGE_IDS, nameCorrelationMessageIdKey, ZbNil.INSTANCE);

    messageWorkflowKey = new ZbCompositeKey<>();
    workflowInstanceKey = new ZbLong();
    correlatedMessageColumnFamily =
        zeebeDb.createColumnFamily(
            ZbColumnFamilies.MESSAGE_CORRELATED, messageWorkflowKey, ZbNil.INSTANCE);

    this.zeebeDb = zeebeDb;
  }

  public void put(final Message message) {
    zeebeDb.batch(
        () -> {
          messageKey.wrapLong(message.getKey());
          messageColumnFamily.put(messageKey, message);

          messageName.wrapBuffer(message.getName());
          correlationKey.wrapBuffer(message.getCorrelationKey());
          nameAndCorrelationKey.wrapKeys(messageName, correlationKey);

          nameCorrelationMessageKey.wrapKeys(nameAndCorrelationKey, messageKey);
          nameCorrelationMessageColumnFamily.put(nameCorrelationMessageKey, ZbNil.INSTANCE);

          deadline.wrapLong(message.getDeadline());
          deadlineMessageKey.wrapKeys(deadline, messageKey);
          deadlineColumnFamily.put(deadlineMessageKey, ZbNil.INSTANCE);

          final DirectBuffer messageId = message.getId();
          if (messageId.capacity() > 0) {
            this.messageId.wrapBuffer(messageId);
            nameCorrelationMessageIdKey.wrapKeys(nameAndCorrelationKey, this.messageId);
            messageIdColumnFamily.put(nameCorrelationMessageIdKey, ZbNil.INSTANCE);
          }
        });
  }

  public void putMessageCorrelation(long messageKey, long workflowInstanceKey) {
    this.messageKey.wrapLong(messageKey);
    this.workflowInstanceKey.wrapLong(workflowInstanceKey);
    messageWorkflowKey.wrapKeys(this.messageKey, this.workflowInstanceKey);
    correlatedMessageColumnFamily.put(messageWorkflowKey, ZbNil.INSTANCE);
  }

  public boolean existMessageCorrelation(long messageKey, long workflowInstanceKey) {
    this.messageKey.wrapLong(messageKey);
    this.workflowInstanceKey.wrapLong(workflowInstanceKey);
    messageWorkflowKey.wrapKeys(this.messageKey, this.workflowInstanceKey);

    return correlatedMessageColumnFamily.exists(messageWorkflowKey);
  }

  public void visitMessages(
      final DirectBuffer name, final DirectBuffer correlationKey, final MessageVisitor visitor) {

    messageName.wrapBuffer(name);
    this.correlationKey.wrapBuffer(correlationKey);
    nameAndCorrelationKey.wrapKeys(messageName, this.correlationKey);

    nameCorrelationMessageColumnFamily.whileEqualPrefix(
        nameAndCorrelationKey,
        (compositeKey, nil) -> {
          final long messageKey = compositeKey.getSecond().getValue();
          final Message message = getMessage(messageKey);
          return visitor.visit(message);
        });
  }

  private Message getMessage(long messageKey) {
    this.messageKey.wrapLong(messageKey);
    return messageColumnFamily.get(this.messageKey);
  }

  public void visitMessagesWithDeadlineBefore(final long timestamp, MessageVisitor visitor) {
    deadlineColumnFamily.whileTrue(
        ((compositeKey, zbNil) -> {
          final long deadline = compositeKey.getFirst().getValue();
          if (deadline <= timestamp) {
            final long messageKey = compositeKey.getSecond().getValue();
            final Message message = getMessage(messageKey);
            return visitor.visit(message);
          }
          return false;
        }));
  }

  public boolean exist(
      final DirectBuffer name, final DirectBuffer correlationKey, final DirectBuffer messageId) {
    messageName.wrapBuffer(name);
    this.correlationKey.wrapBuffer(correlationKey);
    nameAndCorrelationKey.wrapKeys(messageName, this.correlationKey);
    this.messageId.wrapBuffer(messageId);
    nameCorrelationMessageIdKey.wrapKeys(nameAndCorrelationKey, this.messageId);

    return messageIdColumnFamily.exists(nameCorrelationMessageIdKey);
  }

  public void remove(final long key) {
    final Message message = getMessage(key);
    if (message == null) {
      return;
    }

    zeebeDb.batch(
        () -> {
          messageKey.wrapLong(message.getKey());
          messageColumnFamily.delete(messageKey);

          messageName.wrapBuffer(message.getName());
          this.correlationKey.wrapBuffer(message.getCorrelationKey());
          nameAndCorrelationKey.wrapKeys(messageName, this.correlationKey);
          nameCorrelationMessageKey.wrapKeys(nameAndCorrelationKey, messageKey);

          nameCorrelationMessageColumnFamily.delete(nameCorrelationMessageKey);

          final DirectBuffer messageId = message.getId();
          if (messageId.capacity() > 0) {
            this.messageId.wrapBuffer(messageId);
            nameCorrelationMessageIdKey.wrapKeys(nameAndCorrelationKey, this.messageId);
            messageIdColumnFamily.delete(nameCorrelationMessageIdKey);
          }

          deadline.wrapLong(message.getDeadline());
          deadlineMessageKey.wrapKeys(deadline, messageKey);
          deadlineColumnFamily.delete(deadlineMessageKey);

          correlatedMessageColumnFamily.whileEqualPrefix(
              messageKey,
              ((compositeKey, zbNil) -> {
                correlatedMessageColumnFamily.delete(compositeKey);
              }));
        });
  }

  @FunctionalInterface
  public interface MessageVisitor {
    boolean visit(Message message);
  }
}
