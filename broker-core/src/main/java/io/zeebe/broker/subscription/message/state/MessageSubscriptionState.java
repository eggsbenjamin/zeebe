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

public class MessageSubscriptionState {

  private final ZeebeDb<ZbColumnFamilies> zeebeDb;

  // (elementInstanceKey, messageName) => MessageSubscription
  private final ZbLong elementInstanceKey;
  private final ZbString messageName;
  private final MessageSubscription messageSubscription;
  private final ZbCompositeKey<ZbLong, ZbString> elementKeyAndMessageName;
  private final ColumnFamily<ZbCompositeKey<ZbLong, ZbString>, MessageSubscription>
      subscriptionColumnFamily;

  // (sentTime, elementInstanceKey, messageName) => \0
  private final ZbLong sentTime;
  private final ZbCompositeKey<ZbLong, ZbCompositeKey<ZbLong, ZbString>> sentTimeCompositeKey;
  private final ColumnFamily<ZbCompositeKey<ZbLong, ZbCompositeKey<ZbLong, ZbString>>, ZbNil>
      sentTimeColumnFamily;

  // (messageName, correlationKey, elementInstanceKey) => \0
  private final ZbString correlationKey;
  private final ZbCompositeKey<ZbString, ZbString> nameAndCorrelationKey;
  private final ZbCompositeKey<ZbCompositeKey<ZbString, ZbString>, ZbLong>
      nameCorrelationAndElementInstanceKey;
  private final ColumnFamily<ZbCompositeKey<ZbCompositeKey<ZbString, ZbString>, ZbLong>, ZbNil>
      messageNameAndCorrelationKeyColumnFamily;

  public MessageSubscriptionState(ZeebeDb<ZbColumnFamilies> zeebeDb) {
    this.zeebeDb = zeebeDb;

    elementInstanceKey = new ZbLong();
    messageName = new ZbString();
    messageSubscription = new MessageSubscription();
    elementKeyAndMessageName = new ZbCompositeKey<>();
    subscriptionColumnFamily =
        zeebeDb.createColumnFamily(
            ZbColumnFamilies.MESSAGE_SUBSCRIPTION_BY_KEY,
            elementKeyAndMessageName,
            messageSubscription);

    sentTime = new ZbLong();
    sentTimeCompositeKey = new ZbCompositeKey<>();
    sentTimeColumnFamily =
        zeebeDb.createColumnFamily(
            ZbColumnFamilies.MESSAGE_SUBSCRIPTION_BY_SENT_TIME,
            sentTimeCompositeKey,
            ZbNil.INSTANCE);

    correlationKey = new ZbString();
    nameAndCorrelationKey = new ZbCompositeKey<>();
    nameCorrelationAndElementInstanceKey = new ZbCompositeKey<>();
    messageNameAndCorrelationKeyColumnFamily =
        zeebeDb.createColumnFamily(
            ZbColumnFamilies.MESSAGE_SUBSCRIPTION_BY_NAME_AND_CORRELATION_KEY,
            nameCorrelationAndElementInstanceKey,
            ZbNil.INSTANCE);
  }

  public void put(final MessageSubscription subscription) {
    zeebeDb.batch(
        () -> {
          elementInstanceKey.wrapLong(subscription.getElementInstanceKey());
          messageName.wrapBuffer(subscription.getMessageName());
          elementKeyAndMessageName.wrapKeys(elementInstanceKey, messageName);

          subscriptionColumnFamily.put(elementKeyAndMessageName, subscription);

          correlationKey.wrapBuffer(subscription.getCorrelationKey());
          nameAndCorrelationKey.wrapKeys(messageName, correlationKey);
          nameCorrelationAndElementInstanceKey.wrapKeys(nameAndCorrelationKey, elementInstanceKey);
          messageNameAndCorrelationKeyColumnFamily.put(
              nameCorrelationAndElementInstanceKey, ZbNil.INSTANCE);
        });
  }

  public void visitSubscriptions(
      final DirectBuffer messageName,
      final DirectBuffer correlationKey,
      MessageSubscriptionVisitor visitor) {

    this.messageName.wrapBuffer(messageName);
    this.correlationKey.wrapBuffer(correlationKey);
    nameAndCorrelationKey.wrapKeys(this.messageName, this.correlationKey);

    messageNameAndCorrelationKeyColumnFamily.whileEqualPrefix(
        nameAndCorrelationKey,
        (compositeKey, nil) -> {
          final ZbLong elementInstance = compositeKey.getSecond();
          elementKeyAndMessageName.wrapKeys(elementInstance, this.messageName);
          return visitMessageSubscription(elementKeyAndMessageName, visitor);
        });
  }

  private Boolean visitMessageSubscription(
      ZbCompositeKey<ZbLong, ZbString> elementKeyAndMessageName,
      MessageSubscriptionVisitor visitor) {
    final MessageSubscription messageSubscription =
        subscriptionColumnFamily.get(elementKeyAndMessageName);

    if (messageSubscription == null) {
      throw new IllegalStateException(
          String.format(
              "Expected to find subscription with key %d and %s, but no subscription found",
              elementKeyAndMessageName.getFirst().getValue(),
              elementKeyAndMessageName.getSecond()));
    }
    return visitor.visit(messageSubscription);
  }

  public void updateToCorrelatingState(
      final MessageSubscription subscription, DirectBuffer messagePayload, long sentTime) {
    subscription.setMessagePayload(messagePayload);
    updateSentTime(subscription, sentTime);
  }

  public void updateSentTime(final MessageSubscription subscription, long sentTime) {
    zeebeDb.batch(
        () -> {
          elementInstanceKey.wrapLong(subscription.getElementInstanceKey());
          messageName.wrapBuffer(subscription.getMessageName());
          elementKeyAndMessageName.wrapKeys(elementInstanceKey, messageName);

          removeSubscriptionFromSentTimeColumnFamily(subscription);

          subscription.setCommandSentTime(sentTime);
          subscriptionColumnFamily.put(elementKeyAndMessageName, subscription);

          this.sentTime.wrapLong(subscription.getCommandSentTime());
          sentTimeCompositeKey.wrapKeys(this.sentTime, elementKeyAndMessageName);
          sentTimeColumnFamily.put(sentTimeCompositeKey, ZbNil.INSTANCE);
        });
  }

  public void visitSubscriptionBefore(final long deadline, MessageSubscriptionVisitor visitor) {
    sentTimeColumnFamily.whileTrue(
        (compositeKey, nil) -> {
          final long sentTime = compositeKey.getFirst().getValue();
          if (sentTime < deadline) {
            return visitMessageSubscription(compositeKey.getSecond(), visitor);
          }
          return false;
        });
  }

  public boolean existSubscriptionForElementInstance(
      long elementInstanceKey, DirectBuffer messageName) {
    this.elementInstanceKey.wrapLong(elementInstanceKey);
    this.messageName.wrapBuffer(messageName);
    elementKeyAndMessageName.wrapKeys(this.elementInstanceKey, this.messageName);

    return subscriptionColumnFamily.exists(elementKeyAndMessageName);
  }

  public boolean remove(long elementInstanceKey, DirectBuffer messageName) {
    this.elementInstanceKey.wrapLong(elementInstanceKey);
    this.messageName.wrapBuffer(messageName);
    elementKeyAndMessageName.wrapKeys(this.elementInstanceKey, this.messageName);

    final MessageSubscription messageSubscription =
        subscriptionColumnFamily.get(elementKeyAndMessageName);

    final boolean found = messageSubscription != null;
    if (found) {
      remove(messageSubscription);
    }
    return found;
  }

  private void remove(final MessageSubscription subscription) {
    zeebeDb.batch(
        () -> {
          subscriptionColumnFamily.delete(elementKeyAndMessageName);

          messageName.wrapBuffer(subscription.getMessageName());
          correlationKey.wrapBuffer(subscription.getCorrelationKey());
          nameAndCorrelationKey.wrapKeys(messageName, correlationKey);
          nameCorrelationAndElementInstanceKey.wrapKeys(nameAndCorrelationKey, elementInstanceKey);
          messageNameAndCorrelationKeyColumnFamily.delete(nameCorrelationAndElementInstanceKey);

          removeSubscriptionFromSentTimeColumnFamily(subscription);
        });
  }

  private void removeSubscriptionFromSentTimeColumnFamily(MessageSubscription subscription) {
    if (subscription.getCommandSentTime() > 0) {
      sentTime.wrapLong(subscription.getCommandSentTime());
      sentTimeCompositeKey.wrapKeys(sentTime, elementKeyAndMessageName);
      sentTimeColumnFamily.delete(sentTimeCompositeKey);
    }
  }

  @FunctionalInterface
  public interface MessageSubscriptionVisitor {
    boolean visit(MessageSubscription subscription);
  }
}
