package org.camunda.tngp.broker.it.taskqueue;

import static org.assertj.core.api.Assertions.assertThat;
import static org.camunda.tngp.test.util.TestUtil.waitUntil;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

import org.camunda.tngp.broker.it.ClientRule;
import org.camunda.tngp.broker.it.EmbeddedBrokerRule;
import org.camunda.tngp.client.AsyncTasksClient;
import org.camunda.tngp.client.ClientProperties;
import org.camunda.tngp.client.TngpClient;
import org.camunda.tngp.client.task.PollableTaskSubscription;
import org.camunda.tngp.client.task.Task;
import org.camunda.tngp.client.task.TaskHandler;
import org.camunda.tngp.client.task.TaskSubscription;
import org.camunda.tngp.test.util.TestUtil;
import org.camunda.tngp.util.time.ClockUtil;
import org.junit.After;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.RuleChain;
import org.junit.rules.Timeout;

public class TaskSubscriptionTest
{
    public EmbeddedBrokerRule brokerRule = new EmbeddedBrokerRule();

    public ClientRule clientRule = new ClientRule(() ->
    {
        final Properties properties = new Properties();

        properties.put(ClientProperties.CLIENT_TASK_EXECUTION_AUTOCOMPLETE, false);

        return properties;
    });

    @Rule
    public RuleChain ruleChain = RuleChain
        .outerRule(brokerRule)
        .around(clientRule);

    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Rule
    public Timeout timeout = Timeout.seconds(10);

    @After
    public void cleanUp()
    {
        ClockUtil.reset();
    }

    @Test
    public void shouldOpenSubscription() throws InterruptedException
    {
        // given
        final TngpClient client = clientRule.getClient();
        final AsyncTasksClient taskService = client.tasks();

        final Long taskKey = taskService.create()
            .topicId(0)
            .taskType("foo")
            .execute();

        // when
        final RecordingTaskHandler taskHandler = new RecordingTaskHandler();

        taskService.newSubscription()
            .handler(taskHandler)
            .topicId(0)
            .taskType("foo")
            .lockTime(Duration.ofMinutes(5))
            .lockOwner(1)
            .open();

        // then
        waitUntil(() -> !taskHandler.handledTasks.isEmpty());

        assertThat(taskHandler.handledTasks).hasSize(1);
        assertThat(taskHandler.handledTasks.get(0).getKey()).isEqualTo(taskKey);
    }

    @Test
    public void shouldCompleteTask() throws InterruptedException
    {
        // given
        final TngpClient client = clientRule.getClient();
        final AsyncTasksClient taskService = client.tasks();

        final Long taskKey = taskService.create()
            .topicId(0)
            .taskType("foo")
            .payload("{ \"a\" : 1 }")
            .addHeader("b", "2")
            .execute();

        final RecordingTaskHandler taskHandler = new RecordingTaskHandler();

        taskService.newSubscription()
            .handler(taskHandler)
            .topicId(0)
            .taskType("foo")
            .lockTime(Duration.ofMinutes(5))
            .lockOwner(5)
            .open();

        waitUntil(() -> !taskHandler.handledTasks.isEmpty());

        // when
        final Long result = taskService.complete()
            .taskKey(taskKey)
            .topicId(0)
            .lockOwner(5)
            .taskType("foo")
            .payload("{ \"a\" : 2 }")
            .addHeader("b", "3")
            .addHeader("c", "4")
            .execute();

        // then
        assertThat(result).isEqualTo(taskKey);
    }

    @Test
    public void shouldCompletionTaskInHandler() throws InterruptedException
    {
        // given
        final TngpClient client = clientRule.getClient();
        final AsyncTasksClient taskService = client.tasks();

        final Long taskKey = taskService.create()
            .topicId(0)
            .taskType("foo")
            .payload("{\"a\":1}")
            .addHeader("b", "2")
            .execute();

        // when
        final AtomicBoolean taskCompleted = new AtomicBoolean(false);

        final RecordingTaskHandler taskHandler = new RecordingTaskHandler(task ->
        {
            assertThat(task.getKey()).isEqualTo(taskKey);
            assertThat(task.getType()).isEqualTo("foo");
            assertThat(task.getWorkflowInstanceId()).isNull();
            assertThat(task.getLockExpirationTime()).isGreaterThan(Instant.now());

            final String payload = task.getPayload();
            final Map<String, String> headers = task.getHeaders();

            assertThat(payload).isEqualTo("{\"a\":1}");
            assertThat(headers).hasSize(1).containsEntry("b", "2");

            task.setPayload("{\"a\":3}");

            headers.put("b", "4");
            headers.put("c", "5");
            task.setHeaders(headers);

            task.complete();

            taskCompleted.set(true);
        });

        taskService.newSubscription()
            .handler(taskHandler)
            .topicId(0)
            .taskType("foo")
            .lockTime(Duration.ofMinutes(5))
            .lockOwner(2)
            .open();

        // then
        waitUntil(() -> !taskHandler.handledTasks.isEmpty());

        assertThat(taskHandler.handledTasks).hasSize(1);

        assertThat(taskCompleted.get()).isTrue();
    }

    @Test
    public void shouldCloseSubscription() throws InterruptedException
    {
        // given
        final TngpClient client = clientRule.getClient();
        final AsyncTasksClient taskService = client.tasks();

        final RecordingTaskHandler taskHandler = new RecordingTaskHandler();

        final TaskSubscription subscription = taskService.newSubscription()
                .handler(taskHandler)
                .topicId(0)
                .taskType("foo")
                .lockTime(Duration.ofMinutes(5))
                .lockOwner(1)
                .open();

        // when
        subscription.close();

        // then
        taskService.create()
            .topicId(0)
            .taskType("foo")
            .execute();

        Thread.sleep(1000L);

        assertThat(taskHandler.handledTasks).isEmpty();
    }

    @Test
    public void shouldFetchAndHandleTasks()
    {
        // given
        final TngpClient client = clientRule.getClient();

        final int numTasks = 100;
        for (int i = 0; i < numTasks; i++)
        {
            client.tasks().create()
                .topicId(0)
                .taskType("foo")
                .execute();
        }

        final RecordingTaskHandler handler = new RecordingTaskHandler(Task::complete);

        client.tasks().newSubscription()
            .handler(handler)
            .topicId(0)
            .taskType("foo")
            .lockTime(Duration.ofMinutes(5))
            .lockOwner(1)
            .taskFetchSize(10)
            .open();

        // when
        waitUntil(() -> handler.handledTasks.size() == numTasks);

        // then
        assertThat(handler.handledTasks).hasSize(numTasks);
    }

    @Test
    public void shouldMarkTaskAsFailedAsRetryIfHandlerThrowsException()
    {
        // given
        final TngpClient client = clientRule.getClient();
        final AsyncTasksClient taskService = client.tasks();

        final Long taskKey = taskService.create()
            .topicId(0)
            .taskType("foo")
            .execute();

        final RecordingTaskHandler taskHandler = new RecordingTaskHandler(
            t ->
            {
                throw new RuntimeException("expected failure");
            },
            Task::complete
            );

        // when
        taskService.newSubscription()
            .handler(taskHandler)
            .topicId(0)
            .taskType("foo")
            .lockTime(Duration.ofMinutes(5))
            .lockOwner(1)
            .open();

        // then the subscription is not broken and other tasks are still handled
        waitUntil(() -> taskHandler.getHandledTasks().size() == 2);

        assertThat(taskHandler.getHandledTasks()).extracting("key").contains(taskKey, taskKey);
    }

    @Test
    public void shouldExpireTaskLock() throws InterruptedException
    {
        // given
        final TngpClient client = clientRule.getClient();
        final AsyncTasksClient taskService = client.tasks();

        final Long taskKey = taskService.create()
            .topicId(0)
            .taskType("foo")
            .execute();

        final RecordingTaskHandler taskHandler = new RecordingTaskHandler(task ->
        {
            // don't complete the task - just wait for lock expiration
        });

        taskService.newSubscription()
            .handler(taskHandler)
            .topicId(0)
            .taskType("foo")
            .lockTime(Duration.ofMinutes(5))
            .lockOwner(5)
            .open();

        waitUntil(() -> taskHandler.handledTasks.size() == 1);

        // when
        ClockUtil.setCurrentTime(Instant.now().plus(Duration.ofMinutes(5)));

        // then
        waitUntil(() -> taskHandler.handledTasks.size() == 2);

        assertThat(taskHandler.handledTasks)
            .hasSize(2)
            .extracting("key").contains(taskKey, taskKey);
    }


    @Ignore
    @Test
    public void testPollableSubscription()
    {
        // given
        final TngpClient client = clientRule.getClient();
        final AsyncTasksClient taskService = client.tasks();

        final PollableTaskSubscription subscription = taskService.newPollableSubscription()
            .lockTime(123L)
            .taskType("foo")
            .taskQueueId(0)
            .open();

        final Long taskId = taskService.create()
            .topicId(0)
            .taskType("foo")
            .execute();

        // when
        final RecordingTaskHandler taskHandler = new RecordingTaskHandler();

        TestUtil.doRepeatedly(() -> subscription.poll(taskHandler))
            .until((workCount) -> workCount == 1);

        assertThat(taskHandler.handledTasks).hasSize(1);
        assertThat(taskHandler.handledTasks.get(0).getKey()).isEqualTo(taskId);
    }

    @Test
    public void shouldOpenSubscriptionAfterClientReconnect()
    {
        // given
        final TngpClient client = clientRule.getClient();

        client.tasks().create()
            .topicId(0)
            .taskType("foo")
            .execute();

        // when
        client.disconnect();
        client.connect();

        // then
        final RecordingTaskHandler taskHandler = new RecordingTaskHandler();

        client.tasks().newSubscription()
                .topicId(0)
                .taskType("foo")
                .lockTime(Duration.ofMinutes(5))
                .lockOwner(1)
                .handler(taskHandler)
                .open();

        waitUntil(() -> !taskHandler.getHandledTasks().isEmpty());

        assertThat(taskHandler.getHandledTasks()).hasSize(1);
    }

    @Test
    public void shouldGiveTaskToSingleSubscription()
    {
        // given
        final TngpClient client = clientRule.getClient();

        final RecordingTaskHandler taskHandler = new RecordingTaskHandler(Task::complete);

        client.tasks().newSubscription()
            .topicId(0)
            .taskType("foo")
            .lockTime(Duration.ofHours(1))
            .lockOwner(1)
            .handler(taskHandler)
            .open();

        client.tasks().newSubscription()
            .topicId(0)
            .taskType("foo")
            .lockTime(Duration.ofHours(2))
            .lockOwner(1)
            .handler(taskHandler)
            .open();

        // when
        client
            .tasks()
            .create()
            .topicId(0)
            .taskType("foo")
            .execute();

        waitUntil(() -> taskHandler.handledTasks.size() == 1);

        // then
        assertThat(taskHandler.handledTasks).hasSize(1);
    }

    public static class RecordingTaskHandler implements TaskHandler
    {
        protected List<Task> handledTasks = Collections.synchronizedList(new ArrayList<>());
        protected int nextTaskHandler = 0;
        protected final TaskHandler[] taskHandlers;

        public RecordingTaskHandler()
        {
            this(task ->
            {
                // do nothing
            });
        }

        public RecordingTaskHandler(TaskHandler... taskHandlers)
        {
            this.taskHandlers = taskHandlers;
        }

        @Override
        public void handle(Task task)
        {
            final TaskHandler handler = taskHandlers[nextTaskHandler];
            nextTaskHandler = Math.min(nextTaskHandler + 1, taskHandlers.length - 1);

            try
            {
                handler.handle(task);
            }
            finally
            {
                handledTasks.add(task);
            }
        }

        public List<Task> getHandledTasks()
        {
            return handledTasks;
        }

        public void clear()
        {
            handledTasks.clear();
        }
    }

}