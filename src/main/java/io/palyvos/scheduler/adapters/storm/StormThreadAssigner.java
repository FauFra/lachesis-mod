package io.palyvos.scheduler.adapters.storm;

import io.palyvos.scheduler.task.ExternalThread;
import io.palyvos.scheduler.task.Subtask;
import io.palyvos.scheduler.task.Task;
import java.util.ArrayDeque;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import java.util.regex.Matcher;
import org.apache.commons.lang3.Validate;

class StormThreadAssigner {

  public static void assign(Collection<Task> tasks, Collection<ExternalThread> threads) {
    final Map<String, Task> taskIndex = new HashMap<>();
    for (ExternalThread thread : threads) {
      Matcher matcher = StormConstants.EXECUTOR_THREAD_PATTERN.matcher(thread.name());
      if (matcher.matches()) {
        final String taskName = matcher.group(1);
        if (taskName.contains(StormConstants.ACKER_NAME) || taskName.contains(
            StormConstants.METRIC_REPORTER_NAME)) {
          continue;
        }
        //FIXME: Extract JobID? Maybe worker ID?
        //FIXME: Parse and handle parallel instance names/indexes
        Task task = taskIndex.computeIfAbsent(taskName, k -> new Task(taskName, taskName, "DEFAULT"));
        Subtask subtask = new Subtask(taskName, taskName, task.subtasks().size());
        subtask.assignThread(thread);
        task.subtasks().add(subtask);
      }
    }
    tasks.addAll(taskIndex.values());
  }

  private StormThreadAssigner() {
  }
}
