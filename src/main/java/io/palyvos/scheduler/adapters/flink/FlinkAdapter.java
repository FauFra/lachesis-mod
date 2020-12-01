package io.palyvos.scheduler.adapters.flink;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import io.palyvos.scheduler.adapters.OsAdapter;
import io.palyvos.scheduler.adapters.SpeAdapter;
import io.palyvos.scheduler.task.ExternalThread;
import io.palyvos.scheduler.task.Operator;
import io.palyvos.scheduler.task.Task;
import io.palyvos.scheduler.task.TaskIndex;
import io.palyvos.scheduler.util.RequestHelper;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.commons.lang3.Validate;
import org.apache.http.client.utils.URIBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class FlinkAdapter implements SpeAdapter {

  private static final Logger LOG = LogManager.getLogger(FlinkAdapter.class);

  private static final String JOBS_PATH = "jobs";
  public static final String JOBS_KEY = "jobs";
  public static final Pattern MULTIPLE_OPERATOR_PATTERN = Pattern.compile("\\s*\\((.+)\\)\\s*");
  private final URI flinkURI;
  private final Gson gson = new Gson();
  private final List<Task> tasks = new ArrayList<>();
  private final OsAdapter osAdapter;
  private final List<Integer> pids;
  private TaskIndex taskIndex;

  public FlinkAdapter(List<Integer> pids, String host, int port, OsAdapter osAdapter) {
    Validate.notBlank(host, "No host provided!");
    Validate.isTrue(port > 0, "Negative port provided!");
    Validate.notNull(osAdapter, "osAdapter");
    Validate.notEmpty(pids, "At least one worker PID required");
    this.flinkURI = URI.create(String.format("http://%s:%d", host, port));
    this.pids = pids;
    this.osAdapter = osAdapter;
  }

  @Override
  public void updateTasks() {
    this.tasks.clear();
    jobs().stream().filter(job -> job.isRunning()).forEach(job ->
        tasks.addAll(fetchTasks(job)));
    FlinkThreadAssigner.assign(tasks, threads());
    tasks.forEach(task -> task.operators().addAll(operators(task)));
    this.taskIndex = new TaskIndex(this.tasks);
  }

  private Collection<Operator> operators(Task task) {
    String[] chainedOperators = task.id().split("->");
    List<String> operatorNames = new ArrayList<>();
    for (String name : chainedOperators) {
      Matcher multipleOperatorMatcher = MULTIPLE_OPERATOR_PATTERN.matcher(name);
      if (multipleOperatorMatcher.matches()) {
        operatorNames.addAll(Arrays.asList(multipleOperatorMatcher.group(1).split(",")));
      } else {
        operatorNames.add(name);
      }
    }
    // Bring operator names in-sync with Flink's graphite reporter
    return operatorNames.stream()
        .map(name -> name.trim().replace(" ", "-"))
        .map(name -> new Operator(name))
        .collect(Collectors.toList());
  }

  /**
   * @return The jobs of the flink cluster.
   */
  public Collection<FlinkJob> jobs() {
    URIBuilder builder = new URIBuilder(flinkURI);
    builder.setPath(JOBS_PATH);
    String response = RequestHelper.getContent(builder);
    Map<String, List<FlinkJob>> jobs = gson
        .fromJson(response, new TypeToken<Map<String, List<FlinkJob>>>() {
        }.getType());
    LOG.debug("FlinkAdapter jobs: {}", jobs);
    return jobs.get(JOBS_KEY);
  }

  @Override
  public Collection<Task> tasks() {
    return Collections.unmodifiableCollection(tasks);
  }

  @Override
  public Collection<ExternalThread> threads() {
    List<ExternalThread> threads = new ArrayList<>();
    for (int pid : pids) {
      threads.addAll(osAdapter.jvmThreads(pid));
    }
    return Collections.unmodifiableList(threads);
  }

  @Override
  public TaskIndex taskIndex() {
    return taskIndex;
  }

  public Collection<Task> fetchTasks(FlinkJob job) {
    Validate.notNull(job, "job");
    return fetchTasks(job.id);
  }

  public Collection<Task> fetchTasks(String jobId) {
    Validate.notBlank(jobId, "Blank job id provided!");
    URIBuilder builder = new URIBuilder(flinkURI);
    builder.setPathSegments(JOBS_PATH, jobId);
    String response = RequestHelper.getContent(builder);
    FlinkJobInfo jobInfo = gson.fromJson(response, FlinkJobInfo.class);
    LOG.debug(jobInfo);
    return jobInfo.plan.tasks(jobInfo.vertices);
  }

}
