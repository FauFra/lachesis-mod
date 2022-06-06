package io.palyvos.scheduler.adapters.flink;

import io.palyvos.scheduler.metric.ConfigMetric;
import io.palyvos.scheduler.metric.Metric;
import io.palyvos.scheduler.metric.graphite.GraphiteMetricReport;
import io.palyvos.scheduler.task.Operator;
import io.palyvos.scheduler.task.Task;
import io.palyvos.scheduler.util.SchedulerContext;
import java.net.Inet4Address;
import java.net.UnknownHostException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

public enum FlinkGraphiteMetric implements Metric<FlinkGraphiteMetric> {
  SUBTASK_TUPLES_IN_RECENT(
          "groupByNode(%s.taskmanager.*.*.*.*.numRecordsInPerSecond.m1_rate, 4, 'avg')",
          SchedulerContext.METRIC_RECENT_PERIOD_SECONDS, Task::headOperators,
          report -> report.reduce(0, (a, b) -> b)),
  SUBTASK_TUPLES_OUT_RECENT(
          "groupByNode(%s.taskmanager.*.*.*.*.numRecordsOutPerSecond.m1_rate, 4, 'avg')",
          SchedulerContext.METRIC_RECENT_PERIOD_SECONDS, Task::tailOperators,
          report -> report.reduce(0, (a, b) -> b)),
  TASK_QUEUE_SIZE_FROM_SUBTASK_DATA(
          "groupByNode(%s.taskmanager.*.*.*.*.Shuffle.Netty.Input.Buffers.inputQueueLength, 4, 'avg')",
          SchedulerContext.METRIC_RECENT_PERIOD_SECONDS, Task::headOperators,
          report -> report.average(0)),
  TASK_OUTPUT_QUEUE_SIZE_FROM_SUBTASK_DATA(
          "groupByNode(%s.taskmanager.*.*.*.*.Shuffle.Netty.Output.Buffers.outputQueueLength, 4, 'avg')",
          SchedulerContext.METRIC_RECENT_PERIOD_SECONDS, Task::tailOperators,
          report -> report.average(0)),
  INPUT_OUTPUT_QUEUE_SIZE("groupByNode(%s.taskmanager.*.*.*.*.Shuffle.Netty.Input.Buffers.inputQueueLength, 4, 'avg')",
          "groupByNode(%s.taskmanager.*.*.*.*.Shuffle.Netty.Output.Buffers.outputQueueLength, 4, 'avg')",
                          SchedulerContext.METRIC_RECENT_PERIOD_SECONDS, Task::headOperators,
                          report -> report.average(0));

  protected final String[] graphiteQueries;

  private final Map<String, ConfigMetric> configMetrics;
  protected final int window;
  protected final Function<Task, Collection<Operator>> operatorFunction;
  // NOTE: Flink reports tasks metrics directly, so reduceFunction is ignored for now!
  protected final Function<GraphiteMetricReport, Double> reportReduceFunction;

  FlinkGraphiteMetric(String graphiteQuery, int window,
                      Function<Task, Collection<Operator>> operatorFunction,
                      Function<GraphiteMetricReport, Double> reportReduceFunction) {
    this.graphiteQueries = new String[1];
    this.graphiteQueries[0]=onlyLocalTasks(graphiteQuery);
    this.configMetrics = populateConfigMetrics(this.graphiteQueries);
    this.window = window;
    this.operatorFunction = operatorFunction;
    this.reportReduceFunction = reportReduceFunction;
  }

  FlinkGraphiteMetric(String graphiteQuery1, String graphiteQuery2, int window,
                      Function<Task, Collection<Operator>> operatorFunction,
                      Function<GraphiteMetricReport, Double> reportReduceFunction) {
    this.graphiteQueries = new String[2];
    this.graphiteQueries[0] = onlyLocalTasks(graphiteQuery1);
    this.graphiteQueries[1] = onlyLocalTasks(graphiteQuery2);
    this.configMetrics = populateConfigMetrics(this.graphiteQueries);
    this.window = window;
    this.operatorFunction = operatorFunction;
    this.reportReduceFunction = reportReduceFunction;
  }

  private Map<String, ConfigMetric> populateConfigMetrics(String[] graphiteQueries) {
    Map<String, ConfigMetric> aux =  new HashMap<>();

    for(String query: graphiteQueries){
      ConfigMetric configMetric;
      if(query.contains("outputQueueLength")){
        configMetric = new ConfigMetric(query, 1, "-helper");
      }
      else {
        configMetric = new ConfigMetric(query, 1, "");
      }
      String metricName = getMetricName(query);
      aux.put(metricName, configMetric);
    }

    return aux;
  }

  private String getMetricName(String query){
    String allMetric = (query.split(","))[0];
    String[] metricNameArray = allMetric.split("\\.");

    return metricNameArray[metricNameArray.length-1];


  }

  private String onlyLocalTasks(String query) {
    // First part of the metric is the IP, we only want the metrics of tasks in this machine
    try {
      // Replace dots with dashes in graphite, to match the flink key format
      final String localIp = Inet4Address.getLocalHost().getHostAddress().replace(".", "-");
      return String.format(query, localIp);
    } catch (UnknownHostException e) {
      throw new IllegalStateException(String.format("Failed to retrieve local IP: %s", e));
    }
  }

  public void compute(FlinkGraphiteMetricProvider provider) {
    Map<String, Double> operatorMetricValues = provider
            .fetchFromGraphite(this.graphiteQueries, window, reportReduceFunction, this.configMetrics);
    Map<String, Double> taskMetricValues = new HashMap<>();
    provider.traverser.forEachTaskFromSourceBFS(task -> {
      taskMetricValues.put(task.id(), operatorMetricValues.get(task.id().replace(" ", "-")));
      taskMetricValues.put(task.id().concat("-helper"), operatorMetricValues.get(task.id().concat("-helper").replace(" ", "-")));
    });

    if(graphiteQueries.length == 1 && !graphiteQueries[0].contains("external-queue")){ //Adaption to have executor and thread with the same value if the metric is only one
      Map<String, Double> aux = new HashMap<>();
      taskMetricValues.keySet().stream().forEach(k -> aux.put(k.concat("-helper"), taskMetricValues.get(k)));
      aux.keySet().stream().forEach(k -> taskMetricValues.put(k, aux.get(k)));
    }

//    for(String op : taskMetricValues.keySet()){
//      System.out.println(op+" "+taskMetricValues.get(op));
//    }

    provider.replaceMetricValues(this, taskMetricValues);
  }
}


//package io.palyvos.scheduler.adapters.flink;
//
//import io.palyvos.scheduler.metric.Metric;
//import io.palyvos.scheduler.metric.graphite.GraphiteMetricReport;
//import io.palyvos.scheduler.task.Operator;
//import io.palyvos.scheduler.task.Task;
//import io.palyvos.scheduler.util.SchedulerContext;
//import java.net.Inet4Address;
//import java.net.UnknownHostException;
//import java.util.Collection;
//import java.util.HashMap;
//import java.util.Map;
//import java.util.function.Function;
//
//public enum FlinkGraphiteMetric implements Metric<FlinkGraphiteMetric> {
//  SUBTASK_TUPLES_IN_RECENT(
//      "groupByNode(%s.taskmanager.*.*.*.*.numRecordsInPerSecond.m1_rate, 4, 'avg')",
//      SchedulerContext.METRIC_RECENT_PERIOD_SECONDS, Task::headOperators,
//      report -> report.reduce(0, (a, b) -> b)),
//  SUBTASK_TUPLES_OUT_RECENT(
//      "groupByNode(%s.taskmanager.*.*.*.*.numRecordsOutPerSecond.m1_rate, 4, 'avg')",
//      SchedulerContext.METRIC_RECENT_PERIOD_SECONDS, Task::tailOperators,
//      report -> report.reduce(0, (a, b) -> b)),
//  TASK_QUEUE_SIZE_FROM_SUBTASK_DATA(
//      "groupByNode(%s.taskmanager.*.*.*.*.Shuffle.Netty.Input.Buffers.inputQueueLength, 4, 'avg')",
//      SchedulerContext.METRIC_RECENT_PERIOD_SECONDS, Task::headOperators,
//      report -> report.average(0)),
//  TASK_OUTPUT_QUEUE_SIZE_FROM_SUBTASK_DATA(
//      "groupByNode(%s.taskmanager.*.*.*.*.Shuffle.Netty.Output.Buffers.outputQueueLength, 4, 'avg')",
//      SchedulerContext.METRIC_RECENT_PERIOD_SECONDS, Task::tailOperators,
//      report -> report.average(0));
//
//  protected final String[] graphiteQueries;
//  protected final String graphiteQuery;
//  protected final int window;
//  protected final Function<Task, Collection<Operator>> operatorFunction;
//  // NOTE: Flink reports tasks metrics directly, so reduceFunction is ignored for now!
//  protected final Function<GraphiteMetricReport, Double> reportReduceFunction;
//
//  FlinkGraphiteMetric(String graphiteQuery, int window,
//      Function<Task, Collection<Operator>> operatorFunction,
//      Function<GraphiteMetricReport, Double> reportReduceFunction) {
//    this.graphiteQuery = onlyLocalTasks(graphiteQuery);
//    this.window = window;
//    this.operatorFunction = operatorFunction;
//    this.reportReduceFunction = reportReduceFunction;
//  }
//
//  private String onlyLocalTasks(String query) {
//    // First part of the metric is the IP, we only want the metrics of tasks in this machine
//    try {
//      // Replace dots with dashes in graphite, to match the flink key format
//      final String localIp = Inet4Address.getLocalHost().getHostAddress().replace(".", "-");
//      return String.format(query, localIp);
//    } catch (UnknownHostException e) {
//      throw new IllegalStateException(String.format("Failed to retrieve local IP: %s", e));
//    }
//  }
//
//  public void compute(FlinkGraphiteMetricProvider provider) {
//    Map<String, Double> operatorMetricValues = provider
//        .fetchFromGraphite(graphiteQuery, window, reportReduceFunction);
//    Map<String, Double> taskMetricValues = new HashMap<>();
//    provider.traverser.forEachTaskFromSourceBFS(task -> {
//      taskMetricValues.put(task.id(), operatorMetricValues.get(task.id().replace(" ", "-")));
//    });
//    provider.replaceMetricValues(this, taskMetricValues);
//  }
//}
