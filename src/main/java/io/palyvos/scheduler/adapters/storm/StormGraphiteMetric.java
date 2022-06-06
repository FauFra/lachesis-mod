package io.palyvos.scheduler.adapters.storm;

import io.palyvos.scheduler.metric.ConfigMetric;
import io.palyvos.scheduler.metric.Metric;
import io.palyvos.scheduler.util.SchedulerContext;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;

public enum StormGraphiteMetric implements Metric<StormGraphiteMetric> {
  TASK_QUEUE_SIZE_FROM_SUBTASK_DATA(
      "groupByNode(Storm.*.%s.*.*.*.receive.population.value, %d, 'avg')"),
  TASK_OUTPUT_QUEUE_SIZE_FROM_SUBTASK_DATA("groupByNode(Storm.*.%s.*.*.*.sendqueue.population.value, %d, 'avg')"),
  INPUT_OUTPUT_QUEUE_SIZE("groupByNode(Storm.*.%s.*.*.*.receive.population.value, %d, 'avg')","groupByNode(Storm.*.%s.*.*.*.sendqueue.population.value, %d, 'avg')"),
  INPUT_OUTPUT_EXTERNAL_QUEUE_SIZE("groupByNode(Storm.*.%s.*.*.*.receive.population.value, %d, 'avg')","groupByNode(Storm.*.%s.*.*.*.sendqueue.population.value, %d, 'avg')","groupByNode(Storm.*.%s.*.*.*.external-queue-size.*, %d, 'avg')"),
  INPUT_OUTPUT_KAFKA_QUEUE_SIZE("groupByNode(Storm.*.%s.*.*.*.receive.population.value, %d, 'avg')","groupByNode(Storm.*.%s.*.*.*.sendqueue.population.value, %d, 'avg')","groupByNode(Storm.*.%s.*.*.*.kafka-queue-size.*, %d, 'avg')"),
  INPUT_EXTERNAL_QUEUE_SIZE("groupByNode(Storm.*.%s.*.*.*.receive.population.value, %d, 'avg')","groupByNode(Storm.*.%s.*.*.*.external-queue-size.*, %d, 'avg')"),
  INPUT_KAFKA_QUEUE_SIZE("groupByNode(Storm.*.%s.*.*.*.receive.population.value, %d, 'avg')","groupByNode(Storm.*.%s.*.*.*.kafka-queue-size.*, %d, 'avg')"),
  SUBTASK_TUPLES_IN_RECENT("groupByNode(Storm.*.%s.*.*.*.execute-count.*.value, %d, 'avg')"),
  SUBTASK_TUPLES_OUT_RECENT("groupByNode(Storm.*.%s.*.*.*.transfer-count.*.value, %d, 'avg')");

  private final String[] graphiteQueries;

  private final Map<String, ConfigMetric> configMetrics;
  private final int operatorBaseIndex = 3;

  StormGraphiteMetric(String graphiteQueries) {
    this.graphiteQueries = new String[1];
    //query format: Storm.jobName.[hostname-part]+.worker.node.instance...
    this.graphiteQueries[0] = formatGraphiteQuery(graphiteQueries);
    this.configMetrics = populateConfigMetrics(this.graphiteQueries);
  }

  StormGraphiteMetric(String graphiteQuery1, String graphiteQuery2) {
    this.graphiteQueries = new String[2];
    this.graphiteQueries[0] = formatGraphiteQuery(graphiteQuery1);
    this.graphiteQueries[1] = formatGraphiteQuery(graphiteQuery2);
    this.configMetrics = populateConfigMetrics(this.graphiteQueries);
  }

  StormGraphiteMetric(String graphiteQuery1, String graphiteQuery2, String graphiteQuery3) {
    this.graphiteQueries = new String[3];
    this.graphiteQueries[0] = formatGraphiteQuery(graphiteQuery1);
    this.graphiteQueries[1] = formatGraphiteQuery(graphiteQuery2);
    this.graphiteQueries[2] = formatGraphiteQuery(graphiteQuery3);
    this.configMetrics = populateConfigMetrics(this.graphiteQueries);
  }

  private String formatGraphiteQuery(String graphiteQuery){
    try {
      String localHostname = InetAddress.getLocalHost().getCanonicalHostName();
      int hostnamePartsNumber = localHostname.split("\\.").length;
      // Keep only tasks that are running in this host
      return String.format(graphiteQuery, localHostname, operatorBaseIndex + hostnamePartsNumber);
    } catch (UnknownHostException e) {
      throw new IllegalStateException(
              String.format("Hostname not defined correctly in this machine: %s", e.getMessage()));
    }
  }

  private Map<String, ConfigMetric> populateConfigMetrics(String[] graphiteQueries) {
    Map<String, ConfigMetric> aux =  new HashMap<>();

    for(String query: graphiteQueries){
      ConfigMetric configMetric;
      if(query.contains("receive.population")){
        configMetric = new ConfigMetric(query, 100, "");
      }
      else if(query.contains("sendqueue.population")){
        configMetric = new ConfigMetric(query, 1, "-helper");
      }
      else {
        configMetric = new ConfigMetric(query, 1, "");
      }
      String[] metricNameArray = query.split("\\.");
      StringBuilder metricName = new StringBuilder();

      if(! metricNameArray[metricNameArray.length-3].equals("*")) {
        metricName.append(metricNameArray[metricNameArray.length - 3]).append("."); //if is not a number i put it in the name
      }

      metricName.append(metricNameArray[metricNameArray.length-2]);
      System.out.println("Metric name: "+metricName.toString());
      aux.put(metricName.toString(), configMetric);
    }

    return aux;
  }

  public void compute(StormGraphiteMetricProvider stormGraphiteMetricProvider) {
    //FIXME: Adjust default window size and default value depending on metric
    Map<String, Double> metricValues = retrieveMetricValuesGraphite(stormGraphiteMetricProvider, this.graphiteQueries, this.configMetrics);

    if(graphiteQueries.length == 1 && !graphiteQueries[0].contains("external-queue")){ //Adaption to have executor and thread with the same value if the metric is only one
      Map<String, Double> aux = new HashMap<>();
      metricValues.keySet().stream().forEach(k -> aux.put(k.concat("-helper"), metricValues.get(k)));
      aux.keySet().stream().forEach(k -> metricValues.put(k, aux.get(k)));
    }

    stormGraphiteMetricProvider.replaceMetricValues(this, metricValues);
  }

  private Map<String, Double> retrieveMetricValuesGraphite(StormGraphiteMetricProvider stormGraphiteMetricProvider, String[] graphiteQueries, Map<String, ConfigMetric> configMetrics) {
    return stormGraphiteMetricProvider
            .fetchFromGraphite(graphiteQueries, SchedulerContext.METRIC_RECENT_PERIOD_SECONDS,
                    report -> report.average(0), configMetrics);

  }
  private Map<String, Double> retrieveMetricValuesGraphite(StormGraphiteMetricProvider stormGraphiteMetricProvider, String graphiteQuery, String nameExtention, int tuplesBatchSize){
    return stormGraphiteMetricProvider
            .fetchFromGraphite(graphiteQuery, SchedulerContext.METRIC_RECENT_PERIOD_SECONDS,
                    report -> report.average(0), nameExtention, tuplesBatchSize);
  }
}


