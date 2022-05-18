package io.palyvos.scheduler.adapters.storm;

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
  INPUT_KAFKA_QUEUE_SIZE("groupByNode(Storm.*.%s.*.*.*.receive.population.value, %d, 'avg')","groupByNode(Storm.*.%s.*.*.*.kafka-queue-size.*, %d, 'avg')"),
  SUBTASK_TUPLES_IN_RECENT("groupByNode(Storm.*.%s.*.*.*.execute-count.*.value, %d, 'avg')"),
  SUBTASK_TUPLES_OUT_RECENT("groupByNode(Storm.*.%s.*.*.*.transfer-count.*.value, %d, 'avg')");

  private final String[] graphiteQuery;
  private final int operatorBaseIndex = 3;

  StormGraphiteMetric(String graphiteQuery) {
    this.graphiteQuery = new String[1];
    //query format: Storm.jobName.[hostname-part]+.worker.node.instance...
    this.graphiteQuery[0] = formatGraphiteQuery(graphiteQuery);
  }

  StormGraphiteMetric(String graphiteQuery1, String graphiteQuery2) {
    this.graphiteQuery = new String[2];
    this.graphiteQuery[0] = formatGraphiteQuery(graphiteQuery1);
    this.graphiteQuery[1] = formatGraphiteQuery(graphiteQuery2);
  }

  StormGraphiteMetric(String graphiteQuery1, String graphiteQuery2, String graphiteQuery3) {
    this.graphiteQuery = new String[3];
    this.graphiteQuery[0] = formatGraphiteQuery(graphiteQuery1);
    this.graphiteQuery[1] = formatGraphiteQuery(graphiteQuery2);
    this.graphiteQuery[2] = formatGraphiteQuery(graphiteQuery3);
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

  public void compute(StormGraphiteMetricProvider stormGraphiteMetricProvider) {
    //FIXME: Adjust default window size and default value depending on metric
    Map<String, Double> metricValues = new HashMap<>();

    for(int i=0; i<graphiteQuery.length; i++){
      String query = graphiteQuery[i];
      String nameExtention = query.contains("sendqueue.population") ? "-helper" : "";
      int tuplesBatchSize = !query.contains("receive.population") ? 1 : 100;
      Map<String, Double> mapAux = retrieveMetricValuesGraphite(stormGraphiteMetricProvider, query, nameExtention, tuplesBatchSize);

      if (query.contains("kafka")) {
        for(String key : mapAux.keySet()){
          System.out.println(key+" "+mapAux.get(key));
        }
      }


      if(query.contains("external-queue-size")){
        for(String key : mapAux.keySet()){
          metricValues.put("spout", mapAux.get(key));
        }
      }
      else{
        mapAux.keySet().forEach(k -> metricValues.put(k, mapAux.get(k)));
      }
    }

    if(graphiteQuery.length == 1){ //Adaption to have executor and thread with the same value if the metric is only one
      Map<String, Double> aux = new HashMap<>();
      metricValues.keySet().stream().forEach(k -> aux.put(k.concat("-helper"), metricValues.get(k)));
      aux.keySet().stream().forEach(k -> metricValues.put(k, aux.get(k)));
    }

//    for(String key : metricValues.keySet()){
//      System.out.println(key+" "+metricValues.get(key));
//    }

    stormGraphiteMetricProvider.replaceMetricValues(this, metricValues);
  }

  private Map<String, Double> retrieveMetricValuesGraphite(StormGraphiteMetricProvider stormGraphiteMetricProvider, String graphiteQuery, String nameExtention, int tuplesBatchSize){

    return stormGraphiteMetricProvider
            .fetchFromGraphite(graphiteQuery, SchedulerContext.METRIC_RECENT_PERIOD_SECONDS,
                    report -> report.average(0), nameExtention, tuplesBatchSize);
  }
}


//package io.palyvos.scheduler.adapters.storm;
//
//        import io.palyvos.scheduler.metric.Metric;
//        import io.palyvos.scheduler.util.SchedulerContext;
//        import java.net.InetAddress;
//        import java.net.UnknownHostException;
//        import java.util.Map;
//
//public enum StormGraphiteMetric implements Metric<StormGraphiteMetric> {
//  TASK_QUEUE_SIZE_FROM_SUBTASK_DATA(
//          "groupByNode(Storm.*.%s.*.*.*.receive.population.value, %d, 'avg')"),
//  TASK_OUTPUT_QUEUE_SIZE_FROM_SUBTASK_DATA("groupByNode(Storm.*.%s.*.*.*.sendqueue.population.value, %d, 'avg')"),
//  SUBTASK_TUPLES_IN_RECENT("groupByNode(Storm.*.%s.*.*.*.execute-count.*.value, %d, 'avg')"),
//  SUBTASK_TUPLES_OUT_RECENT("groupByNode(Storm.*.%s.*.*.*.transfer-count.*.value, %d, 'avg')");
//
//  private final String graphiteQuery;
//  private final int operatorBaseIndex = 3;
//
//  StormGraphiteMetric(String graphiteQuery) {
//    //query format: Storm.jobName.[hostname-part]+.worker.node.instance...
//    try {
//      String localHostname = InetAddress.getLocalHost().getCanonicalHostName();
//      int hostnamePartsNumber = localHostname.split("\\.").length;
//      // Keep only tasks that are running in this host
//      this.graphiteQuery = String
//              .format(graphiteQuery, localHostname, operatorBaseIndex + hostnamePartsNumber);
//    } catch (UnknownHostException e) {
//      throw new IllegalStateException(
//              String.format("Hostname not defined correctly in this machine: %s", e.getMessage()));
//    }
//  }
//
//  public void compute(StormGraphiteMetricProvider stormGraphiteMetricProvider) {
//    //FIXME: Adjust default window size and default value depending on metric
//    Map<String, Double> metricValues = stormGraphiteMetricProvider
//            .fetchFromGraphite(graphiteQuery, SchedulerContext.METRIC_RECENT_PERIOD_SECONDS,
//                    report -> report.average(0));
//    stormGraphiteMetricProvider.replaceMetricValues(this, metricValues);
//  }
//}
