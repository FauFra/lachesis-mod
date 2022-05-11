package io.palyvos.scheduler.metric.graphite;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import java.io.IOException;
import java.net.InetAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import org.apache.commons.lang3.Validate;
import org.apache.http.client.fluent.Request;
import org.apache.http.client.utils.URIBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class GraphiteDataFetcher {

  private static final Logger LOG = LogManager.getLogger();
  private final URI graphiteURI;
  private final Gson gson = new GsonBuilder().create();
  private int countMax;
  private int countZero;

  public GraphiteDataFetcher(String graphiteHost, int graphitePort) {
    this.graphiteURI = URI
        .create(String.format("http://%s:%d", graphiteHost, graphitePort));
    this.countMax=0;
    this.countZero=0;
  }

  public Map<String, Double> fetchFromGraphite(String target,
      int windowSeconds, Function<GraphiteMetricReport, Double> reduceFunction) {
    GraphiteMetricReport[] reports = rawFetchFromGraphite(target, windowSeconds);
    Map<String, Double> result = new HashMap<String, Double>();
    for (GraphiteMetricReport report : reports) {
      Double reportValue = reduceFunction.apply(report);
      if (reportValue != null) {
        //Null values can exist due to leftovers in graphite data
        String report_name_helper = report.name().concat("-helper");
        result.put(report.name(), reportValue * 100);
        result.put(report_name_helper, reportValue * 100);
      }
    }

//    double avg = result.entrySet().stream().filter(entry -> !entry.getKey().equals("source")).mapToDouble(Map.Entry::getValue).average().getAsDouble();
//    result.put("source",avg);

    //TODO inserisco l'external queue size
//    getExternalQueueSize(windowSeconds, result, reduceFunction);
    getExternalQueueKafka(windowSeconds,result,reduceFunction);

    //TODO inserisco l'output queue size
    getOutputQueueSize(windowSeconds,result,reduceFunction);

    return result;
  }

  GraphiteMetricReport[] rawFetchFromGraphite(String target, int windowSeconds) {
    Validate.notEmpty(target, "empty target");
    URIBuilder builder = new URIBuilder(graphiteURI);
    builder.setPath("render");
    builder.addParameter("target", target);
    builder.addParameter("from", String.format("-%dsec", windowSeconds));
    builder.addParameter("format", "json");
    try {
      URI uri = builder.build();
      LOG.trace("Fetching {}", uri);
      String response = Request.Get(uri).execute().returnContent().toString();
      GraphiteMetricReport[] reports = gson.fromJson(response, GraphiteMetricReport[].class);
      return reports;
    } catch (IOException | URISyntaxException e) {
      throw new RuntimeException(e);
    }
  }

  private void getExternalQueueSize(int windowSeconds, Map<String, Double> result, Function<GraphiteMetricReport, Double> reduceFunction){
    String request = "groupByNode(Storm.*.%s.*.*.*.external-queue-size.*, %d, 'avg')";
    request = formatRequest(request);
    GraphiteMetricReport[] reports1 = rawFetchFromGraphite(request, windowSeconds);
    for(GraphiteMetricReport report : reports1){
      Double reportValue = reduceFunction.apply(report);
      result.put("spout", reportValue);
    }
  }


  private void getOutputQueueSize(int windowSeconds, Map<String, Double> result, Function<GraphiteMetricReport, Double> reduceFunction) {
    String request = "groupByNode(Storm.*.%s.*.*.*.sendqueue.population.value, %d, 'avg')";
    request = formatRequest(request);
    GraphiteMetricReport[] reports1 = rawFetchFromGraphite(request, windowSeconds);
    for(GraphiteMetricReport report : reports1){
      Double reportValue = reduceFunction.apply(report);
//      System.out.println(report.name()+" "+reportValue);
      String report_name_helper = report.name().concat("-helper");
      result.put(report_name_helper, reportValue);
    }
  }

  private void getExternalQueueKafka(int windowSeconds, Map<String, Double> result, Function<GraphiteMetricReport, Double> reduceFunction){
    String request = "groupByNode(kafka-external-queue.*, %d, 'avg')";
    request = formatRequest(request);
    GraphiteMetricReport[] reports1 = rawFetchFromGraphite(request, windowSeconds);
    for(GraphiteMetricReport report : reports1){
      Double reportValue = reduceFunction.apply(report);
      result.put("source", reportValue);
    }
  }

  private void getExternalQueueKafka(int windowSeconds, Map<String, Double> result) {
    String requestStorm = "Storm.*.*.*.*.*.last-offset-tuple.*";
//    String requestStorm = "Storm.*.*.*.source.*.emit-count.default.value";
    String requestKafka = "kafka.tuple.last-offset.value";
//    request = formatRequest(request);
    GraphiteMetricReport[] reports1 = rawFetchFromGraphite(requestStorm, 3600);
    GraphiteMetricReport[] reports2 = rawFetchFromGraphite(requestKafka, windowSeconds);



//    Double stormReport = reduceFunction.apply(reports1[0]);
//    Double kafkaReport = reduceFunction.apply(reports2[0]);
//    Double stormReport = reports1[0].sum();
    Double stormReport = reports1[0].lastNotNull();
    Double kafkaReport = reports2[0].lastNotNull();
    Double reportValue = kafkaReport-stormReport;
//    System.out.println(reportValue);

//    //Make sure that the value doesn't exceed the boundaries
    reportValue = reportValue < 0 ? 0 : reportValue;
    reportValue = reportValue > 102400 ? 102400 :  reportValue;

    result.put("source",reportValue);
  }

  private String formatRequest(String graphiteQuery){
    int operatorBaseIndex = 3;
    try {
      String localHostname = InetAddress.getLocalHost().getCanonicalHostName();
      int hostnamePartsNumber = localHostname.split("\\.").length;
      // Keep only tasks that are running in this host
      graphiteQuery = String
              .format(graphiteQuery, localHostname, operatorBaseIndex + hostnamePartsNumber);
    } catch (UnknownHostException e) {
      throw new IllegalStateException(
              String.format("Hostname not defined correctly in this machine: %s", e.getMessage()));
    }

    return graphiteQuery;
  }



}