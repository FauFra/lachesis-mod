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

import io.palyvos.scheduler.metric.ConfigMetric;
import org.apache.commons.lang3.Validate;
import org.apache.http.client.fluent.Request;
import org.apache.http.client.utils.URIBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class GraphiteDataFetcher {

  private static final Logger LOG = LogManager.getLogger();
  private final URI graphiteURI;
  private final Gson gson = new GsonBuilder().create();

  public GraphiteDataFetcher(String graphiteHost, int graphitePort) {
    this.graphiteURI = URI
        .create(String.format("http://%s:%d", graphiteHost, graphitePort));

  }

  public Map<String, Double> fetchFromGraphite(String[] targets,
                                               int windowSeconds, Function<GraphiteMetricReport, Double> reduceFunction, Map<String,ConfigMetric> configMetrics) {
    GraphiteMetricReport[] reports = rawFetchFromGraphite(targets, windowSeconds);
    Map<String, Double> result = new HashMap<String, Double>();
    for (GraphiteMetricReport report : reports) {
      Double reportValue = reduceFunction.apply(report);
      if (reportValue != null) {
        //Null values can exist due to leftovers in graphite data
        ConfigMetric configMetric = retrieveConfig(configMetrics, report.tagsName());
        String report_name = report.name().concat(configMetric.getMetricSuffix());
        System.out.printf(" Report name: %s\t Batch size: %f\n", report_name, (reportValue * configMetric.getBatchSize()));
        result.put(report_name, reportValue * configMetric.getBatchSize());
      }
    }
    System.out.println();

    return result;
  }

  public Map<String, Double> fetchFromGraphite(String target,
      int windowSeconds, Function<GraphiteMetricReport, Double> reduceFunction, String nameExtension, int tuplesBatchSize) {
    GraphiteMetricReport[] reports = rawFetchFromGraphite(target, windowSeconds);
    Map<String, Double> result = new HashMap<String, Double>();
    for (GraphiteMetricReport report : reports) {
      Double reportValue = reduceFunction.apply(report);
      if (reportValue != null) {
        //Null values can exist due to leftovers in graphite data
        String report_name = report.name().concat(nameExtension);
        result.put(report_name, reportValue * tuplesBatchSize);
      }
    }

    return result;
  }

  public Map<String, Double> fetchFromGraphite(String target,
      int windowSeconds, Function<GraphiteMetricReport, Double> reduceFunction){
    return this.fetchFromGraphite(target, windowSeconds, reduceFunction, "", 1);
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
      System.out.println(uri.toString());
      LOG.trace("Fetching {}", uri);
      String response = Request.Get(uri).execute().returnContent().toString();
      GraphiteMetricReport[] reports = gson.fromJson(response, GraphiteMetricReport[].class);
      return reports;
    } catch (IOException | URISyntaxException e) {
      throw new RuntimeException(e);
    }
  }

  GraphiteMetricReport[] rawFetchFromGraphite(String[] targets, int windowSeconds) {
    URIBuilder builder = createURI(targets, windowSeconds);
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

  URIBuilder createURI(String[] targets, int windowSeconds){
    URIBuilder builder = new URIBuilder(graphiteURI);
    builder.setPath("render");

    for(String target : targets){
      Validate.notEmpty(target, "empty target");
      builder.addParameter("target", target);
    }
    builder.addParameter("from", String.format("-%dsec", windowSeconds));
    builder.addParameter("format", "json");

    return builder;
  }

  /*
   * Returns the configuration associated to tagsName if exists, null otherwise.
   */
  private ConfigMetric retrieveConfig(Map<String,ConfigMetric> configMetrics, String tagsName){

    for(String config : configMetrics.keySet()){
      if(tagsName.contains(config)){
        return  configMetrics.get(config);
      }
    }

    return null;
  }


}