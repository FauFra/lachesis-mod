package io.palyvos.scheduler.metric;

public class ConfigMetric {
    private final String metricName;
    private final int batchSize;
    private final String metricSuffix;

    public ConfigMetric(String metricName, int batchSize, String metricSuffix){
        this.metricName = metricName;
        this.batchSize = batchSize;
        this.metricSuffix = metricSuffix;
    }

    public String getMetricName() {
        return metricName;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public String getMetricSuffix() {
        return metricSuffix;
    }
}
