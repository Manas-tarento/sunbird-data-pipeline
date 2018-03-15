package org.ekstep.ep.samza.task;

import org.apache.samza.config.Config;

public class EsIndexerSecondaryConfig {

    private final String failedTopic;
    private final String metricsTopic;
    private final String elasticSearchHosts;
    private final String elasticSearchPort;
    private String jobName;

    public EsIndexerSecondaryConfig(Config config) {
        failedTopic = config.get("output.failed.topic.name", "telemetry.es-indexer-secondary.fail");
        metricsTopic = config.get("output.metrics.topic.name", "pipeline_metrics");
        elasticSearchHosts = config.get("hosts.elastic_search","localhost");
        elasticSearchPort = config.get("port.elastic_search","9200");
        jobName = config.get("output.metrics.job.name", "EsIndexerPrimary");
    }

    public String failedTopic() {
        return failedTopic;
    }

    public String metricsTopic() {
        return metricsTopic;
    }

    public String[] esHosts() {
        String[] hosts = elasticSearchHosts.split(",");
        return hosts;
    }

    public int esPort() {
        return Integer.parseInt(elasticSearchPort);
    }

    public String jobName() {
        return jobName;
    }
}
