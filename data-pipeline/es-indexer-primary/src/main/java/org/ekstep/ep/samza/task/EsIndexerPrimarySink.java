package org.ekstep.ep.samza.task;

import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemStream;
import org.apache.samza.task.MessageCollector;
import org.ekstep.ep.samza.domain.Event;
import org.ekstep.ep.samza.metrics.JobMetrics;

public class EsIndexerPrimarySink {

    private MessageCollector collector;
    private JobMetrics metrics;
    private EsIndexerPrimaryConfig config;

    public EsIndexerPrimarySink(MessageCollector collector, JobMetrics metrics, EsIndexerPrimaryConfig config) {
        this.collector = collector;
        this.metrics = metrics;
        this.config = config;
    }

    public void toFailedTopic(Event event) {
        collector.send(new OutgoingMessageEnvelope(
                new SystemStream("kafka", config.failedTopic()), event.getMap()));
        metrics.incFailedCounter();
    }

    public void toErrorTopic(Event event) {
        collector.send(new OutgoingMessageEnvelope(
                new SystemStream("kafka", config.failedTopic()), event.getMap()));
        metrics.incErrorCounter();
    }

    public void markSuccess() {
        metrics.incSuccessCounter();
    }
}
