/*
 * Copyright 2026 Kamran Zafar
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.kamranzafar.alfresco.event2.kafka;

import org.alfresco.repo.rawevents.AbstractEventProducer;
import org.apache.camel.Exchange;
import org.apache.camel.LoggingLevel;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.kafka.KafkaConstants;
import org.apache.camel.model.RouteDefinition;
import org.apache.camel.util.URISupport;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Fans out repo event2 messages to each enabled broker target.
 * <p>
 * {@link KeyedEvent2MessageProducer} sends to {@code repo.event2.producer.endpoint} (a synchronous {@code direct:} endpoint), and this route multicasts the message, in order, to:
 * <ol>
 * <li>the ActiveMQ topic ({@code repo.event2.route.activemq.*}), and</li>
 * <li>the Kafka topic ({@code repo.event2.route.kafka.*}).</li>
 * </ol>
 * The Kafka endpoint is {@code repo.event2.route.kafka.endpoint} when that is set. Otherwise it is built from {@code repo.event2.route.kafka.topic}, the individual {@code repo.event2.route.kafka.<option>} properties (each one a Camel Kafka endpoint option; blank ones are left out) and {@code repo.event2.route.kafka.options}.
 * <p>
 * Each target is switched on or off independently, and a disabled target has no route at all. Every enabled target is always attempted: a failure on one never stops delivery to the other. Once all targets have been tried, a failure on the ActiveMQ target is propagated to the sender (as without this module, which logs it). A failure on the Kafka target is logged and swallowed unless {@code repo.event2.route.kafka.failOnError=true}, in which case it is propagated as well.
 * <p>
 * Declared in this module's Messaging subsystem extension context, where the Camel context's {@code <contextScan/>} picks it up.
 *
 * @author Kamran Zafar
 */
public class Event2FanOutRouteBuilder extends RouteBuilder {
    private static final Log LOGGER = LogFactory.getLog(Event2FanOutRouteBuilder.class);
    private static final String LOG_CATEGORY = Event2FanOutRouteBuilder.class.getName();

    public static final String FAN_OUT_ROUTE_ID = "alfresco.event2 -> fan-out";
    public static final String ACTIVEMQ_ROUTE_ID = "alfresco.event2 -> activemq";
    public static final String KAFKA_ROUTE_ID = "alfresco.event2 -> kafka";

    static final String ACTIVEMQ_TARGET = "direct:alfresco.repo.event2.fanout.activemq";
    static final String KAFKA_TARGET = "direct:alfresco.repo.event2.fanout.kafka";

    /** Option values made only of these characters go into the URI as they are; anything else is wrapped in RAW(). */
    private static final Pattern PLAIN_OPTION_VALUE = Pattern.compile("[A-Za-z0-9._:,/-]*");

    private String sourceEndpoint;
    private boolean activeMqEnabled = true;
    private String activeMqEndpoint;
    private boolean kafkaEnabled;
    private String kafkaEndpoint;
    private String kafkaTopic;
    private Map<String, String> kafkaOptions = new LinkedHashMap<>();
    private String kafkaExtraOptions;
    private boolean kafkaFailOnError;

    @Override
    public void configure() {
        if (sourceEndpoint == null || !sourceEndpoint.startsWith("direct:")) {
            // The producer is pointed straight at a broker (stock Alfresco behaviour), so there is nothing to consume here.
            LOGGER.info("Event2 fan-out disabled: repo.event2.producer.endpoint is not a direct endpoint (" + sourceEndpoint + ")");
            return;
        }

        List<String> targets = new ArrayList<>();
        if (activeMqEnabled) {
            configureActiveMqRoute();
            targets.add(ACTIVEMQ_TARGET);
        }
        if (kafkaEnabled) {
            configureKafkaRoute();
            targets.add(KAFKA_TARGET);
        }

        RouteDefinition fanOut = from(sourceEndpoint).routeId(FAN_OUT_ROUTE_ID);
        if (targets.isEmpty()) {
            LOGGER.warn("Event2 fan-out has no enabled targets; events will be dropped. Set repo.event2.enabled=false to stop generating them.");
            fanOut.log(LoggingLevel.DEBUG, LOG_CATEGORY, "Dropping event2 message: no enabled targets");
            return;
        }

        LOGGER.info("Event2 fan-out from " + sourceEndpoint + " to " + targets);
        // sequential (keeps per-node order); no stopOnException, so every target is attempted
        fanOut.multicast(Event2FanOutRouteBuilder::collectFailures).to(targets.toArray(new String[0])).end();
    }

    /**
     * Multicast aggregation: keeps the first target failure (later ones are added as suppressed exceptions) so that it is reported to the sender once every target has been attempted.
     */
    static Exchange collectFailures(Exchange result, Exchange target) {
        if (result == null) {
            return target;
        }
        Exception failure = target.getException();
        if (failure != null) {
            if (result.getException() == null) {
                result.setException(failure);
            } else {
                result.getException().addSuppressed(failure);
            }
        }
        return result;
    }

    private void configureActiveMqRoute() {
        from(ACTIVEMQ_TARGET)
                .routeId(ACTIVEMQ_ROUTE_ID)
                // keep JMS messages identical to stock Alfresco ones
                .removeHeader(KeyedEvent2MessageProducer.RESOURCE_ID_HEADER)
                .to(activeMqEndpoint);
    }

    private void configureKafkaRoute() {
        String endpoint = kafkaEndpointUri();
        LOGGER.info("Event2 Kafka endpoint: " + URISupport.sanitizeUri(endpoint));

        RouteDefinition route = from(KAFKA_TARGET).routeId(KAFKA_ROUTE_ID);
        if (!kafkaFailOnError) {
            route.onException(Exception.class)
                    .handled(true)
                    .log(LoggingLevel.ERROR, LOG_CATEGORY, "Failed to publish event2 message to Kafka: ${exception.message}")
                    .end();
        }
        route
                // partition by node id, so all events for one node stay ordered
                .setHeader(KafkaConstants.KEY, header(KeyedEvent2MessageProducer.RESOURCE_ID_HEADER))
                .removeHeader(KeyedEvent2MessageProducer.RESOURCE_ID_HEADER)
                .removeHeaders(AbstractEventProducer.JMS_AMQP_PREFIX + "*")
                .to(endpoint);
    }

    /**
     * The Kafka endpoint URI: {@code kafkaEndpoint} if set, otherwise one built from the topic and the options.
     */
    String kafkaEndpointUri() {
        if (!isBlank(kafkaEndpoint)) {
            return kafkaEndpoint.trim();
        }
        if (isBlank(kafkaTopic)) {
            throw new IllegalStateException("Set repo.event2.route.kafka.endpoint or repo.event2.route.kafka.topic");
        }

        List<String> options = new ArrayList<>();
        kafkaOptions.forEach((name, value) -> {
            if (!isBlank(value)) {
                options.add(name + "=" + optionValue(value.trim()));
            }
        });
        if (!isBlank(kafkaExtraOptions)) {
            options.add(kafkaExtraOptions.trim().replaceFirst("^[?&]+", ""));
        }

        String uri = "kafka:" + kafkaTopic.trim();
        return options.isEmpty() ? uri : uri + "?" + String.join("&", options);
    }

    /**
     * Wraps values that contain URI-reserved characters (such as a JAAS config or a password) in Camel's RAW() syntax, so they are passed on unchanged.
     */
    static String optionValue(String value) {
        if (PLAIN_OPTION_VALUE.matcher(value).matches() || value.startsWith("RAW(") || value.startsWith("RAW{")) {
            return value;
        }
        return value.contains(")") ? "RAW{" + value + "}" : "RAW(" + value + ")";
    }

    private static boolean isBlank(String value) {
        return value == null || value.isBlank();
    }

    public void setSourceEndpoint(String sourceEndpoint) {
        this.sourceEndpoint = sourceEndpoint;
    }

    public void setActiveMqEnabled(boolean activeMqEnabled) {
        this.activeMqEnabled = activeMqEnabled;
    }

    public void setActiveMqEndpoint(String activeMqEndpoint) {
        this.activeMqEndpoint = activeMqEndpoint;
    }

    public void setKafkaEnabled(boolean kafkaEnabled) {
        this.kafkaEnabled = kafkaEnabled;
    }

    public void setKafkaEndpoint(String kafkaEndpoint) {
        this.kafkaEndpoint = kafkaEndpoint;
    }

    public void setKafkaTopic(String kafkaTopic) {
        this.kafkaTopic = kafkaTopic;
    }

    /**
     * Camel Kafka endpoint options, by option name. Blank values are left out.
     */
    public void setKafkaOptions(Map<String, String> kafkaOptions) {
        this.kafkaOptions = kafkaOptions == null ? new LinkedHashMap<>() : new LinkedHashMap<>(kafkaOptions);
    }

    /**
     * Further options appended to the built endpoint as they are, e.g. {@code maxRequestSize=2097152&retries=5}.
     */
    public void setKafkaExtraOptions(String kafkaExtraOptions) {
        this.kafkaExtraOptions = kafkaExtraOptions;
    }

    public void setKafkaFailOnError(boolean kafkaFailOnError) {
        this.kafkaFailOnError = kafkaFailOnError;
    }
}
