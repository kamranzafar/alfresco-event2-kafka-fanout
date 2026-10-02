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
import org.apache.camel.CamelContext;
import org.apache.camel.CamelExecutionException;
import org.apache.camel.component.kafka.KafkaConstants;
import org.apache.camel.component.kafka.KafkaEndpoint;
import org.apache.camel.component.mock.MockEndpoint;
import org.apache.camel.impl.DefaultCamelContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for {@link Event2FanOutRouteBuilder}.
 *
 * @author Kamran Zafar
 */
class Event2FanOutRouteBuilderTest {
    private static final String SOURCE = "direct:alfresco.repo.event2.fanout";
    private static final String ACTIVEMQ = "mock:activemq";
    private static final String KAFKA = "mock:kafka";
    private static final String BODY = "{\"type\":\"org.alfresco.event.node.Created\"}";
    private static final Map<String, Object> HEADERS = Map.of(
            KeyedEvent2MessageProducer.RESOURCE_ID_HEADER, "node-1",
            AbstractEventProducer.JMS_AMQP_MESSAGE_FORMAT, AbstractEventProducer.AMQP_UNKNOWN);

    private CamelContext camelContext;

    @AfterEach
    void tearDown() {
        if (camelContext != null) {
            camelContext.stop();
        }
    }

    @Test
    void publishesToBothTargetsWhenBothEnabled() throws Exception {
        start(true, true, false);
        MockEndpoint activeMq = mock(ACTIVEMQ, 1);
        MockEndpoint kafka = mock(KAFKA, 1);

        send();

        MockEndpoint.assertIsSatisfied(camelContext);
        Map<String, Object> activeMqHeaders = activeMq.getExchanges().get(0).getMessage().getHeaders();
        assertNull(activeMqHeaders.get(KeyedEvent2MessageProducer.RESOURCE_ID_HEADER));
        assertEquals(AbstractEventProducer.AMQP_UNKNOWN, activeMqHeaders.get(AbstractEventProducer.JMS_AMQP_MESSAGE_FORMAT));

        Map<String, Object> kafkaHeaders = kafka.getExchanges().get(0).getMessage().getHeaders();
        assertEquals("node-1", kafkaHeaders.get(KafkaConstants.KEY));
        assertNull(kafkaHeaders.get(KeyedEvent2MessageProducer.RESOURCE_ID_HEADER));
        assertNull(kafkaHeaders.get(AbstractEventProducer.JMS_AMQP_MESSAGE_FORMAT));
        assertEquals(BODY, kafka.getExchanges().get(0).getMessage().getBody(String.class));
    }

    @Test
    void publishesOnlyToActiveMqWhenKafkaDisabled() throws Exception {
        start(true, false, false);
        mock(ACTIVEMQ, 1);
        mock(KAFKA, 0);

        send();

        MockEndpoint.assertIsSatisfied(camelContext);
        assertNull(camelContext.getRoute(Event2FanOutRouteBuilder.KAFKA_ROUTE_ID));
    }

    @Test
    void publishesOnlyToKafkaWhenActiveMqDisabled() throws Exception {
        start(false, true, false);
        mock(ACTIVEMQ, 0);
        mock(KAFKA, 1);

        send();

        MockEndpoint.assertIsSatisfied(camelContext);
        assertNull(camelContext.getRoute(Event2FanOutRouteBuilder.ACTIVEMQ_ROUTE_ID));
    }

    @Test
    void dropsEventsWhenNoTargetIsEnabled() {
        start(false, false, false);

        send();

        assertEquals(1, camelContext.getRoutes().size());
    }

    @Test
    void addsNoRoutesWhenProducerBypassesFanOut() {
        Event2FanOutRouteBuilder builder = routeBuilder(true, true, false);
        builder.setSourceEndpoint("amqp:topic:alfresco.repo.event2");
        start(builder);

        assertTrue(camelContext.getRoutes().isEmpty());
    }

    @Test
    void swallowsKafkaFailureByDefault() throws Exception {
        start(true, true, false);
        MockEndpoint activeMq = mock(ACTIVEMQ, 1);
        failOn(KAFKA);

        send();

        activeMq.assertIsSatisfied();
    }

    @Test
    void publishesToKafkaEvenWhenActiveMqFails() throws Exception {
        start(true, true, false);
        failOn(ACTIVEMQ);
        MockEndpoint kafka = mock(KAFKA, 1);

        CamelExecutionException failure = assertThrows(CamelExecutionException.class, this::send);

        kafka.assertIsSatisfied();
        assertEquals("node-1", kafka.getExchanges().get(0).getMessage().getHeader(KafkaConstants.KEY));
        assertEquals("activemq unavailable", rootCause(failure).getMessage());
    }

    @Test
    void reportsBothFailuresWhenKafkaFailOnErrorIsSet() {
        start(true, true, true);
        failOn(ACTIVEMQ);
        failOn(KAFKA);

        CamelExecutionException failure = assertThrows(CamelExecutionException.class, this::send);

        Throwable first = rootCause(failure);
        assertEquals("activemq unavailable", first.getMessage());
        assertEquals(1, first.getSuppressed().length);
        assertEquals("kafka unavailable", first.getSuppressed()[0].getMessage());
    }

    @Test
    void publishesToActiveMqAndReportsKafkaFailureWhenFailOnErrorIsSet() throws Exception {
        start(true, true, true);
        MockEndpoint activeMq = mock(ACTIVEMQ, 1);
        failOn(KAFKA);

        CamelExecutionException failure = assertThrows(CamelExecutionException.class, this::send);

        activeMq.assertIsSatisfied();
        assertEquals("kafka unavailable", rootCause(failure).getMessage());
    }

    @Test
    void endpointPropertyTakesPrecedenceOverIndividualOptions() {
        Event2FanOutRouteBuilder builder = new Event2FanOutRouteBuilder();
        builder.setKafkaEndpoint(" kafka:other?brokers=broker:9092 ");
        builder.setKafkaTopic("alfresco.repo.event2");
        builder.setKafkaOptions(Map.of("brokers", "localhost:9092"));

        assertEquals("kafka:other?brokers=broker:9092", builder.kafkaEndpointUri());
    }

    @Test
    void buildsEndpointFromTopicAndNonBlankOptions() {
        Event2FanOutRouteBuilder builder = new Event2FanOutRouteBuilder();
        builder.setKafkaEndpoint("");
        builder.setKafkaTopic("alfresco.repo.event2");
        Map<String, String> options = new LinkedHashMap<>();
        options.put("brokers", "k1:9092,k2:9092");
        options.put("clientId", " ");
        options.put("securityProtocol", null);
        options.put("maxBlockMs", "5000");
        builder.setKafkaOptions(options);
        builder.setKafkaExtraOptions("&retries=5");

        assertEquals("kafka:alfresco.repo.event2?brokers=k1:9092,k2:9092&maxBlockMs=5000&retries=5", builder.kafkaEndpointUri());
    }

    @Test
    void failsWithoutEndpointOrTopic() {
        Event2FanOutRouteBuilder builder = new Event2FanOutRouteBuilder();
        builder.setKafkaEndpoint("");
        builder.setKafkaTopic("");

        assertThrows(IllegalStateException.class, builder::kafkaEndpointUri);
    }

    @Test
    void passesOptionValuesWithSpecialCharactersUnchanged() {
        String jaas = "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"user\" password=\"p&s=s+w%d\";";
        String password = "se(cr)et&1";
        Event2FanOutRouteBuilder builder = new Event2FanOutRouteBuilder();
        builder.setKafkaTopic("alfresco.repo.event2");
        Map<String, String> options = new LinkedHashMap<>();
        options.put("brokers", "localhost:9092");
        options.put("securityProtocol", "SASL_SSL");
        options.put("saslJaasConfig", jaas);
        options.put("sslTruststorePassword", password);
        builder.setKafkaOptions(options);
        camelContext = new DefaultCamelContext();
        camelContext.start();

        KafkaEndpoint endpoint = assertInstanceOf(KafkaEndpoint.class, camelContext.getEndpoint(builder.kafkaEndpointUri()));

        assertEquals(jaas, endpoint.getConfiguration().getSaslJaasConfig());
        assertEquals(password, endpoint.getConfiguration().getSslTruststorePassword());
        assertEquals("SASL_SSL", endpoint.getConfiguration().getSecurityProtocol());
    }

    @Test
    void defaultKafkaEndpointOptionsAreValid() throws Exception {
        Properties defaults = new Properties();
        try (InputStream in = getClass().getClassLoader().getResourceAsStream("alfresco/module/alfresco-event2-kafka-fanout/alfresco-global.properties")) {
            defaults.load(in);
        }
        String prefix = "repo.event2.route.kafka.";
        Map<String, String> options = new LinkedHashMap<>();
        for (String name : defaults.stringPropertyNames()) {
            String option = name.substring(prefix.length());
            if (name.startsWith(prefix) && !Set.of("enabled", "endpoint", "topic", "options", "failOnError").contains(option)) {
                options.put(option, defaults.getProperty(name));
            }
        }
        Event2FanOutRouteBuilder builder = new Event2FanOutRouteBuilder();
        builder.setKafkaEndpoint(defaults.getProperty(prefix + "endpoint"));
        builder.setKafkaTopic(defaults.getProperty(prefix + "topic"));
        builder.setKafkaOptions(options);
        builder.setKafkaExtraOptions(defaults.getProperty(prefix + "options"));
        camelContext = new DefaultCamelContext();
        camelContext.start();

        // rejects unknown or invalid options; does not connect to a broker
        KafkaEndpoint endpoint = assertInstanceOf(KafkaEndpoint.class, camelContext.getEndpoint(builder.kafkaEndpointUri()));
        assertEquals("alfresco.repo.event2", endpoint.getConfiguration().getTopic());
        assertEquals("localhost:9092", endpoint.getConfiguration().getBrokers());
        assertEquals("all", endpoint.getConfiguration().getRequestRequiredAcks());
        assertEquals(10000, endpoint.getConfiguration().getDeliveryTimeoutMs());
    }

    private void start(boolean activeMqEnabled, boolean kafkaEnabled, boolean kafkaFailOnError) {
        start(routeBuilder(activeMqEnabled, kafkaEnabled, kafkaFailOnError));
    }

    private static Event2FanOutRouteBuilder routeBuilder(boolean activeMqEnabled, boolean kafkaEnabled, boolean kafkaFailOnError) {
        Event2FanOutRouteBuilder builder = new Event2FanOutRouteBuilder();
        builder.setSourceEndpoint(SOURCE);
        builder.setActiveMqEnabled(activeMqEnabled);
        builder.setActiveMqEndpoint(ACTIVEMQ);
        builder.setKafkaEnabled(kafkaEnabled);
        builder.setKafkaEndpoint(KAFKA);
        builder.setKafkaFailOnError(kafkaFailOnError);
        return builder;
    }

    private void start(Event2FanOutRouteBuilder builder) {
        try {
            camelContext = new DefaultCamelContext();
            camelContext.addRoutes(builder);
            camelContext.start();
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    private MockEndpoint mock(String uri, int expectedCount) {
        MockEndpoint endpoint = camelContext.getEndpoint(uri, MockEndpoint.class);
        endpoint.expectedMessageCount(expectedCount);
        return endpoint;
    }

    private void failOn(String uri) {
        String target = uri.substring(uri.indexOf(':') + 1);
        camelContext.getEndpoint(uri, MockEndpoint.class).whenAnyExchangeReceived(exchange -> {
            throw new IllegalStateException(target + " unavailable");
        });
    }

    private static Throwable rootCause(Throwable failure) {
        Throwable cause = failure;
        while (cause.getCause() != null) {
            cause = cause.getCause();
        }
        return cause;
    }

    private void send() {
        camelContext.createProducerTemplate().sendBodyAndHeaders(SOURCE, BODY, HEADERS);
    }
}
