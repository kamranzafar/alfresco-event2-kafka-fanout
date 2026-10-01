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
import java.util.Map;
import java.util.Properties;

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
    void defaultKafkaEndpointOptionsAreValid() throws Exception {
        Properties defaults = new Properties();
        try (InputStream in = getClass().getClassLoader().getResourceAsStream("alfresco/module/alfresco-event2-kafka-fanout/alfresco-global.properties")) {
            defaults.load(in);
        }
        camelContext = new DefaultCamelContext();
        camelContext.start();

        // rejects unknown or invalid options; does not connect to a broker
        assertInstanceOf(KafkaEndpoint.class, camelContext.getEndpoint(defaults.getProperty("repo.event2.route.kafka.endpoint")));
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
