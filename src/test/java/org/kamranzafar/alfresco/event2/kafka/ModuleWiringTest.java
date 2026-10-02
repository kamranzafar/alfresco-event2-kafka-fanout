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

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.camel.ProducerTemplate;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.config.PropertyPlaceholderConfigurer;
import org.springframework.context.support.GenericXmlApplicationContext;
import org.springframework.test.util.ReflectionTestUtils;

import java.io.InputStream;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.mockito.Mockito.mock;

/**
 * Loads the module's Spring XML with its default properties, the way Alfresco does (global properties placeholder, stock beans present).
 *
 * @author Kamran Zafar
 */
class ModuleWiringTest {
    @Test
    void moduleContextReplacesTheStockProducer() throws Exception {
        try (GenericXmlApplicationContext context = context("classpath:alfresco/module/alfresco-event2-kafka-fanout/module-context.xml")) {
            KeyedEvent2MessageProducer producer = assertInstanceOf(KeyedEvent2MessageProducer.class, context.getBean("event2MessageProducer"));
            assertEquals("direct:alfresco.repo.event2.fanout", ReflectionTestUtils.getField(producer, "endpoint"));
        }
    }

    @Test
    void subsystemContextDeclaresTheRouteBuilderWithDefaults() throws Exception {
        try (GenericXmlApplicationContext context = context("classpath:alfresco/extension/subsystems/Messaging/default/default/event2-kafka-fanout-context.xml")) {
            Event2FanOutRouteBuilder builder = context.getBean(Event2FanOutRouteBuilder.class);
            assertEquals("direct:alfresco.repo.event2.fanout", ReflectionTestUtils.getField(builder, "sourceEndpoint"));
            assertEquals(true, ReflectionTestUtils.getField(builder, "activeMqEnabled"));
            assertEquals("amqp:topic:alfresco.repo.event2", ReflectionTestUtils.getField(builder, "activeMqEndpoint"));
            assertEquals(false, ReflectionTestUtils.getField(builder, "kafkaEnabled"));
            assertEquals(false, ReflectionTestUtils.getField(builder, "kafkaFailOnError"));
            assertEquals("kafka:alfresco.repo.event2?brokers=localhost:9092&requestRequiredAcks=all&enableIdempotence=true&maxBlockMs=5000&requestTimeoutMs=5000&deliveryTimeoutMs=10000",
                    builder.kafkaEndpointUri());
        }
    }

    @SuppressWarnings("deprecation")
    private static GenericXmlApplicationContext context(String location) throws Exception {
        Properties properties = new Properties();
        // stock default from alfresco/repository.properties, then this module's defaults
        properties.setProperty("repo.event2.topic.endpoint", "amqp:topic:alfresco.repo.event2");
        try (InputStream in = ModuleWiringTest.class.getClassLoader().getResourceAsStream("alfresco/module/alfresco-event2-kafka-fanout/alfresco-global.properties")) {
            properties.load(in);
        }
        PropertyPlaceholderConfigurer placeholders = new PropertyPlaceholderConfigurer();
        placeholders.setProperties(properties);

        GenericXmlApplicationContext context = new GenericXmlApplicationContext();
        context.getBeanFactory().registerSingleton("camelProducerTemplate", mock(ProducerTemplate.class));
        context.getBeanFactory().registerSingleton("event2ObjectMapper", new ObjectMapper());
        context.addBeanFactoryPostProcessor(placeholders);
        context.load(location);
        context.refresh();
        return context;
    }
}
