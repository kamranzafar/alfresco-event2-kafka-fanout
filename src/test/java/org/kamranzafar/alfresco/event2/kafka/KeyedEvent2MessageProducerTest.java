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
import org.alfresco.repo.event.v1.model.*;
import org.apache.camel.ExchangePattern;
import org.apache.camel.ProducerTemplate;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

/**
 * Tests for {@link KeyedEvent2MessageProducer}.
 *
 * @author Kamran Zafar
 */
class KeyedEvent2MessageProducerTest {
    @Test
    void usesNodeIdForNodeEvents() {
        Map<String, Object> headers = KeyedEvent2MessageProducer.withResourceIdHeader(event(NodeResource.builder().setId("node-1").build()), null);

        assertEquals("node-1", headers.get(KeyedEvent2MessageProducer.RESOURCE_ID_HEADER));
    }

    @Test
    void usesChildIdForChildAssociationEvents() {
        Map<String, Object> headers = KeyedEvent2MessageProducer.withResourceIdHeader(
                event(new ChildAssociationResource("parent-1", "child-1", "cm:contains", "cm:doc")), Map.of("existing", "value"));

        assertEquals("child-1", headers.get(KeyedEvent2MessageProducer.RESOURCE_ID_HEADER));
        assertEquals("value", headers.get("existing"));
    }

    @Test
    void usesSourceIdForPeerAssociationEvents() {
        Map<String, Object> headers = KeyedEvent2MessageProducer.withResourceIdHeader(
                event(new PeerAssociationResource("source-1", "target-1", "cm:references")), null);

        assertEquals("source-1", headers.get(KeyedEvent2MessageProducer.RESOURCE_ID_HEADER));
    }

    @Test
    void leavesHeadersUntouchedForNonRepoEvents() {
        assertNull(KeyedEvent2MessageProducer.withResourceIdHeader("{}", null));
    }

    @Test
    void addsHeaderWhenSendingToFanOut() {
        ProducerTemplate template = mock(ProducerTemplate.class);
        producer(template, "direct:alfresco.repo.event2.fanout").send(event(NodeResource.builder().setId("node-1").build()));

        assertEquals("node-1", sentHeaders(template, "direct:alfresco.repo.event2.fanout").get(KeyedEvent2MessageProducer.RESOURCE_ID_HEADER));
    }

    @Test
    void addsNoHeaderWhenSendingStraightToBroker() {
        ProducerTemplate template = mock(ProducerTemplate.class);
        producer(template, "amqp:topic:alfresco.repo.event2").send(event(NodeResource.builder().setId("node-1").build()));

        assertFalse(sentHeaders(template, "amqp:topic:alfresco.repo.event2").containsKey(KeyedEvent2MessageProducer.RESOURCE_ID_HEADER));
    }

    private static KeyedEvent2MessageProducer producer(ProducerTemplate template, String endpoint) {
        KeyedEvent2MessageProducer producer = new KeyedEvent2MessageProducer();
        producer.setProducer(template);
        producer.setObjectMapper(new ObjectMapper());
        producer.setEndpoint(endpoint);
        return producer;
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> sentHeaders(ProducerTemplate template, String endpoint) {
        ArgumentCaptor<Map<String, Object>> headers = ArgumentCaptor.forClass(Map.class);
        verify(template).sendBodyAndHeaders(eq(endpoint), eq(ExchangePattern.InOnly), any(), headers.capture());
        return headers.getValue();
    }

    private static <R extends Resource> RepoEvent<EventData<R>> event(R resource) {
        return RepoEvent.<EventData<R>>builder()
                .setData(EventData.<R>builder().setResource(resource).build())
                .build();
    }
}
