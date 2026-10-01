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

import org.alfresco.repo.event.v1.model.*;
import org.alfresco.repo.event2.Event2MessageProducer;
import org.apache.camel.ExchangePattern;

import java.util.HashMap;
import java.util.Map;

/**
 * Replaces the core {@code event2MessageProducer} bean (see this module's {@code module-context.xml}).
 * <p>
 * When sending to an in-process {@code direct:} endpoint (the fan-out route), it adds {@link #RESOURCE_ID_HEADER}: the id of the node the event is about, used as the Kafka partition key. When sending straight to a broker, it behaves exactly like {@link Event2MessageProducer}.
 *
 * @author Kamran Zafar
 */
public class KeyedEvent2MessageProducer extends Event2MessageProducer {
    /**
     * Id of the node an event is about: the node itself, the child of a child association, or the source of a peer association.
     */
    public static final String RESOURCE_ID_HEADER = "alfrescoEventResourceId";

    @Override
    public void send(String endpointUri, ExchangePattern exchangePattern, Object event, Map<String, Object> headers) {
        if (endpointUri != null && endpointUri.startsWith("direct:")) {
            headers = withResourceIdHeader(event, headers);
        }
        super.send(endpointUri, exchangePattern, event, headers);
    }

    static Map<String, Object> withResourceIdHeader(Object event, Map<String, Object> headers) {
        String resourceId = getResourceId(event);
        if (resourceId == null) {
            return headers;
        }
        Map<String, Object> result = headers == null ? new HashMap<>() : new HashMap<>(headers);
        result.put(RESOURCE_ID_HEADER, resourceId);
        return result;
    }

    private static String getResourceId(Object event) {
        if (!(event instanceof RepoEvent<?> repoEvent) || !(repoEvent.getData() instanceof DataAttributes<?> data)) {
            return null;
        }
        Resource resource = data.getResource();
        if (resource instanceof AbstractNodeResource nodeResource) {
            return nodeResource.getId();
        }
        if (resource instanceof ChildAssociationResource childAssoc && childAssoc.getChild() != null) {
            return childAssoc.getChild().getId();
        }
        if (resource instanceof PeerAssociationResource peerAssoc && peerAssoc.getSource() != null) {
            return peerAssoc.getSource().getId();
        }
        return null;
    }
}
