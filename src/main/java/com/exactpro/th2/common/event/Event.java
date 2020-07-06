/*
 * Copyright 2020-2020 Exactpro (Exactpro Systems Limited)
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
package com.exactpro.th2.common.event;

import static com.exactpro.th2.common.event.EventUtils.generateUUID;
import static com.exactpro.th2.common.message.MessageUtils.toProtoMessageID;
import static com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL;
import static java.util.Objects.requireNonNull;
import static org.apache.commons.lang3.StringUtils.defaultIfBlank;
import static org.apache.commons.lang3.StringUtils.isNotBlank;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import com.exactpro.th2.infra.grpc.EventID;
import com.exactpro.th2.infra.grpc.EventStatus;
import com.exactpro.th2.infra.grpc.MessageID;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.ByteString;
import com.google.protobuf.Timestamp;

public class Event {

    public static final String UNKNOWN_EVENT_NAME = "Unknown event name";
    public static final String UNKNOWN_EVENT_TYPE = "Unknown event type";

    protected static final ThreadLocal<ObjectMapper> OBJECT_MAPPER = ThreadLocal.withInitial(() -> new ObjectMapper().setSerializationInclusion(NON_NULL));

    protected final String id = generateUUID();
    protected final List<Event> subEvents = new ArrayList<>();
    protected final List<MessageID> attachedMessageIDS = new ArrayList<>();
    protected final List<IBodyData> body = new ArrayList<>();
    protected final Instant startTimestamp;
    protected Instant endTimestamp;
    protected String type;
    protected String name;
    protected String description;
    protected Status status = Status.PASSED;

    protected Event(Instant startTimestamp, @Nullable Instant endTimestamp) {
        this.startTimestamp = startTimestamp;
        this.endTimestamp = endTimestamp;
    }

    protected Event(Instant startTimestamp) {
        this(Instant.now(), null);
    }

    protected Event() {
        this(Instant.now());
    }

    /**
     * Creates event with current time as start
     * @return new event
     */
    public static Event start() {
        return new Event();
    }

    /**
     * Creates event with passed time as start
     * @return new event
     */
    public static Event from(Instant startTimestamp) {
        return new Event(startTimestamp);
    }

    // FIXME: move to th2-common
    private static EventID toEventID(String id) {
        return EventID.newBuilder()
                .setId(id)
                .build();
    }

    // FIXME: move to th2-common
    private static Timestamp toTimestamp(Instant instant) {
        return Timestamp.newBuilder()
                .setSeconds(instant.getEpochSecond())
                .setNanos(instant.getNano())
                .build();
    }

    public Event endTimestamp() {
        if (endTimestamp != null) {
            throw new IllegalStateException(formatStateException("End time", endTimestamp));
        }
        endTimestamp = Instant.now();
        return this;
    }

    /**
     * Sets event name if passed {@code eventName} is not blank.
     * The {@link #UNKNOWN_EVENT_NAME} value will be used as default in the {@link #toProtoEvent(String)} and {@link #toProtoEvents(String)} methods if this property isn't set
     * @return current event
     * @throws IllegalStateException if name already set
     */
    public Event name(String eventName) {
        if (isNotBlank(eventName)) {
            if (name != null) {
                throw new IllegalStateException(formatStateException("Name", name));
            }
            name = eventName;
        }
        return this;
    }

    /**
     * Sets event description if passed {@code description} is not blank.
     * This property value will be appended to the end of event name and added into event body in the {@link #toProtoEvent(String)} and {@link #toProtoEvents(String)} methods if this property isn't set
     * @return current event
     * @throws IllegalStateException if description already set
     */
    public Event description(String description) {
        if (isNotBlank(description)) {
            if (this.description != null) {
                throw new IllegalStateException(formatStateException("Description", this.description));
            }
            body.add(0, EventUtils.createMessageBean(description));
            this.description = description;
        }
        return this;
    }

    /**
     * Sets event type if passed {@code eventType} is not blank.
     * The {@link #UNKNOWN_EVENT_TYPE} value will be used as default in the {@link #toProtoEvent(String)} and {@link #toProtoEvents(String)} methods if this property isn't set
     * @return current event
     * @throws IllegalStateException if type already set
     */
    public Event type(String eventType) {
        if (isNotBlank(eventType)) {
            if (type != null) {
                throw new IllegalStateException(formatStateException("Type", type));
            }
            type = eventType;
        }
        return this;
    }

    /**
     * Sets event status if passed {@code eventStatus} isn't null.
     * Default value is {@link Status#PASSED}
     * @return current event
     */
    public Event status(Status eventStatus) {
        if (eventStatus != null) {
            status = eventStatus;
        }
        return this;
    }

    /**
     * Cretaes and adds new event with the same start / end time as current event
     * @return created event
     */
    @SuppressWarnings("NonBooleanMethodNameMayNotStartWithQuestion")
    public Event addSubEventWithSamePeriod() {
        return addSubEvent(new Event(startTimestamp, endTimestamp));
    }

    /**
     * Adds passed event as sub event
     * @return passed event
     * @throws NullPointerException if {@code subEvent} is null
     */
    @SuppressWarnings("NonBooleanMethodNameMayNotStartWithQuestion")
    public Event addSubEvent(Event subEvent) {
        subEvents.add(requireNonNull(subEvent, "Sub event can't be null"));
        return subEvent;
    }

    /**
     * Adds passed body data bodyData
     * @return current event
     */
    public Event bodyData(IBodyData bodyData) {
        body.add(requireNonNull(bodyData, "Body data can't be null"));
        return this;
    }

    /**
     * Adds message id as linked
     * @return current event
     */
    public Event messageID(MessageID attachedMessageID) {
        attachedMessageIDS.add(requireNonNull(attachedMessageID, "Attached message id can't be null"));
        return this;
    }

    /**
     * @deprecated please use {@link #messageID(MessageID)} method
     */
    @Deprecated(since = "TH2 1.1", forRemoval = true)
    public Event messageID(com.exactpro.th2.common.event.bean.MessageID attachedMessageID) {
        messageID(toProtoMessageID(requireNonNull(attachedMessageID, "Attached message id can't be null")));
        return this;
    }


    /**
     * @deprecated Please use {@link #toProtoEvent(String)}
     */
    @Deprecated(since = "TH2 1.1", forRemoval = true)
    public List<com.exactpro.th2.infra.grpc.Event> toProtoEventBuilders(String parentID) throws JsonProcessingException {
        return toProtoEvents(parentID);
    }

    public List<com.exactpro.th2.infra.grpc.Event> toProtoEvents(String parentID) throws JsonProcessingException {
        return collectSubEvents(new ArrayList<>(), parentID);
    }

    public com.exactpro.th2.infra.grpc.Event toProtoEvent(@Nullable String parentID) throws JsonProcessingException {
        if (endTimestamp == null) {
            endTimestamp();
        }
        StringBuilder nameBuilder = new StringBuilder(defaultIfBlank(name, UNKNOWN_EVENT_NAME));
        if (isNotBlank(description)) {
            nameBuilder.append(" - ")
                    .append(description);
        }
        var eventBuilder = com.exactpro.th2.infra.grpc.Event.newBuilder()
                .setId(toEventID(id))
                .setName(nameBuilder.toString())
                .setType(defaultIfBlank(type, UNKNOWN_EVENT_TYPE))
                .setStartTimestamp(toTimestamp(startTimestamp))
                .setEndTimestamp(toTimestamp(endTimestamp))
                .setStatus(getAggrigatedStatus().eventStatus)
                .setBody(ByteString.copyFrom(buildBody()));
        if (parentID != null) {
            eventBuilder. setParentId(toEventID(parentID));
        }
        for (MessageID messageID : attachedMessageIDS) {
            eventBuilder.addAttachedMessageIds(messageID);
        }
        return eventBuilder.build();
    }

    public String getId() {
        return id;
    }

    public Instant getStartTimestamp() {
        return startTimestamp;
    }

    public Instant getEndTimestamp() {
        return endTimestamp;
    }

    protected List<com.exactpro.th2.infra.grpc.Event> collectSubEvents(List<com.exactpro.th2.infra.grpc.Event> protoEvents, @Nullable String parentID) throws JsonProcessingException {
        protoEvents.add(toProtoEvent(parentID)); // collect current level
        for (Event subEvent : subEvents) {
            subEvent.collectSubEvents(protoEvents, id); // collect sub level
        }
        return protoEvents;
    }

    protected byte[] buildBody() throws JsonProcessingException {
        return OBJECT_MAPPER.get().writeValueAsBytes(body);
    }

    protected String formatStateException(String fieldName, Object value) {
        return fieldName + " in event '" + id + "' already sed with value '" + value + '\'';
    }

    @NotNull
    protected Status getAggrigatedStatus() {
        if (status == Status.PASSED) {
            return subEvents.stream().anyMatch(subEvent -> subEvent.getAggrigatedStatus() == Status.FAILED)
                    ? Status.FAILED
                    : Status.PASSED;
        }
        return Status.FAILED;
    }

    public enum Status {
        PASSED(EventStatus.SUCCESS),
        FAILED(EventStatus.FAILED);

        private final EventStatus eventStatus;

        Status(EventStatus eventStatus) {
            this.eventStatus = eventStatus;
        }
    }
}
