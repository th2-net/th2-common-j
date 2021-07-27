/*
 * Copyright 2020-2021 Exactpro (Exactpro Systems Limited)
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

import static com.exactpro.th2.common.event.EventUtils.createMessageBean;
import static com.exactpro.th2.common.event.EventUtils.generateUUID;
import static com.exactpro.th2.common.event.EventUtils.toEventID;
import static com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL;
import static com.google.protobuf.TextFormat.shortDebugString;
import static java.util.Objects.requireNonNull;
import static org.apache.commons.lang3.StringUtils.defaultIfBlank;
import static org.apache.commons.lang3.StringUtils.isNotBlank;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.exactpro.th2.common.grpc.EventBatch;
import com.exactpro.th2.common.grpc.EventBatchOrBuilder;
import com.exactpro.th2.common.grpc.EventID;
import com.exactpro.th2.common.grpc.EventStatus;
import com.exactpro.th2.common.grpc.MessageID;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.ByteString;
import com.google.protobuf.Timestamp;

public class Event {
    private static final Logger LOGGER = LoggerFactory.getLogger(Event.class);

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
        this(startTimestamp, null);
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

    @Contract("null -> null")
    private static @Nullable Timestamp toTimestamp(@Nullable Instant instant) {
        if (instant == null) {
            return null;
        }
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
            body.add(0, createMessageBean(description));
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
     * The default value is {@link Status#PASSED}
     * @return current event
     */
    public Event status(Status eventStatus) {
        if (eventStatus != null) {
            status = eventStatus;
        }
        return this;
    }

    /**
     * Creates and adds a new event with the same start / end time as the current event
     * @return created event
     */
    @SuppressWarnings("NonBooleanMethodNameMayNotStartWithQuestion")
    public Event addSubEventWithSamePeriod() {
        return addSubEvent(new Event(startTimestamp, endTimestamp));
    }

    /**
     * Adds passed event as a sub event
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
     * Adds the passed exception and optionally all the causes to the body data as a series of messages
     * @param includeCauses if `true` attache messages for the caused of <code>throwable</code>
     * @return current event
     */
    public Event exception(@NotNull Throwable throwable, boolean includeCauses) {
        Throwable error = Objects.requireNonNull(throwable, "Throwable can't be null");
        do {
            bodyData(createMessageBean(error.toString()));
            error = error.getCause();
        } while (includeCauses && error != null);
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
     * @deprecated prefer to use full object instead of part of them, use the {@link #toListProto(EventID)} method
     */
    @Deprecated
    public List<com.exactpro.th2.common.grpc.Event> toProtoEvents(@Nullable String parentID) throws JsonProcessingException {
        return toListProto(toEventID(parentID));
    }


    public List<com.exactpro.th2.common.grpc.Event> toListProto(@Nullable EventID parentID) throws JsonProcessingException {
        return collectSubEvents(new ArrayList<>(), parentID);
    }

    /**
     * @deprecated prefer to use full object instead of part of them, use the {@link #toProto(EventID)} method
     */
    @Deprecated
    public com.exactpro.th2.common.grpc.Event toProtoEvent(@Nullable String parentID) throws JsonProcessingException {
        return toProto(toEventID(parentID));
    }

    public com.exactpro.th2.common.grpc.Event toProto(@Nullable EventID parentID) throws JsonProcessingException {
        if (endTimestamp == null) {
            endTimestamp();
        }
        StringBuilder nameBuilder = new StringBuilder(defaultIfBlank(name, UNKNOWN_EVENT_NAME));
        if (isNotBlank(description)) {
            nameBuilder.append(" - ")
                    .append(description);
        }
        var eventBuilder = com.exactpro.th2.common.grpc.Event.newBuilder()
                .setId(toEventID(id))
                .setName(nameBuilder.toString())
                .setType(defaultIfBlank(type, UNKNOWN_EVENT_TYPE))
                .setStartTimestamp(toTimestamp(startTimestamp))
                .setEndTimestamp(toTimestamp(endTimestamp))
                .setStatus(getAggregatedStatus().eventStatus)
                .setBody(ByteString.copyFrom(buildBody()));
        if (parentID != null) {
            eventBuilder. setParentId(parentID);
        }
        for (MessageID messageID : attachedMessageIDS) {
            eventBuilder.addAttachedMessageIds(messageID);
        }
        return eventBuilder.build();
    }

    public List<EventBatch> toListBatchProto(@Nullable EventID parentID) throws JsonProcessingException {
        List<com.exactpro.th2.common.grpc.Event> events = toListProto(parentID);

        EventBatch.Builder builder = EventBatch.newBuilder()
            .addAllEvents(events);

        if (events.size() != 1) {
            builder.setParentEventId(parentID);
        }

        return Collections.singletonList(builder.build());
    }

    public List<EventBatch> toListBatchProto(int maxEventBatchContentSize, @Nullable EventID parentID) throws JsonProcessingException {
        if (maxEventBatchContentSize <= 0) {
            throw new IllegalArgumentException("'maxEventBatchContentSize' should be greater than zero, actual: " + maxEventBatchContentSize);
        }

        List<com.exactpro.th2.common.grpc.Event> events = toListProto(parentID);

        List<EventBatch> result = new ArrayList<>();
        Map<EventID, List<com.exactpro.th2.common.grpc.Event>> eventGroups = events.stream().collect(Collectors.groupingBy(com.exactpro.th2.common.grpc.Event::getParentId));
        batch(maxEventBatchContentSize, result, eventGroups, parentID);
        return result;
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

    /**
     * @deprecated prefer to use full object instead of part of them, use the {@link #collectSubEvents(List, EventID)} method
     */
    @Deprecated
    protected List<com.exactpro.th2.common.grpc.Event> collectSubEvents(List<com.exactpro.th2.common.grpc.Event> protoEvents, @Nullable String parentID) throws JsonProcessingException {
        return collectSubEvents(protoEvents, toEventID(parentID));
    }

    protected List<com.exactpro.th2.common.grpc.Event> collectSubEvents(List<com.exactpro.th2.common.grpc.Event> protoEvents, @Nullable EventID parentID) throws JsonProcessingException {
        protoEvents.add(toProto(parentID)); // collect current level
        for (Event subEvent : subEvents) {
            subEvent.collectSubEvents(protoEvents, toEventID(id)); // collect sub level
        }
        return protoEvents;
    }

    protected byte[] buildBody() throws JsonProcessingException {
        return OBJECT_MAPPER.get().writeValueAsBytes(body);
    }

    protected String formatStateException(String fieldName, Object value) {
        return fieldName + " in event '" + id + "' already sed with value '" + value + '\'';
    }

    /**
     * @deprecated use {@link #getAggregatedStatus} instead
     */
    @Deprecated
    protected Status getAggrigatedStatus() {
        return getAggregatedStatus();
    }

    @NotNull
    protected Status getAggregatedStatus() {
        if (status == Status.PASSED) {
            return subEvents.stream().anyMatch(subEvent -> subEvent.getAggregatedStatus() == Status.FAILED)
                    ? Status.FAILED
                    : Status.PASSED;
        }
        return Status.FAILED;
    }

    private void batch(int maxEventBatchContentSize, List<EventBatch> result, Map<EventID, List<com.exactpro.th2.common.grpc.Event>> eventGroups, EventID eventID) throws JsonProcessingException {
        EventBatch.Builder builder = EventBatch.newBuilder().setParentEventId(eventID);

        List<com.exactpro.th2.common.grpc.Event> events = Objects.requireNonNull(eventGroups.get(eventID),
                "Neither of events refers to " + shortDebugString(eventID));

        for (com.exactpro.th2.common.grpc.Event event : events) {
            com.exactpro.th2.common.grpc.Event checkedEvent = checkAndRebuild(maxEventBatchContentSize, event);

            LOGGER.trace("Process {} {}", checkedEvent.getName(), checkedEvent.getType());
            if(eventGroups.containsKey(checkedEvent.getId())) {
                result.add(checkAndBuild(maxEventBatchContentSize, EventBatch.newBuilder()
                        .addEvents(checkedEvent)));

                batch(maxEventBatchContentSize, result, eventGroups, checkedEvent.getId());
            } else {
                if(builder.getEventsCount() > 0
                        && getContentSize(builder) + getContentSize(checkedEvent) > maxEventBatchContentSize) {
                    result.add(checkAndBuild(maxEventBatchContentSize, builder));
                    builder = EventBatch.newBuilder().setParentEventId(eventID);
                }
                builder.addEvents(checkedEvent);
            }
        }

        if(builder.getEventsCount() > 0) {
            result.add(checkAndBuild(maxEventBatchContentSize, builder));
        }
    }

    private EventBatch checkAndBuild(int maxEventBatchContentSize, EventBatch.Builder builder) {
        int contentSize = getContentSize(builder);
        if(contentSize > maxEventBatchContentSize) {
            throw new IllegalStateException("The smallest batch size exceeds the max event batch content size, max " + maxEventBatchContentSize + ", actual " + contentSize);
        }

        return builder.build();
    }

    private com.exactpro.th2.common.grpc.Event checkAndRebuild(int maxEventBatchContentSize, com.exactpro.th2.common.grpc.Event event) throws JsonProcessingException {
        if (getContentSize(event) > maxEventBatchContentSize) {
            return com.exactpro.th2.common.grpc.Event.newBuilder(event)
                    .setStatus(EventStatus.FAILED)
                    .setBody(ByteString.copyFrom(OBJECT_MAPPER.get().writeValueAsBytes(
                            Collections.singletonList(createMessageBean("Event " + shortDebugString(event.getId()) + " exceeds max size, max " + maxEventBatchContentSize + ", actual " + getContentSize(event))))))
                    .build();
        }
        return event;
    }

    private static int getContentSize(com.exactpro.th2.common.grpc.Event event) {
        return event.getBody().size();
    }

    private static int getContentSize(EventBatchOrBuilder eventBatch) {
        return eventBatch.getEventsList().stream().map(Event::getContentSize).mapToInt(Integer::intValue).sum();
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
