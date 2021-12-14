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
import static com.exactpro.th2.common.event.EventUtils.requireNonBlankBookName;
import static com.exactpro.th2.common.event.EventUtils.requireNonBlankScope;
import static com.exactpro.th2.common.event.EventUtils.requireNonNullParentId;
import static com.exactpro.th2.common.event.EventUtils.toEventID;
import static com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL;
import static com.google.protobuf.TextFormat.shortDebugString;
import static java.util.Objects.requireNonNull;
import static java.util.Objects.requireNonNullElse;
import static org.apache.commons.lang3.StringUtils.defaultIfBlank;
import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.apache.commons.lang3.StringUtils.isNotBlank;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
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
    public static final EventID DEFAULT_EVENT_ID = EventID.getDefaultInstance();

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
     * The {@link #UNKNOWN_EVENT_NAME} value will be used as default in the {@link #toProto(com.exactpro.th2.common.grpc.EventID)} and {@link #toListProto(com.exactpro.th2.common.grpc.EventID)} methods if this property isn't set
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
     * This property value will be appended to the end of event name and added into event body in the {@link #toProto(com.exactpro.th2.common.grpc.EventID)} and {@link #toListProto(com.exactpro.th2.common.grpc.EventID)} methods if this property isn't set
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
     * The {@link #UNKNOWN_EVENT_TYPE} value will be used as default in the {@link #toProto(com.exactpro.th2.common.grpc.EventID)} and {@link #toListProto(com.exactpro.th2.common.grpc.EventID)} methods if this property isn't set
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
     * Adds passed collection of body data
     * @return current event
     */
    public Event bodyData(Collection<? extends IBodyData> bodyDataCollection) {
        body.addAll(requireNonNull(bodyDataCollection, "Body data collection cannot be null"));
        return this;
    }

    /**
     * Adds the passed exception and optionally all the causes to the body data as a series of messages
     * @param includeCauses if `true` attache messages for the caused of <code>throwable</code>
     * @return current event
     */
    public Event exception(@NotNull Throwable throwable, boolean includeCauses) {
        Throwable error = requireNonNull(throwable, "Throwable can't be null");
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

    public List<com.exactpro.th2.common.grpc.Event> toListProto(@NotNull EventID parentId) throws JsonProcessingException {
        return toListProto(
                new ArrayList<>(),
                requireNonNullParentId(parentId),
                requireNonBlankBookName(parentId.getBookName()),
                parentId.getScope()
        );
    }

    public List<com.exactpro.th2.common.grpc.Event> toListProto(@NotNull String bookName) throws JsonProcessingException {
        return toListProto(
                new ArrayList<>(),
                null,
                requireNonBlankBookName(bookName),
                null
        );
    }

    public List<com.exactpro.th2.common.grpc.Event> toListProto(
            @NotNull String bookName,
            @NotNull String scope
    ) throws JsonProcessingException {
        return toListProto(
                new ArrayList<>(),
                null,
                requireNonBlankBookName(bookName),
                requireNonBlankScope(scope)
        );
    }

    private List<com.exactpro.th2.common.grpc.Event> toListProto(
            List<com.exactpro.th2.common.grpc.Event> protoEvents,
            @Nullable EventID parentId,
            @NotNull String bookName,
            @Nullable String scope
    ) throws JsonProcessingException {
        protoEvents.add(toProto(parentId, bookName, scope)); // collect current level
        for (Event subEvent : subEvents) {
            EventID eventId = isBlank(scope)
                    ? toEventID(bookName, id)
                    : toEventID(bookName, scope, id);
            subEvent.toListProto(protoEvents, eventId, bookName, scope); // collect sub level
        }
        return protoEvents;
    }

    public com.exactpro.th2.common.grpc.Event toProto(@NotNull EventID parentId) throws JsonProcessingException {
        return toProto(
                requireNonNullParentId(parentId),
                requireNonBlankBookName(parentId.getBookName()),
                parentId.getScope()
        );
    }

    public com.exactpro.th2.common.grpc.Event toProto(@NotNull String bookName) throws JsonProcessingException {
        return toProto(
                null,
                requireNonBlankBookName(bookName),
                null
        );
    }

    public com.exactpro.th2.common.grpc.Event toProto(
            @NotNull String bookName,
            @NotNull String scope
    ) throws JsonProcessingException {
        return toProto(
                null,
                requireNonBlankBookName(bookName),
                requireNonBlankScope(scope)
        );
    }

    private com.exactpro.th2.common.grpc.Event toProto(
            @Nullable EventID parentId,
            @NotNull String bookName,
            @Nullable String scope
    ) throws JsonProcessingException {
        if (endTimestamp == null) {
            endTimestamp();
        }
        StringBuilder nameBuilder = new StringBuilder(defaultIfBlank(name, UNKNOWN_EVENT_NAME));
        if (isNotBlank(description)) {
            nameBuilder.append(" - ")
                    .append(description);
        }
        EventID eventId = isBlank(scope)
                ? toEventID(bookName, id)
                : toEventID(bookName, scope, id);
        var eventBuilder = com.exactpro.th2.common.grpc.Event.newBuilder()
                .setId(eventId)
                .setName(nameBuilder.toString())
                .setType(defaultIfBlank(type, UNKNOWN_EVENT_TYPE))
                .setStartTimestamp(toTimestamp(startTimestamp))
                .setEndTimestamp(toTimestamp(endTimestamp))
                .setStatus(getAggregatedStatus().eventStatus)
                .setBody(ByteString.copyFrom(buildBody()));
        if (parentId != null) {
            eventBuilder.setParentId(parentId);
        }
        for (MessageID messageID : attachedMessageIDS) {
            eventBuilder.addAttachedMessageIds(messageID);
        }
        return eventBuilder.build();
    }

    public EventBatch toBatchProto(@NotNull EventID parentId) throws JsonProcessingException {
        return toBatchProto(
                requireNonNullParentId(parentId),
                requireNonBlankBookName(parentId.getBookName()),
                parentId.getScope()
        );
    }

    public EventBatch toBatchProto(@NotNull String bookName) throws JsonProcessingException {
        return toBatchProto(
                null,
                requireNonBlankBookName(bookName),
                null
        );
    }

    public EventBatch toBatchProto(
            @NotNull String bookName,
            @NotNull String scope
    ) throws JsonProcessingException {
        return toBatchProto(
                null,
                requireNonBlankBookName(bookName),
                requireNonBlankScope(scope)
        );
    }

    private EventBatch toBatchProto(
            @Nullable EventID parentId,
            @NotNull String bookName,
            @Nullable String scope
    ) throws JsonProcessingException {
        List<com.exactpro.th2.common.grpc.Event> events = toListProto(new ArrayList<>(), parentId, bookName, scope);

        EventBatch.Builder builder = EventBatch.newBuilder()
            .addAllEvents(events);

        if (parentId != null && events.size() != 1) {
            builder.setParentEventId(parentId);
        }

        return builder.build();
    }

    public List<EventBatch> toBatchesProtoWithLimit(
            int maxEventBatchContentSize,
            @NotNull EventID parentId
    ) throws JsonProcessingException {
        return toBatchesProtoWithLimit(
                maxEventBatchContentSize,
                requireNonNullParentId(parentId),
                requireNonBlankBookName(parentId.getBookName()),
                parentId.getScope()
        );
    }

    public List<EventBatch> toBatchesProtoWithLimit(
            int maxEventBatchContentSize,
            @NotNull String bookName
    ) throws JsonProcessingException {
        return toBatchesProtoWithLimit(
                maxEventBatchContentSize,
                null,
                requireNonBlankBookName(bookName),
                null
        );
    }

    public List<EventBatch> toBatchesProtoWithLimit(
            int maxEventBatchContentSize,
            @NotNull String bookName,
            @NotNull String scope
    ) throws JsonProcessingException {
        return toBatchesProtoWithLimit(
                maxEventBatchContentSize,
                null,
                requireNonBlankBookName(bookName),
                requireNonBlankScope(scope)
        );
    }

    /**
     * Converts the event with all child events to a sequence of the th2 events then organizes them into batches according to event tree structure and value of max event batch content size argent.
     * Splitting to batch executes by principles:
     * * Events with children are put into distinct batches because events can't be a child of an event from another batch.
     * * Events without children are collected into batches according to the max size. For example, little child events can be put into one batch; big child events can be put into separate batches.
     * @param maxEventBatchContentSize - the maximum size of useful content in one batch which is calculated as the sum of the size of all event bodies in the batch
     * @param parentId                 - reference to parent event for the current event tree. It may be null if the current event is root, in this case {@code bookName} is required.
     * @param bookName                 - book name for the current event tree. It may not be null.
     * @param scope                    - scope for the current event tree. It may be null.
     */
    private List<EventBatch> toBatchesProtoWithLimit(
            int maxEventBatchContentSize, 
            @Nullable EventID parentId,
            @NotNull String bookName,
            @Nullable String scope
    ) throws JsonProcessingException {
        if (maxEventBatchContentSize <= 0) {
            throw new IllegalArgumentException("'maxEventBatchContentSize' should be greater than zero, actual: " + maxEventBatchContentSize);
        }

        List<com.exactpro.th2.common.grpc.Event> events = toListProto(new ArrayList<>(), parentId, bookName, scope);

        List<EventBatch> result = new ArrayList<>();
        Map<EventID, List<com.exactpro.th2.common.grpc.Event>> eventGroups = events.stream().collect(Collectors.groupingBy(com.exactpro.th2.common.grpc.Event::getParentId));
        batch(maxEventBatchContentSize, result, eventGroups, parentId);
        return result;
    }

    public List<EventBatch> toListBatchProto(@NotNull EventID parentId) throws JsonProcessingException {
        return toListBatchProto(
                requireNonNullParentId(parentId),
                requireNonBlankBookName(parentId.getBookName()),
                parentId.getScope()
        );
    }

    public List<EventBatch> toListBatchProto(@NotNull String bookName) throws JsonProcessingException {
        return toListBatchProto(
                null,
                requireNonBlankBookName(bookName),
                null
        );
    }

    public List<EventBatch> toListBatchProto(
            @NotNull String bookName,
            @NotNull String scope
    ) throws JsonProcessingException {
        return toListBatchProto(
                null,
                requireNonBlankBookName(bookName),
                requireNonBlankScope(scope)
        );
    }

    /**
     * Converts the event with all child events to a sequence of the th2 events then organizes them into batches according to event tree structure.
     * Splitting to batch executes by principles:
     * * Events with children are put into distinct batches because events can't be a child of an event from another batch.
     * * Events without children are collected into batches.
     * @param parentId - reference to parent event for the current event tree. It may be null if the current event is root, in this case {@code bookName} is required.
     * @param bookName - book name for the current event tree. It may not be null.
     * @param scope    - scope for the current event tree. It may be null.
     */
    private List<EventBatch> toListBatchProto(
            @Nullable EventID parentId,
            @NotNull String bookName,
            @Nullable String scope
    ) throws JsonProcessingException {
        return toBatchesProtoWithLimit(Integer.MAX_VALUE, parentId, bookName, scope);
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

    protected byte[] buildBody() throws JsonProcessingException {
        return OBJECT_MAPPER.get().writeValueAsBytes(body);
    }

    protected String formatStateException(String fieldName, Object value) {
        return fieldName + " in event '" + id + "' already sed with value '" + value + '\'';
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

    private void batch(int maxEventBatchContentSize, List<EventBatch> result, Map<EventID, List<com.exactpro.th2.common.grpc.Event>> eventGroups, @Nullable EventID eventID) throws JsonProcessingException {
        eventID = requireNonNullElse(eventID, DEFAULT_EVENT_ID);
        EventBatch.Builder builder = setParentId(EventBatch.newBuilder(), eventID);

        List<com.exactpro.th2.common.grpc.Event> events = requireNonNull(eventGroups.get(eventID),
                eventID == DEFAULT_EVENT_ID
                        ? "Neither of events is root event"
                        : "Neither of events refers to " + shortDebugString(eventID));

        for (var protoEvent : events) {
            var checkedProtoEvent = checkAndRebuild(maxEventBatchContentSize, protoEvent);

            LOGGER.trace("Process {} {}", checkedProtoEvent.getName(), checkedProtoEvent.getType());
            if(eventGroups.containsKey(checkedProtoEvent.getId())) {
                result.add(checkAndBuild(maxEventBatchContentSize, EventBatch.newBuilder()
                        .addEvents(checkedProtoEvent)));

                batch(maxEventBatchContentSize, result, eventGroups, checkedProtoEvent.getId());
            } else {
                if(builder.getEventsCount() > 0
                        && getContentSize(builder) + getContentSize(checkedProtoEvent) > maxEventBatchContentSize) {
                    result.add(checkAndBuild(maxEventBatchContentSize, builder));
                    builder = setParentId(EventBatch.newBuilder(), eventID);
                }
                builder.addEvents(checkedProtoEvent);
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
        int contentSize = getContentSize(event);
        if (contentSize > maxEventBatchContentSize) {
            return com.exactpro.th2.common.grpc.Event.newBuilder(event)
                    .setStatus(EventStatus.FAILED)
                    .setBody(ByteString.copyFrom(OBJECT_MAPPER.get().writeValueAsBytes(
                            Collections.singletonList(createMessageBean("Event " + shortDebugString(event.getId()) + " exceeds max size, max " + maxEventBatchContentSize + ", actual " + contentSize)))))
                    .build();
        }
        return event;
    }

    private static EventBatch.Builder setParentId(EventBatch.Builder builder, @NotNull EventID parentId) {
        if (parentId != DEFAULT_EVENT_ID) {
            builder.setParentEventId(parentId);
        }
        return builder;
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
