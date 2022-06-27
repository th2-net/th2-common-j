# th2 common library (Java) (3.40.0)

## Usage

Firstly, you must import CommonFactory class:
```
import com.exactpro.th2.common.schema.factory.CommonFactory
```
Then you will create an instance of imported class, by choosing one of the following options:
1. Create factory with configs from the default path (`/var/th2/config/*`):
    ```
    var factory = CommonFactory();
    ```
1. Create factory with configs from the specified file paths:
    ```
    var factory = CommonFactory(rabbitMQ, routerMQ, routerGRPC, cradle, custom, prometheus, dictionariesDir);
    ```
1. Create factory with configs from the specified arguments:
    ```
    var factory = CommonFactory.createFromArguments(args);
    ```
    You can use one of the following groups of arguments. Arguments from different
    groups cannot be used together. 
    
    The first group:
    * --rabbitConfiguration - path to json file with RabbitMQ configuration
    * --messageRouterConfiguration - path to json file with configuration for MessageRouter
    * --grpcRouterConfiguration - path to json file with configuration for GrpcRouter
    * --cradleConfiguration - path to json file with configuration for Cradle
    * --customConfiguration - path to json file with custom configuration
    * --dictionariesDir - path to the directory which contains files with the encoded dictionaries
    * --prometheusConfiguration - path to json file with configuration for prometheus metrics server
    * --boxConfiguration - path to json file with boxes configuration and information
    * -c/--configs - folder with json files for schemas configurations with special names:
    1. rabbitMq.json - configuration for RabbitMQ
    2. mq.json - configuration for MessageRouter
    3. grpc.json - configuration for GrpcRouter
    4. cradle.json - configuration for cradle
    5. custom.json - custom configuration
    
    The second group:
    * --namespace - the namespace in Kubernetes to search config maps
    * --boxName - the name of the target th2 box placed in the specified Kubernetes namespace
    * --contextName - the context name to search connect parameters in Kube config
    * --dictionaries - the mapping between a dictionary in infra schema and a dictionary type in the format: 
      `--dictionaries <dictionary name>=<dictionary type >[ <dictionary name>=<dictionary type >]`. 
      It can be useful when you required dictionaries to start a specific box. 
    
    Their usage is disclosed further.
    
1. Create factory with a namespace in Kubernetes and the name of the target th2 box from Kubernetes:
    ```
    var factory = CommonFactory.createFromKubernetes(namespace, boxName);
    ```
    It also can be called by using `createFromArguments(args)` with arguments `--namespace` and `--boxName`.
1. Create factory with a namespace in Kubernetes, the name of the target th2 box from Kubernetes and the name of context to choose from Kube config: 
    ```
    var factory = CommonFactory.createFromKubernetes(namespace, boxName, contextName);
    ```
    It also can be called by using `createFromArguments(args)` with arguments `--namespace`, `--boxName` and `--contextName`. 
    ContextName parameter is `@Nullable`; if it is set to null, the current context will not be changed.

### Configuration formats

The `CommonFactory` reads a RabbitMQ configuration from the rabbitMQ.json file.
* host - the required setting defines the RabbitMQ host.
* vHost - the required setting defines the virtual host that will be used for connecting to RabbitMQ. 
  Please see more details about the virtual host in RabbitMQ via [link](https://www.rabbitmq.com/vhosts.html)
* port - the required setting defines the RabbitMQ port.
* username - the required setting defines the RabbitMQ username. 
  The user must have permission to publish messages via routing keys and subscribe to message queues.
* password - the required setting defines the password that will be used for connecting to RabbitMQ.
* exchangeName - the required setting defines the exchange that will be used for sending/subscribing operation in MQ routers. 
  Please see more details about the exchanges in RabbitMQ via [link](https://www.rabbitmq.com/tutorials/amqp-concepts.html#exchanges)
* connectionTimeout - the connection TCP establishment timeout in milliseconds with its default value set to 60000. Use zero for infinite waiting.
* connectionCloseTimeout - the timeout in milliseconds for completing all the close-related operations, use -1 for infinity, the default value is set to 10000.
* maxRecoveryAttempts - this option defines the number of reconnection attempts to RabbitMQ, with its default value set to 5. 
  The `th2_readiness` probe is set to false and publishers are blocked after a lost connection to RabbitMQ. The `th2_readiness` probe is reverted to true if the connection will be recovered during specified attempts otherwise the `th2_liveness` probe will be set to false.
* minConnectionRecoveryTimeout - this option defines a minimal interval in milliseconds between reconnect attempts, with its default value set to 10000. Common factory increases the reconnect interval values from minConnectionRecoveryTimeout to maxConnectionRecoveryTimeout. 
* maxConnectionRecoveryTimeout - this option defines a maximum interval in milliseconds between reconnect attempts, with its default value set to 60000. Common factory increases the reconnect interval values from minConnectionRecoveryTimeout to maxConnectionRecoveryTimeout.
* prefetchCount - this option is the maximum number of messages that the server will deliver, with its value set to 0 if unlimited, the default value is set to 10.
* messageRecursionLimit - an integer number denotes how deep nested protobuf message might be, default = 100

```json
{
  "host": "<host>",
  "vHost": "<virtual host>",
  "port": 5672,
  "username": "<user name>",
  "password": "<password>",
  "exchangeName": "<exchange name>",
  "connectionTimeout": 60000,
  "connectionCloseTimeout": 10000,
  "maxRecoveryAttempts": 5,
  "minConnectionRecoveryTimeout": 10000,
  "maxConnectionRecoveryTimeout": 60000,
  "prefetchCount": 10,
  "messageRecursionLimit": 100
}
```

The `CommonFactory` reads a message's router configuration from the `mq.json` file.
* queues - the required settings defines all pins for an application
    * name - routing key in RabbitMQ for sending
    * queue - queue's name in RabbitMQ for subscribe
    * exchange - exchange in RabbitMQ
    * attributes - pin's attribute for filtering. Default attributes:
        * first
        * second
        * subscribe
        * publish        
        * parsed
        * raw
        * store
        * event
      
    * filters - pin's message's filters
        * metadata - a metadata filters
        * message - a message's fields filters
    
Filters format: 
* fieldName - a field's name
* expectedValue - expected field's value (not used for all operations)
* operation - operation's type
    * `EQUAL` - the filter passes if the field is equal to the exact value
    * `NOT_EQUAL` - the filter passes if the field does not equal the exact value
    * `EMPTY` - the filter passes if the field is empty
    * `NOT_EMPTY` - the filter passes if the field is not empty
    * `WILDCARD` - filters the field by wildcard expression

```json
{
  "queues": {
    "pin1": {
      "name": "routing_key_1",
      "queue": "queue_1",
      "exchange": "exchange",
      "attributes": [
        "publish",
        "subscribe"
      ],
      "filters": {
        "metadata": [
          {
            "fieldName": "session-alias",
            "expectedValue": "connection1",
            "operation": "EQUAL"
          }
        ],
        "message": [
          {
            "fieldName": "checkField",
            "expectedValue": "wil?card*",
            "operation": "WILDCARD"
          }
        ]
      }
    }
  }
}
```

The `CommonFactory` reads a Cradle configuration from the cradle.json file.
* dataCenter - the required setting defines the data center in the Cassandra cluster.
* host - the required setting defines the Cassandra host.
* port - the required setting defines the Cassandra port.
* keyspace - the required setting defines the keyspace (top-level database object) in the Cassandra data center.
* username - the required setting defines the Cassandra username. The user must have permission to write data using a specified keyspace.
* password - the required setting defines the password that will be used for connecting to Cassandra.
* cradleInstanceName - this option defines a special identifier that divides data within one keyspace with infra set as the default value.
* cradleMaxEventBatchSize - this option defines the maximum event batch size in bytes with its default value set to 1048576.
* cradleMaxMessageBatchSize - this option defines the maximum message batch size in bytes with its default value set to 1048576.
* timeout - this option defines connection timeout in milliseconds. If set to 0 or omitted, the default value of 5000 is used.
* pageSize - this option defines the size of the result set to fetch at a time. If set to 0 or omitted, the default value of 5000 is used.
* prepareStorage - enables database schema initialization if Cradle is used. By default, it has value of `false`

```json
{
  "dataCenter": "<datacenter>",
  "host": "<host>",
  "port": 9042,
  "keyspace": "<keyspace>",
  "username": "<username>",
  "password": "<password>",
  "cradleInstanceName": "<cradle instance name>",
  "cradleMaxEventBatchSize": 1048576,
  "cradleMaxMessageBatchSize": 1048576,
  "timeout": 5000,
  "pageSize": 5000,
  "prepareStorage": false
}
```

### Requirements for creating factory with Kubernetes

1. It is necessary to have Kubernetes configuration written in ~/.kube/config. See more on kubectl configuration [here](https://kubernetes.io/docs/tasks/access-application-cluster/configure-access-multiple-clusters/).

1. Also note that `generated_configs` directory will be created to store `.json` files with configs from Kubernetes. Those files are overridden when `CommonFactory.createFromKubernetes(namespace, boxName)` and `CommonFactory.createFromKubernetes(namespace, boxName, contextName)` are invoked again. 

1. User needs to have authentication with service account token that has the necessary access to read CRs and secrets from the specified namespace. 

After that you can receive various Routers through factory properties:
```
var messageRouter = factory.getMessageRouterParsedBatch();
var rawRouter = factory.getMessageRouterRawBatch();
var eventRouter = factory.getEventBatchRouter();
```

`messageRouter` is working with `MessageBatch` <br>
`rawRouter` is working with `RawMessageBatch` <br>
`eventRouter` is working with `EventBatch`

Please refer to [th2-grpc-common](https://github.com/th2-net/th2-grpc-common/blob/master/src/main/proto/th2_grpc_common/common.proto "common.proto") for further details.

With the router created, you can subscribe to pins (by specifying the callback function) or to send data that the router works with:
```
router.subscribe(callback)  # subscribe to only one pin 
router.subscribeAll(callback)  # subscribe to one or several pins
router.send(message)  # send to only one pin
router.sendAll(message)  # send to one or several pins
```
You can perform these actions by providing pin attributes in addition to the default ones.
```
router.subscribe(callback, attrs...)  # subscribe to only one pin
router.subscribeAll(callback, attrs...)  # subscribe to one or several pins
router.send(message, attrs...)  # send to only one pin
router.sendAll(message, attrs...)  # send to one or several pins
```
The default attributes are:
- `message_parsed_batch_router`
    - Subscribe: `subscribe`, `parsed`
    - Send: `publish`, `parsed`
- `message_raw_batch_router`
    - Subscribe: `subscribe`, `raw`
    - Send: `publish`, `raw`
- `event_batch_router`
    - Subscribe: `subscribe`, `event`
    - Send: `publish`, `event`

This library allows you to:

## Export common metrics to Prometheus
  
It can be performed by the following utility methods in CommonMetrics class

* `setLiveness` - sets "liveness" metric of a service (exported as `th2_liveness` gauge)
* `setReadiness` - sets "readiness" metric of a service (exported as th2_readiness gauge)

NOTES:

* in order for the metrics to be exported, you also will need to create an instance of CommonFactory
* common JVM metrics will also be exported alongside common service metrics
* some metric labels are enumerations (`th2_type`: `MESSAGE_GROUP`, `EVENT`, `<customTag>`;`message_type`: `RAW_MESSAGE`, `MESSAGE`)

RABBITMQ METRICS:
* th2_rabbitmq_message_size_publish_bytes (`th2_pin`, `th2_type`, `exchange`, `routing_key`): number of published message bytes to RabbitMQ. The message is meant for any data transferred via RabbitMQ, for example, th2 batch message or event or custom content
* th2_rabbitmq_message_publish_total (`th2_pin`, `th2_type`, `exchange`, `routing_key`): quantity of published messages to RabbitMQ. The message is meant for any data transferred via RabbitMQ, for example, th2 batch message or event or custom content
* th2_rabbitmq_message_size_subscribe_bytes (`th2_pin`, `th2_type`, `queue`): number of bytes received from RabbitMQ, it includes bytes of messages dropped after filters. For information about the number of dropped messages, please refer to 'th2_message_dropped_subscribe_total' and 'th2_message_group_dropped_subscribe_total'. The message is meant for any data transferred via RabbitMQ, for example, th2 batch message or event or custom content
* th2_rabbitmq_message_process_duration_seconds (`th2_pin`, `th2_type`, `queue`): time of message processing during subscription from RabbitMQ in seconds. The message is meant for any data transferred via RabbitMQ, for example, th2 batch message or event or custom content

GRPC METRICS:
* th2_grpc_invoke_call_total (`th2_pin`, `service_name`, `service_method`): total number of calling particular gRPC method
* th2_grpc_invoke_call_request_bytes (`th2_pin`, `service_name`, `service_method`): number of bytes sent to particular gRPC call
* th2_grpc_invoke_call_response_bytes (`th2_pin`, `service_name`, `service_method`): number of bytes sent to particular gRPC call
* th2_grpc_receive_call_total (`th2_pin`, `service_name`, `service_method`): total number of consuming particular gRPC method
* th2_grpc_receive_call_request_bytes (`th2_pin`, `service_name`, `service_method`): number of bytes received from particular gRPC call
* th2_grpc_receive_call_response_bytes (`th2_pin`, `service_name`, `service_method`): number of bytes sent to particular gRPC call

MESSAGES METRICS:
* th2_message_publish_total (`th2_pin`, `session_alias`, `direction`, `message_type`): quantity of published raw or parsed messages
* th2_message_subscribe_total (`th2_pin`, `session_alias`, `direction`, `message_type`): quantity of received raw or parsed messages, includes dropped after filters. For information about the number of dropped messages, please refer to 'th2_message_dropped_subscribe_total'
* th2_message_dropped_publish_total (`th2_pin`, `session_alias`, `direction`, `message_type`): quantity of published raw or parsed messages dropped after filters
* th2_message_dropped_subscribe_total (`th2_pin`, `session_alias`, `direction`, `message_type`): quantity of received raw or parsed messages dropped after filters
* th2_message_group_publish_total (`th2_pin`, `session_alias`, `direction`): quantity of published message groups 
* th2_message_group_subscribe_total (`th2_pin`, `session_alias`, `direction`): quantity of received message groups, includes dropped after filters. For information about the number of dropped messages, please refer to 'th2_message_group_dropped_subscribe_total'
* th2_message_group_dropped_publish_total (`th2_pin`, `session_alias`, `direction`): quantity of published message groups dropped after filters
* th2_message_group_dropped_subscribe_total (`th2_pin`, `session_alias`, `direction`): quantity of received message groups dropped after filters
* th2_message_group_sequence_publish (`th2_pin`, `session_alias`, `direction`): last published sequence
* th2_message_group_sequence_subscribe (`th2_pin`, `session_alias`, `direction`): last received sequence

EVENTS METRICS:

* th2_event_publish_total (`th2_pin`): quantity of published events
* th2_event_subscribe_total (`th2_pin`): quantity of received events

### Test extensions:

To be able to use test extensions please fill build.gradle as in example below: 
```groovy
plugins {
    id 'java-test-fixtures'
}

dependencies {
    testImplementation testFixtures("com.exactpro.th2:common:3.39.0")
}
```

## Release notes

### 3.40.0

+ Started using cradle 3.2, version with grouped messages

### 3.39.2

+ Fixed:
  + The message is not confirmed when it was filtered

### 3.39.1

+ Fixed:
  + cradle version updated to 3.1.2. Unicode character sizes are calculated properly during serialization and tests are added for it also.

### 3.39.0

+ gRPC metrics added

### 3.38.0

+ Migration to log4j2. **th2-bom must be updated to _3.2.0_ or higher**
  + There is backward compatibility with the log4j format. 
  + If both configurations are available, log4j2 is preferable.

### 3.37.2

+ Corrected logging for configuration files

### 3.37.1

+ Fixed:
  + When creating the `CommonFactory` from k8s the logging configuration wouldn't be downloaded

### 3.37.0

+ Added support for gRPC pins filters

### 3.36.0

* Cradle version was updated from `2.20.2` to `3.1.1`.
  **Please, note, that migration is required for `3.1.1` usage**.
* New parameter `prepareStorage` is added to the `cradle_manager.json`.
  It allows enabling/disabling Cradle schema initialization.

### 3.35.0

* Included dependency to the io.prometheus:simpleclient_log4j:0.9.0

### 3.34.0

+ Added ability to read dictionaries by aliases and as group of all available aliases
+ New methods for api: loadDictionary(String), getDictionaryAliases(), loadSingleDictionary()

### 3.33.0

#### Added:

+ Methods for subscription with manual acknowledgement
  (if the **prefetch count** is requested and no messages are acknowledged the reading from the queue will be suspended).
  Please, note that only one subscriber with manual acknowledgement can be subscribed to a queue

### 3.32.1

+ Fixed: gRPC router didn't shut down underlying Netty's EventLoopGroup and ExecutorService 

### 3.32.0

+ Added new test utils for assertion of **AnyMessage** or **Groups** of messages

### 3.31.6

+ Update Cradle version from 2.20.0 to [2.20.2](https://github.com/th2-net/cradleapi/releases/tag/2.20.2)

### 3.31.4

+ Ignore unknown fields in `box.json` and `prometheus.json`

### 3.31.3

+ Add support for `null_value` filter during conversion to table

### 3.31.2

+ Update grpc-common from 3.8.0 to 3.9.0

### 3.31.1
+ Feature as test assertion methods for messages from fixtures

### 3.31.0

+ Fix printing of empty MessageGroupBatch in debug logs of MessageGroupBatch router
+ Print message ids of MessageGroupBatch in debug logs of MessageGroupBatch router

### 3.30.0

+ Added util methods from store-common to use in estore/mstore

### 3.29.2

+ Do not publish messages if the whole batch was filtered

### 3.29.1

+ Fix problem with filtering by `message_type` in MessageGroupBatch router

### 3.29.0

+ Update Cradle version from `2.13.0` to `2.20.0`

### 3.28.0

+ Added new parameter `hint` for `VerificationEntry`

### 3.27.0

+ Added new abstract router `AbstractRabbitRouter`, removed `MessageQueue` hierarchy
+ Parsed/raw routers work with `MessageGroupBatch` router
+ Added new metrics and removed old

### 3.26.5
+ Migrated `grpc-common` version from `3.7.0` to `3.8.0`
    + Added `time_precision` and `decimal_precision` parameters to `RootComparisonSettings`

### 3.26.4
+ Migrated `grpc-common` version from `3.6.0` to `3.7.0`
  + Added `check_repeating_group_order` parameter to `RootComparisonSettings` message

### 3.26.3
+ Migrated `grpc-common` version from `3.5.0` to `3.6.0`
  + Added `description` parameter to `RootMessageFilter` message

### 3.26.2
+ Fix `SimpleFilter` and `ValueFilter` treeTable convertation

### 3.26.1
+ Add `SimpleList` display to `TreeTableEntry`;

### 3.26.0
+ Update the grpc-common version to 3.5.0
    + Added `SimpleList` to `SimpleFilter`

### 3.25.2

+ Added support for converting `SimpleList` to readable payload body

### 3.25.1

#### Changed:
+ Extension method for `MessageRouter<EventBatch>` now send the event to all pins that satisfy the requested attributes set

### 3.25.0
+ Added util to convert RootMessageFilter into readable collection of payload bodies

### 3.24.2
+ Fixed `messageRecursionLimit` - still was not applied to all kind of RabbitMQ subscribers

### 3.24.1
+ Fixed `messageRecursionLimit` setting for all kind of RabbitMQ subscribers

### 3.24.0
+ Added setting `messageRecursionLimit`(the default value is set to 100) to RabbitMQ configuration that denotes how deep nested protobuf messages might be.

### 3.23.0
+ Update the grpc-common version to 3.4.0
    + Added `IN`, `LIKE`, `MORE`, `LESS`, `WILDCARD` FilterOperation and their negative versions

### 3.22.0

+ Change class for filtering in router from `FilterOperation` to `FieldFilterOperation`
+ Added `WILDCARD` filter operation, which filter a field by wildcard expression.

### 3.21.2
+ Fixed grpc server start.

### 3.21.1
+ Update the grpc-common version to 3.3.0:
  + Added information about message timestamp into the checkpoint

### 3.21.0
+ Added classes for management metrics.
+ Added ability for resubscribe on canceled subscriber.

### 3.20.0

+ Extended filter's format
    + Add multiply filters support

### 3.19.0

+ Added the Event.toListBatchProto method to covert instance of the Event class to list of event batches.

### 3.18.2

#### Changed:

+ Fix possible NPE when adding the `Exception` to the event with `null` message
+ Correct exception messages

### 3.18.1

#### Changed:

+ The `toMetadataFilter` returns `null` the original metadata has nothing to compare
+ The `toRootMessageFilter` does not add the `MetadataFilter` if the message's metadata has nothing to compare

### 3.18.0

#### Changed:

+ Update Cradle version from `2.9.1` to `2.13.0`

### 3.17.0
+ Extended message utility class
  + Added the toRootMessageFilter method to convert message to root message filter

### 3.16.5
+ Update `th2-grpc-common` and `th2-grpc-service-generator` versions to `3.2.0` and `3.1.12` respectively

### 3.16.4

+ Disable waiting for connection recovery when closing the `SubscribeMonitor`

### 3.16.3

+ Change the way that channels are stored (they are mapped to the pin instead of to the thread).
  It might increase the average number of channels used by the box, but it also limits the max number of channels to the number of pins

### 3.16.2

+ Restore backward compatibility for MessageConverter factory methods.
  **NOTE: one of the methods was not restored and update to this version might require manual update for your code**.
  The old methods without `toTraceString` supplier will be removed in the future
+ Fixed configuration for gRPC server.
  + Added the property `workers`, which changes the count of gRPC server's threads
  
### 3.16.0

+ Extended Utility classes
  + Added the toTreeTable method to convert message/message filter to event data
  + Added the Event.exception method to include an exception and optionally all the causes to the body data as a series of messages

### 3.15.0

+ add `session alias` and `direction` labels to incoming metrics
+ rework logging for incoming and outgoing messages

### 3.14.0

+ add `toProtoEvent(EventID)` and `toProtoEvents(EventID)` overloads into the Event class

### 3.13.6

+ resets embedded `log4j` configuration before configuring it from a file

### 3.13.5 

+ fixed a bug with message filtering by `message_type`

### 3.13.4

+ migration to Sonatype

### 3.13.3

+ additional fixes for dictionary reading

### 3.13.2

+ add backward compatibility for reading dictionaries

### 3.13.1

+ removed gRPC event loop handling 
+ fixed dictionary reading

### 3.13.0 
+ reads dictionaries from the /var/th2/config/dictionary folder.
+ uses mq_router, grpc_router, cradle_manager optional JSON configs from the /var/th2/config folder

### 3.11.0

+ tries to load log4j.properties files from sources in order: '/var/th2/config', '/home/etc', configured path via cmd, default configuration

### 3.6.0

+ update Cradle version. Introduce async API for storing events

### 3.0.1

+ metrics related to time measurement of an incoming message handling (Raw / Parsed / Event) migrated to Prometheus [histogram](https://prometheus.io/docs/concepts/metric_types/#histogram)
