# th2 common library (Java) (5.10.1)

## Usage

Firstly, you must import CommonFactory class:

```
import com.exactpro.th2.common.schema.factory.CommonFactory
```

Then you will create an instance of imported class, by choosing one of the following options:

1. Create factory with configs from the `th2.common.configuration-directory` environment variable or default path (`/var/th2/config/*`):
    ```
    var factory = CommonFactory();
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
    * -c/--configs - folder with json files for schemas configurations with special names.
      If you doesn't specify -c/--configs common factory uses configs from the `th2.common.configuration-directory` environment variable or default path (`/var/th2/config/*`)

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
1. Create factory with a namespace in Kubernetes, the name of the target th2 box from Kubernetes and the name of context
   to choose from Kube config:
    ```
    var factory = CommonFactory.createFromKubernetes(namespace, boxName, contextName);
    ```
   It also can be called by using `createFromArguments(args)` with arguments `--namespace`, `--boxName`
   and `--contextName`.
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
* exchangeName - the required setting defines the exchange that will be used for sending/subscribing operation in MQ
  routers.
  Please see more details about the exchanges in RabbitMQ
  via [link](https://www.rabbitmq.com/tutorials/amqp-concepts.html#exchanges)
* connectionTimeout - the connection TCP establishment timeout in milliseconds with its default value set to 60000. Use
  zero for infinite waiting.
* connectionCloseTimeout - the timeout in milliseconds for completing all the close-related operations, use -1 for
  infinity, the default value is set to 10000.
* maxRecoveryAttempts - this option defines the number of reconnection attempts to RabbitMQ, with its default value set
  to 5.
  The `th2_readiness` probe is set to false and publishers are blocked after a lost connection to RabbitMQ.
  The `th2_readiness` probe is reverted to true if the connection will be recovered during specified attempts otherwise
  the `th2_liveness` probe will be set to false.
* minConnectionRecoveryTimeout - this option defines a minimal interval in milliseconds between reconnect attempts, with
  its default value set to 10000. Common factory increases the reconnect interval values from
  minConnectionRecoveryTimeout to maxConnectionRecoveryTimeout.
* maxConnectionRecoveryTimeout - this option defines a maximum interval in milliseconds between reconnect attempts, with
  its default value set to 60000. Common factory increases the reconnect interval values from
  minConnectionRecoveryTimeout to maxConnectionRecoveryTimeout.
* retryTimeDeviationPercent - specifies random deviation to delay interval duration. Default value is 10 percents.
  E.g. if delay interval is 30 seconds and `retryTimeDeviationPercent` is 10 percents the actual duration of interval
  will be random value from 27 to 33 seconds.
* prefetchCount - this option is the maximum number of messages that the server will deliver, with its value set to 0 if
  unlimited, the default value is set to 10.
* messageRecursionLimit - an integer number denotes how deep the nested protobuf message might be, set by default 100

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
  "retryTimeDeviationPercent": 10,
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

* globalNotification - notification exchange in RabbitMQ
    * exchange - `global-notification` by default

Filters format:

* fieldName - a field's name
* expectedValue - expected field's value (not used for all operations)
* operation - operation's type
    * `EQUAL` - the filter passes if the field is equal to the exact value
    * `NOT_EQUAL` - the filter passes if the field is not equal to the exact value
    * `EMPTY` - the filter passes if the field is empty
    * `NOT_EMPTY` - the filter passes if the field is not empty
    * `WILDCARD` - filters the field by wildcard expression
    * `NOT_WILDCARD` - filters the field which isn't matched by wildcard expression

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
    },
    "globalNotification": {
      "exchange": "global-notification"
    }
  }
}
```

The `CommonFactory` reads a gRPC router configuration from the `grpc_router.json` file.

* enableSizeMeasuring - this option enables the gRPC message size measuring. Please note the feature decreases gRPC
  throughput. Default value is false.
* keepAliveInterval - number of seconds between keep alive messages. Default value is 60
* maxMessageSize - this option enables endpoint message filtering based on message size (message with size larger than
  option value will be skipped). By default, it has a value of `4 MB`. The unit of measurement of the value is number of
  bytes.
* retryConfiguration - this settings aria is responsible for how a component executes gRPC retries before gives up with exception.
    Component executes request attempts with growing timeout between them until success or attempts over
  * maxAttempts - number of attempts before give up
  * minMethodRetriesTimeout - minimal timeout between retry in milliseconds
  * maxMethodRetriesTimeout - maximum timeout between retry in milliseconds

```json
{
  "enableSizeMeasuring": false,
  "keepAliveInterval": 60,
  "maxMessageSize": 4194304,
  "retryConfiguration": {
    "maxAttempts": 60,
    "minMethodRetriesTimeout": 100,
    "maxMethodRetriesTimeout": 120000
  }
}
```

The `CommonFactory` reads a gRPC configuration from the `grpc.json` file.

* services - grpc services configurations
* server - grpc server configuration
* endpoint - grpc endpoint configuration

```json
{
  "services": {
    "test": {
      "endpoints": {
        "endpoint": {
          "host": "host",
          "port": 12345,
          "attributes": [
            "test_attr"
          ]
        }
      },
      "service-class": "com.exactpro.th2.common.schema.grpc.configuration.GrpcConfiguration",
      "strategy": {
        "endpoints": [
          "endpoint"
        ],
        "name": "robin"
      }
    }
  },
  "server": {
    "host": "host123",
    "port": 1234,
    "workers": 58
  }
}
```

The `CommonFactory` reads a Cradle configuration from the cradle.json file.

* dataCenter - the required setting defines the data center in the Cassandra cluster.
* host - the required setting defines the Cassandra host.
* port - the required setting defines the Cassandra port.
* keyspace - the required setting defines the keyspace (top-level database object) in the Cassandra data center.
* username - the required setting defines the Cassandra username. The user must have permission to write data using a
  specified keyspace.
* password - the required setting defines the password that will be used for connecting to Cassandra.
* cradleMaxEventBatchSize - this option defines the maximum event batch size in bytes with its default value set to
  1048576.
* cradleMaxMessageBatchSize - this option defines the maximum message batch size in bytes with its default value set to
  1048576.
* timeout - this option defines connection timeout in milliseconds. If set to 0 or omitted, the default value of 5000 is
  used.
* pageSize - this option defines the size of the result set to fetch at a time. If set to 0 or omitted, the default
  value of 5000 is used.
* prepareStorage - enables database schema initialization if Cradle is used. By default, it has a value of `false`

```json
{
  "dataCenter": "<datacenter>",
  "host": "<host>",
  "port": 9042,
  "keyspace": "<keyspace>",
  "username": "<username>",
  "password": "<password>",
  "cradleMaxEventBatchSize": 1048576,
  "cradleMaxMessageBatchSize": 1048576,
  "timeout": 5000,
  "pageSize": 5000,
  "prepareStorage": false
}
```

### Requirements for creating factory with Kubernetes

1. It is necessary to have Kubernetes configuration written in ~/.kube/config. See more on kubectl
   configuration [here](https://kubernetes.io/docs/tasks/access-application-cluster/configure-access-multiple-clusters/).

1. Also note that `generated_configs` directory will be created to store `.json` files with configs from Kubernetes.
   Those files are overridden when `CommonFactory.createFromKubernetes(namespace, boxName)`
   and `CommonFactory.createFromKubernetes(namespace, boxName, contextName)` are invoked again.

1. Users need to have authentication with the service account token that has the necessary access to read CRs and
   secrets from the specified namespace.

After that you can receive various Routers through factory properties:

```

var protoMessageGroupRouter = factory.getMessageRouterMessageGroupBatch();
var protoMessageRouter = factory.getMessageRouterParsedBatch(); 
var protoRawRouter = factory.getMessageRouterRawBatch();
var protoEventRouter = factory.getEventBatchRouter();

var transportGroupRouter = factory.getTransportGroupBatchRouter();
```

`protoMessageGroupRouter` is working with `MessageGroupBatch` <br>
`protoMessageRouter` is working with `MessageBatch` <br>
`protoRawRouter` is working with `RawMessageBatch` <br>
`protoEventRouter` is working with `EventBatch` <br>

`transportGroupRouter` is working with `GroupBatch` <br>

Note: MessageRouterParsedBatch and MessageRouterRawBatch are not recommended to use because they are adapters for
MessageRouterMessageGroupBatch and execute additional repacking

Please refer
to [th2-grpc-common](https://github.com/th2-net/th2-grpc-common/blob/master/src/main/proto/th2_grpc_common/common.proto "common.proto")
for further details.

With the router created, you can subscribe to pins (by specifying the callback function) or to send data that the router
works with:

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

- `proto_message_group_batch_router`
    - Subscribe: `subscribe`
    - Send: `publish`
- `proto_message_parsed_batch_router`
    - Subscribe: `subscribe`, `parsed`
    - Send: `publish`, `parsed`
- `proto_message_raw_batch_router`
    - Subscribe: `subscribe`, `raw`
    - Send: `publish`, `raw`
- `proto_event_batch_router`
    - Subscribe: `subscribe`, `event`
    - Send: `publish`, `event`
- `transport_group_message_batch_router`
    - Subscribe: `subscribe`, `transport-group`
    - Send: `publish`, `transport-group`

This library allows you to:

## Routers

### gRPC router

This kind of router provides the ability for boxes to interact between each other via gRPC interface.

#### Server

gRPC router rises a gRPC server with
enabled [grpc-service-reflection](https://github.com/grpc/grpc-java/blob/master/documentation/server-reflection-tutorial.md#grpc-server-reflection-tutorial)
since the 3.38.0 version.
It means that the users can execute calls from the console or through scripts
via [grpcurl](https://github.com/fullstorydev/grpcurl#grpcurl) without gRPC schema (files with proto extensions
describes gRPC service structure)

## MQ router

This kind of router provides the ability for component to send / receive messages via RabbitMQ.
Router has several methods to subscribe and publish RabbitMQ messages steam (th2 use batches of messages or events as transport).
Supports recovery of subscriptions cancelled by RabbitMQ due to following errors: "delivery acknowledgement timed out" and "queue not found". 

#### Choice pin by attributes

Pin attributes are key mechanism to choose pin for action execution. Router search all pins which have full set of passed attributes.
For example, the pins: `first` [`publish`, `raw`, `custom_a` ], `second` [`publish`, `raw`, `custom_b` ].
* only the `first` pin will be chosen by attribut sets: [`custom_a`], [`custom_a`, `raw`], [`custom_a`, `publish`], [`publish`, `raw`, `custom_a` ]
* both pins will be chosen by attribut sets: [`raw`], [`publish`], [`publish`, `raw` ]

Router implementation and methods have predefined attributes. Result set of attributes for searching pin is union of  <router>, <method>, <passed> attributes.
Predefined attributes:
* `RabbitMessageGroupBatchRouter` hasn't got any predefined attributes
* `EventBatchRouter` has `evnet` attribute
* `TransportGroupBatchRouter` has `transport-group` attribute

* `send*` exclude `sendExclusive` methods have `publish` attribute
* `subscribe*` excluded `subscribeExclusive` methods have `subscribe` attribute

#### Choice publish pin 

Router chooses pins in two stages. At first select all pins matched by attributes than check passed message (batch) by
pin's filters and then send the whole message or its part via pins leaved after two steps.  

#### Choice subscribe pin 

Router chooses pins only by attributes. Pin's filters are used when message has been delivered and parsed. Registered lister doesn't receive message, or it parts failure check by pin's filter.  

### Restrictions:

Some methods have `All` suffix, it means that developer can publish or subscribe message via 1 or several pins otherwise via only 1 pin.
If number of passed check pins are different then required range, method throw an exception. 

Developer can register only one listener for each pin but one listener can handle messages from several pins.   

`TransportGroupBatchRouter` router doesn't split incoming or outgoing batch by filter not unlike `RabbitMessageGroupBatchRouter` router

## Export common metrics to Prometheus

It can be performed by the following utility methods in CommonMetrics class

* `setLiveness` - sets "liveness" metric of a service (exported as `th2_liveness` gauge)
* `setReadiness` - sets "readiness" metric of a service (exported as th2_readiness gauge)

NOTES:

* in order for the metrics to be exported, you will also need to create an instance of CommonFactory
* common JVM metrics will also be exported alongside common service metrics
* some metric labels are
  enumerations (`th2_type`: `MESSAGE_GROUP`, `EVENT`, `<customTag>`;`message_type`: `RAW_MESSAGE`, `MESSAGE`)

COMMON METRICS:

* th2_component (`name`): information about the current component

RABBITMQ METRICS:

* th2_rabbitmq_message_size_publish_bytes (`th2_pin`, `th2_type`, `exchange`, `routing_key`): number of published
  message bytes to RabbitMQ. The intended is intended for any data transferred via RabbitMQ, for example, th2 batch
  message or event or custom content
* th2_rabbitmq_message_publish_total (`th2_pin`, `th2_type`, `exchange`, `routing_key`): quantity of published messages
  to RabbitMQ. The intended is intended for any data transferred via RabbitMQ, for example, th2 batch message or event
  or custom content
* th2_rabbitmq_message_size_subscribe_bytes (`th2_pin`, `th2_type`, `queue`): number of bytes received from RabbitMQ, it
  includes bytes of messages dropped after filters. For information about the number of dropped messages, please refer
  to 'th2_message_dropped_subscribe_total' and 'th2_message_group_dropped_subscribe_total'. The message is intended for
  any data transferred via RabbitMQ, for example, th2 batch message or event or custom content
* th2_rabbitmq_message_process_duration_seconds (`th2_pin`, `th2_type`, `queue`): time of message processing during
  subscription from RabbitMQ in seconds. The message is intended for any data transferred via RabbitMQ, for example, th2
  batch message or event or custom content

GRPC METRICS:

* th2_grpc_invoke_call_total (`th2_pin`, `service_name`, `service_method`): total number of calling particular gRPC
  method
* th2_grpc_invoke_call_request_bytes (`th2_pin`, `service_name`, `service_method`): number of bytes sent to particular
  gRPC call
* th2_grpc_invoke_call_response_bytes (`th2_pin`, `service_name`, `service_method`): number of bytes sent to particular
  gRPC call
* th2_grpc_receive_call_total (`th2_pin`, `service_name`, `service_method`): total number of consuming particular gRPC
  method
* th2_grpc_receive_call_request_bytes (`th2_pin`, `service_name`, `service_method`): number of bytes received from
  particular gRPC call
* th2_grpc_receive_call_response_bytes (`th2_pin`, `service_name`, `service_method`): number of bytes sent to particular
  gRPC call

MESSAGES METRICS:

* th2_message_publish_total (`th2_pin`, `session_alias`, `direction`, `message_type`): quantity of published raw or
  parsed messages
* th2_message_subscribe_total (`th2_pin`, `session_alias`, `direction`, `message_type`): quantity of received raw or
  parsed messages, includes dropped after filters. For information about the number of dropped messages, please refer
  to 'th2_message_dropped_subscribe_total'
* th2_message_dropped_publish_total (`th2_pin`, `session_alias`, `direction`, `message_type`): quantity of published raw
  or parsed messages dropped after filters
* th2_message_dropped_subscribe_total (`th2_pin`, `session_alias`, `direction`, `message_type`): quantity of received
  raw or parsed messages dropped after filters
* th2_message_group_publish_total (`th2_pin`, `session_alias`, `direction`): quantity of published message groups
* th2_message_group_subscribe_total (`th2_pin`, `session_alias`, `direction`): quantity of received message groups,
  includes dropped after filters. For information about the number of dropped messages, please refer to '
  th2_message_group_dropped_subscribe_total'
* th2_message_group_dropped_publish_total (`th2_pin`, `session_alias`, `direction`): quantity of published message
  groups dropped after filters
* th2_message_group_dropped_subscribe_total (`th2_pin`, `session_alias`, `direction`): quantity of received message
  groups dropped after filters
* th2_message_group_sequence_publish (`th2_pin`, `session_alias`, `direction`): last published sequence
* th2_message_group_sequence_subscribe (`th2_pin`, `session_alias`, `direction`): last received sequence

EVENTS METRICS:

* th2_event_publish_total (`th2_pin`): quantity of published events
* th2_event_subscribe_total (`th2_pin`): quantity of received events

### Test extensions:

To be able to use test extensions please fill build.gradle as in the example below:

```groovy
plugins {
    id 'java-test-fixtures'
}

dependencies {
    testImplementation testFixtures("com.exactpro.th2:common:3.39.0")
}
```

## Release notes

### 5.11.0-dev

+ Migrated to the th2 gradle plugin: `0.0.5` (bom: `4.6.1`)
+ Updated:
  + grpc-common: `4.5.0-dev`

### 5.10.1-dev

+ Use box name from `box.json` config as RabbitMQ connection name
+ Rise `th2_component` metric with box name as `name` label value
+ Fixed UUID generation for each event id

### 5.10.0-dev

+ Update bom: 4.5.0 -> 4.6.0
+ Update grpc-service-generator: 3.5.1 -> 3.6.0

### 5.9.1-dev

#### Updated:
+ cradle: `5.1.5-dev` (fixed: NullPointerException on AbstractMessageIteratorProvider creation for book with no pages in it)

### 5.9.0-dev
+ Added retry in case of a RabbitMQ channel or connection error (when possible).
+ Added InterruptedException to basicConsume method signature.
+ Added additional logging for RabbitMQ errors.
+ Fixed connection recovery delay time.
+ Integration tests for RabbitMQ retry scenarios.

### 5.8.0-dev
+ Added `NOT_WILDCARD` filter operation, which filter a field which isn't matched by wildcard expression.

### 5.7.2-dev
+ Event builder checks is the book name from attached message match to the event book name

### 5.7.1-dev

#### Updated:
+ grpc-service-generator: `3.5.1`

### 5.7.0-dev

#### Fix:
+ gRPC `retryConfiguration` has been moved from grpc.json to grpc_router.json
+ the whole default gRPC retry interval is about 1 minute

#### Updated:
+ grpc-service-generator: `3.5.0`

### 5.6.0-dev

#### Added:
+ New methods for transport message builders which allows checking whether the field is set or not
+ Serialization support for date time types (e.g. Instant, LocalDateTime/Date/Time) to event body serialization

### 5.5.0-dev

#### Changed:
+ Provided the ability to define configs directory using the `th2.common.configuration-directory` environment variable

### 5.4.2-dev

#### Fix

+ The serialization of `LocalTime`, `LocalDate` and `LocalDateTime` instances corrected for th2 transport parsed message.
  Old result would look like `[2023,9,7]`. Corrected serialization result looks like `2023-09-07`

### 5.4.1-dev
#### Fix
+ `SubscriberMonitor` is returned from `MessageRouter.subscribe` methods is proxy object to manage RabbitMQ subscribtion without internal listener  

### 5.4.0-dev
#### Updated
+ bom: `4.4.0-dev` to `4.5.0-dev`
+ kotlin: `1.8.22`
+ kubernetes-client: `6.1.1` to `6.8.0`
  + okhttp: `4.10.0` to `4.11.0`
  + okio: `3.0.0` to `3.5.0`

### 5.3.2-dev

#### Fix
+ Pin filters behaviour changed: conditions inside the message and metadata now combined as "and"

#### Feature
+ Added `protocol` field name for MQ pin filter
+ Added `hashCode`, `equals`, `toString`, `toBuilder` for th2 transport classes
+ Added `get` method for th2 transport `MapBuilder`, `CollectionBuilder` classes

### 5.3.1-dev

+ Auto-print git metadata from `git.properties` resource file.
  Child project should include `com.gorylenko.gradle-git-properties` Gradle plugin to generate required file

#### Change user code required:
+ Migrated from JsonProcessingException to IOException in Event class methods.
  This change allow remove required `jackson.core` dependency from child projects 

### 5.3.0-dev

+ Implemented message routers used th2 transport protocol for interaction

#### Updated:
+ cradle: `5.1.1-dev`
+ bom: `4.4.0`
+ grpc-common: `4.3.0-dev`
+ grpc-service-generator: `3.4.0`

#### Gradle plugins:
+ Updated org.owasp.dependencycheck: `8.3.1`
+ Added com.gorylenko.gradle-git-properties `2.4.1`
+ Added com.github.jk1.dependency-license-report `2.5`
+ Added de.undercouch.download `5.4.0`

### 5.2.2

#### Changed:
+ Book, session group and protocol message filtering added.

### 5.2.1

#### Changed:

+ The Cradle version is update to 5.0.2-dev-*.

### 5.2.0

+ Merged with 3.44.1

### 5.1.1

+ Added script for publishing dev-release for maven artefacts
+ Migrated to bom:4.2.0
+ Migrated to grpc-common:4.1.1-dev

### 5.1.0

+ Migrated to grpc-common 4.1.0

### 5.0.0

+ Migration to books/pages cradle 4.0.0
+ Migration to bom 4.0.2
+ Removed log4j 1.x from dependency
+ Removed `cradleInstanceName` parameter from `cradle.json`
+ Added `prepareStorage` property to `cradle.json`
+ `com.exactpro.th2.common.event.Event.toProto...()` by `parentEventId`/`bookName`/`(bookName + scope)`
+ Added `isRedelivered` flag to message

---

### 3.44.1

+ Remove unused dependency
+ Updated bom:4.2.0

### 3.43.0

+ There is no support for log4j version 1.
+ Work was done to eliminate vulnerabilities in _common_ and _bom_ dependencies.
    + **th2-bom must be updated to _4.0.3_ or higher.**

### 3.42.0

+ Added the `enableSizeMeasuring`, `maxMessageSize`, `keepAliveInterval` options into gRPC router configuration.
  Default values are false, 4194304, 60

### 3.41.1

+ log4j and slf4j versions update
    + **th2-bom must be updated to _4.0.2_ or higher.**

### 3.41.0

+ Work was done to eliminate vulnerabilities in _common_ and _bom_ dependencies.
    + **th2-bom must be updated to _4.0.1_ or higher.**

### 3.40.0

+ gRPC router creates server
  support [grpc-service-reflection](https://github.com/grpc/grpc-java/blob/master/documentation/server-reflection-tutorial.md#grpc-server-reflection-tutorial)

### 3.39.3

+ Fixed:
    + The message is not confirmed when the delivery has incorrect format

### 3.39.2

+ Fixed:
    + The message is not confirmed when it was filtered

### 3.39.1

+ Fixed:
    + cradle version updated to 3.1.2. Unicode character sizes are calculated properly during serialization and tests
      are added for it also.

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

+ Added the ability to read dictionaries by aliases and as group of all available aliases
+ New methods for api: loadDictionary(String), getDictionaryAliases(), loadSingleDictionary()

### 3.33.0

#### Added:

+ Methods for subscription with manual acknowledgement
  (if the **prefetch count** is requested and no messages are acknowledged the reading from the queue will be
  suspended).
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

+ Fix the problem with filtering by `message_type` in MessageGroupBatch router

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

+ Extension method for `MessageRouter<EventBatch>` now send the event to all pins that satisfy the requested attributes
  set

### 3.25.0

+ Added util to convert RootMessageFilter into readable collection of payload bodies

### 3.24.2

+ Fixed `messageRecursionLimit` - still was not applied to all kind of RabbitMQ subscribers

### 3.24.1

+ Fixed `messageRecursionLimit` setting for all kind of RabbitMQ subscribers

### 3.24.0

+ Added setting `messageRecursionLimit`(the default value is set to 100) to RabbitMQ configuration that denotes how deep
  nested protobuf messages might be.

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
+ Added the ability for resubscribe on canceled subscriber.

### 3.20.0

+ Extended filter's format
    + Add multiply filters support

### 3.19.0

+ Added the Event.toListBatchProto method to covert an instance of the Event class to list of event batches.

### 3.18.2

#### Changed:

+ Fix possible NPE when adding the `Exception` to the event with `null` message
+ Correct exception messages

### 3.18.1

#### Changed:

+ The `toMetadataFilter` returns `null` if the original metadata has nothing to compare
+ The `toRootMessageFilter` does not add the `MetadataFilter` if the message's metadata has nothing to compare

### 3.18.0

#### Changed:

+ Update Cradle version from `2.9.1` to `2.13.0`

### 3.17.0

+ Extended message utility class
    + Added the toRootMessageFilter method to convert a message to root message filter

### 3.16.5

+ Update `th2-grpc-common` and `th2-grpc-service-generator` versions to `3.2.0` and `3.1.12` respectively

### 3.16.4

+ Disable waiting for connection recovery when closing the `SubscribeMonitor`

### 3.16.3

+ Change the way that channels are stored (they are mapped to the pin instead of to the thread).
  It might increase the average number of channels used by the box, but it also limits the max number of channels to the
  number of pins

### 3.16.2

+ Restore backward compatibility for MessageConverter factory methods.
  **NOTE: one of the methods was not restored and an update to this version might require manual update for your code**.
  The old methods without `toTraceString` supplier will be removed in the future
+ Fixed configuration for gRPC server.
    + Added the property `workers`, which changes the count of gRPC server's threads

### 3.16.0

+ Extended Utility classes
    + Added the toTreeTable method to convert message/message filter to event data
    + Added the Event.exception method to include an exception and optionally all the causes to the body data as a
      series of messages

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
+ uses mq_router, grpc_router, cradle_manager optional JSON configs from the `/var/th2/config` folder

### 3.11.0

+ tries to load log4j.properties files from sources in order: '/var/th2/config', '/home/etc', configured path via cmd,
  default configuration

### 3.6.0

+ update Cradle version. Introduce async API for storing events

### 3.0.1

+ metrics related to time measurement of an incoming message handling (Raw / Parsed / Event) migrated to
  Prometheus [histogram](https://prometheus.io/docs/concepts/metric_types/#histogram)