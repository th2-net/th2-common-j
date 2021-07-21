# th2 common library (Java) (3.18.0)

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
1. Create factory with a namespace in Kubernetes, the name of the target th2 box from Kubernetes and the name of context to choose the context from Kube config: 
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
  "prefetchCount": 10
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
* timeout - this option defines connection timeout in milliseconds. If set to 0 or ommited, the default value of 5000 is used.
* pageSize - this option defines the size of the result set to fetch at a time. If set to 0 or ommited, the default value of 5000 is used.

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
  "pageSize": 5000
}
```

### Requirements for creating factory with Kubernetes

1. It is necessary to have Kubernetes configuration written in ~/.kube/config. See more on kubectl configuration [here](https://kubernetes.io/docs/tasks/access-application-cluster/configure-access-multiple-clusters/).

1. Also note that `generated_configs` directory will be created to store `.json` files with configs from Kubernetes. Those files are overridden when `CommonFactory.createFromKubernetes(namespace, boxName)` and `CommonFactory.createFromKubernetes(namespace, boxName, contextName)` are invoked again. 

1. User needs to have authentication with service account token that has necessary access to read CRs and secrets from the specified namespace. 

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
router.send(message)  # send to only one pim
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

## Release notes

### 3.18.0

+ Extended filter's format
    + Add multiply filters support 

### 3.17.0
+ Extended message utility class
  + Added the toRootMessageFilter method to convert message to root message filter

### 3.16.5
+ Update `th2-grpc-common` and `th2-grpc-service-generator` versions to `3.2.0` and `3.1.12` respectively

### 3.16.4

+ Disable waiting for connection recovery when closing the `SubscribeMonitor`

### 3.16.3

+ Change the way channels are stored (they mapped to the pin instead of the thread).
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
