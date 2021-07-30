# th2 common library (Java) (2.20.0)

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
1. Create factory with a namespace in Kubernetes and the name of the target th2 box from Kubernetes:
    ```
    var factory = CommonFactory.createFromKubernetes(namespace, boxName);
    ```

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


1. It is necessary to have environment variables `CASSANDRA_PASS` and `RABBITMQ_PASS` to use configs from `cradle.json` and `rabbitMQ.json` as the passwords are not stored there explicitly. 

1. Also note that `generated_configs` directory will be created to store `.json` files with configs from Kubernetes. Those files are overridden when `CommonFactory.createFromKubernetes(namespace, boxName)` is invoked again. 

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

With router created, you can subscribe to pins (by specifying the callback function) or to send data that the router works with:
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
* (2.20.0) - Added the Event.toListBatchProto method to covert instance of the Event class to list of event batches.
* (2.19.3) - Disable waiting for connection recovery when closing the `SubscribeMonitor`
* (2.19.2) - Change the way channels are stored (they mapped to the pin instead of the thread).
  It might increase the average number of channels used by the box, but it also limits the max number of channels to the number of pins
* (2.19.1) - resets embedded log4j configuration before configuring from a file
* (2.19.0) - Extended Utility classes
    * Added the toTreeTable method to convert message/message filter to event data
    * Added the Event.exception method to include an exception and optionally all the causes to the body data as a series of messages
* (2.18.0) - added features: ability to get service from gRPC router via attributes, reading dictionary from new directory (`var/th2/config/directory`) 
* (2.17.2) - fix bug with filtering messages by `message_type`
* (2.17.0) - added toProtoEvent(EventID) and toProtoEvents(EventID) overloads into the Event class  
* (2.15.0) - tries to load log4j.properties files from sources in order: '/var/th2/config', '/home/etc', configured path via cmd, default configuration
* (2.13.0) - Update Cradle version. Introduce async API for storing events
* (2.9.2) - Metrics related to time measurement of an incoming message handling (Raw / Parsed / Event) migrated to Prometheus [histogram](https://prometheus.io/docs/concepts/metric_types/#histogram)
