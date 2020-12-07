# th2 common library (Java)

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
    
### Requirements for creatring factory with Kubernetes

1. It is necessary to have Kubernetes configuration written in ~/.kube/config. See more on kubectl configuration: 
https://kubernetes.io/docs/tasks/access-application-cluster/configure-access-multiple-clusters/

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

Please refer to [th2-grpc-common] (https://github.com/th2-net/th2-grpc-common/blob/master/src/main/proto/th2_grpc_common/common.proto "common.proto") for further details.

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
