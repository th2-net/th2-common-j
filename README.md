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

After that you can get various `Routers` through `factory` properties:
```
var messageRouter = factory.getMessageRouterParsedBatch();
var rawRouter = factory.getMessageRouterRawBatch();
var eventRouter = factory.getEventBatchRouter();
```

`messageRouter` is working with `MessageBatch` <br>
`rawRouter` is working with `RawMessageBatch` <br>
`eventRouter` is working with `EventBatch`

See [th2-grpc-common](https://github.com/th2-net/th2-grpc-common/blob/master/src/main/proto/th2_grpc_common/common.proto "common.proto") for details.

With `router` created, you can subscribe to pins (specifying callback function) or send data that router works with:
```
router.subscribe(callback)  # subscribe to only one pin 
router.subscribeAll(callback)  # subscribe to one or several pins
router.send(message)  # send to only one pim
router.sendAll(message)  # send to one or several pins
```
You can do these actions provide pin attributes in addition to default ones.
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
