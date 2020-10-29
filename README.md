# TH2 common library

This library allows you to:

* export common metrics to Prometheus
  
    It can be done via following utility methods in `CommonMetrics` class
    
    * `setLiveness` - sets "liveness" metric of a service (exported as `th2_liveness` gauge)
    * `setReadiness` - sets "readiness" metric of a service which (exported as `th2_readiness` gauge)
      
    NOTES:
     
    * for metrics to be exported you also need to create an instance of `CommonFactory`
    * common JVM metrics are also exported alongside common service metrics