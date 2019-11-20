# User-defined Sources & Sinks
This module helps users to create and use custom sources & sinks

## Build Sources & Sinks

### Custom Sink
The example of custom retract table sink lives in sinks module. You need to build this code:

```shell
cd sinks; mvn clean package
```

1. put the created user_defined_sources_and_sinks.jar in target directory into Python site-packages/pyflink/lib directory

2. you can refer to [CustomTableSourceDemo.py](https://github.com/HuangXingBo/pyflink-demo/blob/master/table/user_defined_sources_and_sinks/CustomTableSourceDemo.py)
how to use custom connectors.
