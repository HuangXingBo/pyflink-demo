import os

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, CsvTableSource, DataTypes
from pyflink.table.descriptors import CustomConnectorDescriptor, Schema


def distinct_streaming():
    s_env = StreamExecutionEnvironment.get_execution_environment()
    s_env.set_parallelism(1)
    st_env = StreamTableEnvironment.create(s_env)
    source_file = os.getcwd() + "/../resources/table_orders.csv"
    st_env.register_table_source("Orders",
                                 CsvTableSource(source_file,
                                                ["a", "b", "c", "rowtime"],
                                                [DataTypes.STRING(),
                                                 DataTypes.INT(),
                                                 DataTypes.INT(),
                                                 DataTypes.TIMESTAMP()]))

    orders = st_env.scan("Orders")
    result = orders.select("a, b").distinct()
    # use custom retract sink connector
    custom_connector = CustomConnectorDescriptor('pyflink-test', 1, False)
    st_env.connect(custom_connector) \
        .with_schema(
        Schema()
            .field("a", DataTypes.STRING())
            .field("b", DataTypes.INT())
    ).register_table_sink("sink")
    result.insert_into("sink")
    st_env.execute("distinct streaming")
    # (true, a, 1)
    # (true, b, 2)
    # (true, a, 3)
    # (true, a, 4)
    # (true, b, 4)
    # (true, a, 5)


if __name__ == '__main__':
    distinct_streaming()
