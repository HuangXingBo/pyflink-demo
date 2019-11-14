from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, DataTypes
from pyflink.table.descriptors import CustomConnectorDescriptor, Schema


def in_streaming():
    s_env = StreamExecutionEnvironment.get_execution_environment()
    s_env.set_parallelism(1)
    st_env = StreamTableEnvironment.create(s_env)
    left = st_env.from_elements(
        [(1, "ra", "raa"), (2, "lb", "lbb"), (3, "", "lcc"), (2, "lb", "lbb"), (4, "ra", "raa")],
        ["a", "b", "c"]).select("a, b, c")
    right = st_env.from_elements([(1, "ra", "raa"), (2, "", "rbb"), (3, "rc", "rcc"), (1, "ra", "raa")],
                                 ["a", "b", "c"]).select("a")

    result = left.where("a.in(%s)" % right).select("b, c")
    # another way
    # st_env.register_table("RightTable", right)
    # result = left.where("a.in(RightTable)")

    # use custom retract sink connector
    custom_connector = CustomConnectorDescriptor('pyflink-test', 1, False)
    st_env.connect(custom_connector) \
        .with_schema(
        Schema()
            .field("a", DataTypes.STRING())
            .field("b", DataTypes.STRING())
    ).register_table_sink("sink")
    result.insert_into("sink")
    st_env.execute("in streaming")
    # (true, ra, raa)
    # (true, lb, lbb)
    # (true, lb, lbb)
    # (true,, lcc)


if __name__ == '__main__':
    in_streaming()
