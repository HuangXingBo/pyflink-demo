import functools
import os

from pyflink.datastream import StreamExecutionEnvironment, TimeCharacteristic
from pyflink.table import StreamTableEnvironment, DataTypes, EnvironmentSettings, CsvTableSource
from pyflink.table.descriptors import Schema, Kafka, Json, Rowtime, FileSystem, OldCsv
from pyflink.table.udf import ScalarFunction, udf


def scalar_func_basic():
    # prepare source and sink
    s_env = StreamExecutionEnvironment.get_execution_environment()
    s_env.set_stream_time_characteristic(TimeCharacteristic.EventTime)
    s_env.set_parallelism(1)
    # use blink table planner
    st_env = StreamTableEnvironment.create(s_env, environment_settings=EnvironmentSettings.new_instance()
                                           .in_streaming_mode().use_blink_planner().build())

    # use flink table planner
    # st_env = StreamTableEnvironment.create(s_env)

    st_env \
        .connect(  # declare the external system to connect to
            Kafka()
            .version("0.11")
            .topic("user")
            .start_from_earliest()
            .property("zookeeper.connect", "localhost:2181")
            .property("bootstrap.servers", "localhost:9092")
        ) \
        .with_format(  # declare a format for this system
            Json()
            .fail_on_missing_field(True)
            .json_schema(
                "{"
                "  type: 'object',"
                "  properties: {"
                "    a: {"
                "      type: 'string'"
                "    },"
                "    b: {"
                "      type: 'string'"
                "    },"
                "    c: {"
                "      type: 'string'"
                "    },"
                "    time: {"
                "      type: 'string',"
                "      format: 'date-time'"
                "    }"
                "  }"
                "}"
             )
         ) \
        .with_schema(  # declare the schema of the table
             Schema()
             .field("rowtime", DataTypes.TIMESTAMP())
             .rowtime(
                Rowtime()
                .timestamps_from_field("time")
                .watermarks_periodic_bounded(60000))
             .field("a", DataTypes.STRING())
             .field("b", DataTypes.STRING())
             .field("c", DataTypes.STRING())
         ) \
        .in_append_mode() \
        .register_table_source("source")

    result_file = "/tmp/scalar_func_basic.csv"
    if os.path.exists(result_file):
        os.remove(result_file)
    st_env.connect(FileSystem().path(result_file)) \
        .with_format(OldCsv()
                     .field_delimiter(',')
                     .field("c1", DataTypes.BIGINT())
                     .field("c2", DataTypes.BIGINT())
                     .field("c3", DataTypes.BIGINT())
                     .field("c4", DataTypes.BIGINT())
                     .field("c5", DataTypes.BIGINT())) \
        .with_schema(Schema()
                     .field("c1", DataTypes.BIGINT())
                     .field("c2", DataTypes.BIGINT())
                     .field("c3", DataTypes.BIGINT())
                     .field("c4", DataTypes.BIGINT())
                     .field("c5", DataTypes.BIGINT())) \
        .register_table_sink("Results")

    my_table = st_env.from_path("source")

    # create Python Scalar Function(5 choices)
    # option 1: extending the base class `ScalarFunction`
    class HashCodeMean(ScalarFunction):
        def eval(self, i, j):
            return (hash(i) + hash(j)) / 2

    hash_code_mean_1 = udf(HashCodeMean(), [DataTypes.STRING(), DataTypes.STRING()], DataTypes.BIGINT())

    # option 2: Python function
    @udf(input_types=[DataTypes.STRING(), DataTypes.STRING()], result_type=DataTypes.BIGINT())
    def hash_code_mean_2(i, j):
        return (hash(i) + hash(j)) / 2

    # option 3: lambda function
    hash_code_mean_3 = udf(lambda i, j: (hash(i) + hash(j)) / 2,
                           [DataTypes.STRING(), DataTypes.STRING()], DataTypes.BIGINT())

    # option 4: callable function
    class CallableHashCodeMean(object):
        def __call__(self, i, j):
            return (hash(i) + hash(j)) / 2

    hash_code_mean_4 = udf(CallableHashCodeMean(), [DataTypes.STRING(), DataTypes.STRING()], DataTypes.BIGINT())

    # option 5: partial function
    def partial_hash_code_mean(i, j, k):
        return (hash(i) + hash(j)) / 2 + k

    hash_code_mean_5 = udf(functools.partial(partial_hash_code_mean, k=0), [DataTypes.STRING(), DataTypes.STRING()],
                           DataTypes.BIGINT())

    # register the Python function
    st_env.register_function("hash_code_mean_1", hash_code_mean_1)
    st_env.register_function("hash_code_mean_2", hash_code_mean_2)
    st_env.register_function("hash_code_mean_3", hash_code_mean_3)
    st_env.register_function("hash_code_mean_4", hash_code_mean_4)
    st_env.register_function("hash_code_mean_5", hash_code_mean_5)

    # use the function in Python Table API
    my_table.select("hash_code_mean_1(a, b),hash_code_mean_2(a, c),"
                    "hash_code_mean_3(b, c),hash_code_mean_4(a + b, c),"
                    "hash_code_mean_5(a + c, b)").insert_into("Results")

    st_env.execute("scalar_func_basic")


if __name__ == '__main__':
    from table.prepare_environment import prepare_env
    prepare_env(need_stream_source=True)
    scalar_func_basic()
