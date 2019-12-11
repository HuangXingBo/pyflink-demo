import os

from pyflink.datastream import StreamExecutionEnvironment, TimeCharacteristic
from pyflink.table import StreamTableEnvironment, CsvTableSink, DataTypes
from pyflink.table.descriptors import Schema, Rowtime, Json, Kafka
from pyflink.table.window import Slide


def slide_time_window_streaming():
    s_env = StreamExecutionEnvironment.get_execution_environment()
    s_env.set_parallelism(1)
    s_env.set_stream_time_characteristic(TimeCharacteristic.EventTime)
    st_env = StreamTableEnvironment.create(s_env)
    result_file = "/tmp/slide_time_window_streaming.csv"
    if os.path.exists(result_file):
        os.remove(result_file)
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

    st_env.register_table_sink("sink",
                               CsvTableSink(["a", "b"],
                                            [DataTypes.STRING(),
                                             DataTypes.STRING()],
                                            result_file))

    st_env.scan("source").window(Slide.over("1.hours").every("10.minutes").on("rowtime").alias("w")) \
        .group_by("w, a") \
        .select("a, max(b)").insert_into("sink")

    st_env.execute("slide time window streaming")
    # cat /tmp/slide_time_window_streaming.csv
    # a,1
    # a,1
    # b,2
    # a,3
    # b,2
    # b,2
    # a,3
    # b,2
    # a,3
    # b,2
    # a,3
    # b,2
    # a,4
    # b,4
    # a,4


def slide_row_window_streaming():
    s_env = StreamExecutionEnvironment.get_execution_environment()
    s_env.set_parallelism(1)
    s_env.set_stream_time_characteristic(TimeCharacteristic.ProcessingTime)
    st_env = StreamTableEnvironment.create(s_env)
    result_file = "/tmp/slide_row_window_streaming.csv"
    if os.path.exists(result_file):
        os.remove(result_file)
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
             .field("proctime", DataTypes.TIMESTAMP())
             .proctime()
             .field("a", DataTypes.STRING())
             .field("b", DataTypes.STRING())
             .field("c", DataTypes.STRING())
         ) \
        .in_append_mode() \
        .register_table_source("source")

    st_env.register_table_sink("sink",
                               CsvTableSink(["a", "b"],
                                            [DataTypes.STRING(),
                                             DataTypes.STRING()],
                                            result_file))

    st_env.scan("source").window(Slide.over("2.rows").every("1.rows").on("proctime").alias("w")) \
        .group_by("w, a") \
        .select("a, max(b)").insert_into("sink")

    st_env.execute("slide row window streaming")
    # cat /tmp/slide_row_window_streaming.csv
    # a,1
    # b,2
    # a,3
    # a,4
    # b,4
    # a,5


if __name__ == '__main__':
    from table.prepare_environment import prepare_env
    prepare_env(need_stream_source=True)
    # slide_time_window_streaming()
    slide_row_window_streaming()
