import os

from pyflink.dataset import ExecutionEnvironment
from pyflink.table import BatchTableEnvironment, CsvTableSource, CsvTableSink, DataTypes
from pyflink.table.window import Tumble


def tumble_row_window_batch():
    b_env = ExecutionEnvironment.get_execution_environment()
    b_env.set_parallelism(1)
    bt_env = BatchTableEnvironment.create(b_env)
    source_file = os.getcwd() + "/../resources/table_orders.csv"
    result_file = "/tmp/table_tumble_row_window_batch.csv"
    if os.path.exists(result_file):
        os.remove(result_file)
    bt_env.register_table_source("Orders",
                                 CsvTableSource(source_file,
                                                ["a", "b", "c", "rowtime"],
                                                [DataTypes.STRING(),
                                                 DataTypes.INT(),
                                                 DataTypes.INT(),
                                                 DataTypes.TIMESTAMP()]))
    bt_env.register_table_sink("sink",
                               CsvTableSink(["a"],
                                            [DataTypes.INT()],
                                            result_file))
    orders = bt_env.scan("Orders")
    result = orders.window(Tumble.over("2.rows").on("rowtime").alias("w")) \
        .group_by("w, a").select("b.sum")
    result.insert_into("sink")
    bt_env.execute("tumble row window batch")
    # cat /tmp/table_tumble_row_window_batch.csv
    # 4
    # 9
    # 6


def tumble_time_window_batch():
    b_env = ExecutionEnvironment.get_execution_environment()
    b_env.set_parallelism(1)
    bt_env = BatchTableEnvironment.create(b_env)
    source_file = os.getcwd() + "/../resources/table_orders.csv"
    result_file = "/tmp/table_tumble_time_window_batch.csv"
    if os.path.exists(result_file):
        os.remove(result_file)
    bt_env.register_table_source("Orders",
                                 CsvTableSource(source_file,
                                                ["a", "b", "c", "rowtime"],
                                                [DataTypes.STRING(),
                                                 DataTypes.INT(),
                                                 DataTypes.INT(),
                                                 DataTypes.TIMESTAMP()]))
    bt_env.register_table_sink("sink",
                               CsvTableSink(["a"],
                                            [DataTypes.INT()],
                                            result_file))
    orders = bt_env.scan("Orders")
    result = orders.window(Tumble.over("30.minutes").on("rowtime").alias("w")) \
        .group_by("w, a").select("b.sum")
    result.insert_into("sink")
    bt_env.execute("tumble time window batch")
    # cat /tmp/table_tumble_time_window_batch.csv
    # 1
    # 3
    # 4
    # 5
    # 2
    # 4


if __name__ == '__main__':
    tumble_row_window_batch()
    # tumble_time_window_batch()
