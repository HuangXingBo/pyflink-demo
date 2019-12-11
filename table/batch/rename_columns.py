import os

from pyflink.dataset import ExecutionEnvironment
from pyflink.table import BatchTableEnvironment, CsvTableSource, CsvTableSink, DataTypes


def rename_columns_batch():
    b_env = ExecutionEnvironment.get_execution_environment()
    b_env.set_parallelism(1)
    bt_env = BatchTableEnvironment.create(b_env)
    source_file = os.getcwd() + "/../resources/table_orders.csv"
    result_file = "/tmp/table_rename_columns_batch.csv"
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
                               CsvTableSink(["a", "b"],
                                            [DataTypes.STRING(),
                                             DataTypes.INT()],
                                            result_file))
    orders = bt_env.scan("Orders")
    result = orders.rename_columns("a as a2, b as b2").select("a2, b2")
    result.insert_into("sink")
    bt_env.execute("rename columns batch")
    # cat /tmp/table_rename_columns_batch.csv
    # a,1
    # b,2
    # a,3
    # a,4
    # b,4
    # a,5


if __name__ == '__main__':
    rename_columns_batch()
