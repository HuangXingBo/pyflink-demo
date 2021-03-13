################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
################################################################################
import time

from pyflink.datastream import StreamExecutionEnvironment, TimeCharacteristic
from pyflink.table import StreamTableEnvironment, EnvironmentSettings
from pyflink.table.expressions import col
from pyflink.table.types import DataTypes, DataType
from pyflink.table.udf import AggregateFunction, ACC, T, udaf

from table.connectors.sink import PrintTableSink


class CountAggregateFunction(AggregateFunction):

    def get_value(self, accumulator):
        return accumulator[0]

    def create_accumulator(self):
        return [0]

    def accumulate(self, accumulator, *args):
        accumulator[0] = accumulator[0] + 1

    def retract(self, accumulator, *args):
        accumulator[0] = accumulator[0] - 1

    def merge(self, accumulator, accumulators):
        for other_acc in accumulators:
            accumulator[0] = accumulator[0] + other_acc[0]

    def get_accumulator_type(self):
        return DataTypes.ARRAY(DataTypes.BIGINT())

    def get_result_type(self):
        return DataTypes.BIGINT()


class MeanAggregateFunction(AggregateFunction):

    def get_value(self, accumulator: ACC) -> T:
        if accumulator[1] == 0:
            return None
        else:
            return accumulator[0] / accumulator[1]

    def create_accumulator(self) -> ACC:
        return [0, 0]

    def accumulate(self, accumulator: ACC, *args):
        accumulator[0] += args[0]
        accumulator[1] += 1

    def retract(self, accumulator: ACC, *args):
        accumulator[0] -= args[0]
        accumulator[1] -= 1

    def merge(self, accumulator: ACC, accumulators):
        for other_acc in accumulators:
            accumulator[0] += other_acc[0]
            accumulator[1] += other_acc[1]

    def get_accumulator_type(self) -> DataType:
        return DataTypes.ARRAY(DataTypes.BIGINT())

    def get_result_type(self) -> DataType:
        return DataTypes.FLOAT()


def test_general_python_udaf():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    env.set_stream_time_characteristic(TimeCharacteristic.EventTime)
    environment_settings = EnvironmentSettings.new_instance().use_blink_planner().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=environment_settings)

    # add needed jar files
    t_env.get_config().get_configuration().set_string(
        "pipeline.jars",
        "file:////Users/duanchen/PycharmProjects/pyflink-demo/table/pythonudf/aggregate_funciton/"
        "flink-perf-tests-0.1.jar")

    # config some params
    # t_env.get_config().get_configuration().set_integer("python.fn-execution.bundle.size", 1000000)
    t_env.get_config().get_configuration().set_integer("python.fn-execution.bundle.time", 1000)

    # 1. use DSL
    python_avg = udaf(MeanAggregateFunction())

    # 2. register function
    # t_env.create_temporary_function("python_avg", MeanAggregateFunction())

    num_rows = 10000000

    # register custom source
    t_env.execute_sql(f"""
        CREATE TABLE source (
            id INT,
            num INT,
            rowtime TIMESTAMP(3),
            WATERMARK FOR rowtime AS rowtime - INTERVAL '60' MINUTE
        ) WITH (
          'connector' = 'Range',
          'start' = '1',
          'end' = '{num_rows}',
          'step' = '1',
          'partition' = '200'
        )
    """)

    # register custom sink
    t_env.register_table_sink(
        "sink",
        PrintTableSink(
            ["num", "value"],
            [DataTypes.INT(False), DataTypes.FLOAT(False)], 1000000))

    t = t_env.from_path("source")
    result = t.select((t.num % 100).alias('num'), t.id) \
        .group_by(col('num')) \
        .select(t.num, python_avg(t.id))

    beg_time = time.time()
    result.execute_insert("sink").wait()
    print("PyFlink stream group agg consume time: " + str(time.time() - beg_time))


if __name__ == '__main__':
    test_general_python_udaf()
