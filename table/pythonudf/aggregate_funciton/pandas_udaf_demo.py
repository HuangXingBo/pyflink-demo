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

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, EnvironmentSettings, BatchTableEnvironment
from pyflink.table.types import DataTypes
from pyflink.table.udf import udf, udaf, AggregateFunction

from table.connectors.sink import PrintTableSink


def test_batch_job():
    t_env = BatchTableEnvironment.create(environment_settings=EnvironmentSettings.new_instance()
                                         .in_batch_mode().use_blink_planner().build())
    t_env._j_tenv.getPlanner().getExecEnv().setParallelism(1)

    # add needed jar files
    t_env.get_config().get_configuration().set_string(
        "pipeline.jars",
        "file:////Users/duanchen/PycharmProjects/pyflink-demo/table/pythonudf/aggregate_funciton/"
        "flink-perf-tests-0.1.jar")

    # config some params
    # t_env.get_config().get_configuration().set_integer("python.fn-execution.bundle.size", 1000000)
    t_env.get_config().get_configuration().set_integer("python.fn-execution.bundle.time", 1000)

    @udaf(result_type=DataTypes.FLOAT(False), func_type="pandas")
    def mean(x):
        return x.mean()

    @udf(result_type=DataTypes.INT())
    def inc(x):
        return x + 1

    class MaxAdd(AggregateFunction):

        def open(self, function_context):
            pass

        def get_value(self, accumulator):
            return accumulator[0]

        def create_accumulator(self):
            return []

        def accumulate(self, accumulator, *args):
            result = 0
            for arg in args:
                result += arg.max()
            accumulator.append(result)

    t_env.create_temporary_system_function("mean", mean)
    t_env.create_java_temporary_function("java_avg", "com.alibaba.flink.function.JavaAvg")
    t_env.create_temporary_system_function("max_add", udaf(MaxAdd(),
                                                           result_type=DataTypes.INT(),
                                                           func_type="pandas"))

    num_rows = 100000000

    t_env.execute_sql(f"""
        CREATE TABLE source (
            id INT,
            num INT,
            rowtime TIMESTAMP(3)
        ) WITH (
          'connector' = 'Range',
          'start' = '1',
          'end' = '{num_rows}',
          'step' = '1',
          'partition' = '200000'
        )
    """)

    # ------------------------ batch group agg -----------------------------------------------------
    t_env.register_table_sink(
        "sink",
        PrintTableSink(
            ["value"],
            [DataTypes.FLOAT(False)], 100000))
    result = t_env.from_path("source").group_by("num").select("mean(id)")  # 76s
    # result = t_env.from_path("source").group_by("num").select("id.avg")  # 13s
    # result = t_env.from_path("source").group_by("num").select("java_avg(id)")  # 28s
    result.insert_into("sink")
    beg_time = time.time()
    t_env.execute("Python UDF")
    print("PyFlink Pandas batch group agg consume time: " + str(time.time() - beg_time))

    # ------------------------ batch group window agg ----------------------------------------------
    # t_env.get_config().get_configuration().set_integer(
    #     "table.exec.window-agg.buffer-size-limit", 100 * 10000)
    # t_env.register_table_sink(
    #     "sink",
    #     PrintTableSink(
    #         ["start", "end", "value"],
    #         [DataTypes.TIMESTAMP(3), DataTypes.TIMESTAMP(3), DataTypes.FLOAT(False)],
    #         100))
    # tumble_window = Tumble.over(expr.lit(1).hours) \
    #     .on(expr.col("rowtime")) \
    #     .alias("w")
    # (true,1970-01-01 00:00:00.0,1970-01-01 01:00:00.0,4.9999848E7)
    # (true,1970-01-01 00:00:00.0,1970-01-01 01:00:00.0,4.9999952E7)
    # (true,1970-01-01 00:00:00.0,1970-01-01 01:00:00.0,5.0000048E7)
    # (true,1970-01-01 00:00:00.0,1970-01-01 01:00:00.0,5.0000152E7)
    # (true,1970-01-01 00:00:00.0,1970-01-01 01:00:00.0,5.0000248E7)
    # PyFlink Pandas batch group window agg consume time: 128.3926558494568

    # result = t_env.from_path("source").window(tumble_window) \
    #     .group_by("w, num") \
    #     .select("w.start, w.end, mean(id)")

    # (true,1970-01-01 00:00:00.0,1970-01-01 01:00:00.0,4.9999848E7)
    # (true,1970-01-01 00:00:00.0,1970-01-01 01:00:00.0,4.9999952E7)
    # (true,1970-01-01 00:00:00.0,1970-01-01 01:00:00.0,5.0000048E7)
    # (true,1970-01-01 00:00:00.0,1970-01-01 01:00:00.0,5.0000152E7)
    # (true,1970-01-01 00:00:00.0,1970-01-01 01:00:00.0,5.0000248E7)
    # PyFlink Pandas batch group window agg consume time: 12.697863817214966
    # result = t_env.from_path("source").window(tumble_window) \
    #     .group_by("w, num") \
    #     .select("w.start, w.end, id.avg")

    # (true,1970-01-01 00:00:00.0,1970-01-01 01:00:00.0,4.9999852E7)
    # (true,1970-01-01 00:00:00.0,1970-01-01 01:00:00.0,4.9999952E7)
    # (true,1970-01-01 00:00:00.0,1970-01-01 01:00:00.0,5.0000052E7)
    # (true,1970-01-01 00:00:00.0,1970-01-01 01:00:00.0,5.0000152E7)
    # (true,1970-01-01 00:00:00.0,1970-01-01 01:00:00.0,5.0000252E7)
    # PyFlink Pandas batch group window agg consume time: 63.56175494194031
    # result = t_env.from_path("source").window(tumble_window) \
    #     .group_by("w, num") \
    #     .select("w.start, w.end, mean(id)")
    #
    # result.insert_into("sink")
    # beg_time = time.time()
    # t_env.execute("Python UDF")
    # print("PyFlink Pandas batch group window agg consume time: " + str(time.time() - beg_time))

    # ------------------------ batch over window agg -----------------------------------------------
    # t_env.register_table_sink(
    #     "sink",
    #     PrintTableSink(
    #         ["num", "a",
    #          # "b", "c", "d", "e", "f",
    #          # "g",
    #          # "h", "i"
    #          ],
    #         [DataTypes.INT(), DataTypes.FLOAT(),
    #          # DataTypes.FLOAT(), DataTypes.FLOAT(),
    #          # DataTypes.FLOAT(), DataTypes.FLOAT(), DataTypes.FLOAT(),
    #          # DataTypes.FLOAT(), DataTypes.FLOAT(),
    #          ],
    #         10000000))
    # beg_time = time.time()
    # (true,7999,8000.0,8000.0,8000.0,8000.0,8000.0,8000.0,8000.0,8000.0,8000.0)
    # (true,15999,16000.0,16000.0,16000.0,16000.0,16000.0,16000.0,16000.0,16000.0,16000.0)
    # (true,23999,24000.0,24000.0,24000.0,24000.0,24000.0,24000.0,24000.0,24000.0,24000.0)
    # (true,31999,32000.0,32000.0,32000.0,32000.0,32000.0,32000.0,32000.0,32000.0,32000.0)
    # (true,39999,40000.0,40000.0,40000.0,40000.0,40000.0,40000.0,40000.0,40000.0,40000.0)
    # (true,47999,48000.0,48000.0,48000.0,48000.0,48000.0,48000.0,48000.0,48000.0,48000.0)
    # PyFlink Pandas batch group over window agg consume time: 130.6760711669922
    # t_env.execute_sql("""
    #             insert into sink
    #             select num,
    #              mean(id)
    #              over (PARTITION BY num ORDER BY rowtime
    #              ROWS BETWEEN UNBOUNDED preceding AND UNBOUNDED FOLLOWING),
    #              mean(id)
    #              over (PARTITION BY num ORDER BY rowtime
    #              ROWS BETWEEN UNBOUNDED preceding AND 0 FOLLOWING),
    #              mean(id)
    #              over (PARTITION BY num ORDER BY rowtime
    #              ROWS BETWEEN 1 PRECEDING AND UNBOUNDED FOLLOWING),
    #              mean(id)
    #              over (PARTITION BY num ORDER BY rowtime
    #              ROWS BETWEEN 1 PRECEDING AND 0 FOLLOWING),
    #              mean(id)
    #              over (PARTITION BY num ORDER BY rowtime
    #              RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING),
    #              mean(id)
    #              over (PARTITION BY num ORDER BY rowtime
    #              RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW),
    #              mean(id)
    #              over (PARTITION BY num ORDER BY rowtime
    #              RANGE BETWEEN INTERVAL '20' MINUTE PRECEDING AND UNBOUNDED FOLLOWING),
    #              mean(id)
    #              over (PARTITION BY num ORDER BY rowtime
    #              RANGE BETWEEN INTERVAL '20' MINUTE PRECEDING AND UNBOUNDED FOLLOWING),
    #              mean(id)
    #              over (PARTITION BY num ORDER BY rowtime
    #              RANGE BETWEEN INTERVAL '20' MINUTE PRECEDING AND CURRENT ROW)
    #             from source
    #         """).wait()

    #                  AVG(id)
    #                  over (PARTITION BY num ORDER BY rowtime
    #                  RANGE BETWEEN INTERVAL '20' MINUTE PRECEDING AND UNBOUNDED FOLLOWING),
    #                  AVG(id)
    #                  over (PARTITION BY num ORDER BY rowtime
    #                  RANGE BETWEEN INTERVAL '20' MINUTE PRECEDING AND UNBOUNDED FOLLOWING),

    # ,
    #                  AVG(id)
    #                  over (PARTITION BY num ORDER BY rowtime
    #                  RANGE BETWEEN INTERVAL '20' MINUTE PRECEDING AND CURRENT ROW)

    # ROWS BETWEEN UNBOUNDED preceding AND UNBOUNDED FOLLOWING)
    # 151.00761222839355s
    # 83.5823130607605

    # ROWS BETWEEN UNBOUNDED preceding AND 0 FOLLOWING)
    #
    # 76.46760702133179

    # t_env.execute_sql("""
    #             insert into sink
    #             select num,
    #              mean(id)
    #              over (PARTITION BY num ORDER BY rowtime
    #              ROWS BETWEEN UNBOUNDED preceding AND 0 FOLLOWING)
    #             from source
    #         """).wait()
    # print("PyFlink Pandas batch group over window agg consume time: " + str(time.time() - beg_time))


def test_stream_job():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    environment_settings = EnvironmentSettings.new_instance().use_blink_planner().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=environment_settings)

    t_env.get_config().get_configuration().set_integer("python.fn-execution.bundle.size", 500)
    t_env.get_config().get_configuration().set_integer("python.fn-execution.bundle.time", 1000)
    t_env.get_config().get_configuration().set_boolean("pipeline.object-reuse", True)

    @udaf(result_type=DataTypes.FLOAT(False), func_type="pandas")
    def mean(x):
        return x.mean()

    @udf(result_type=DataTypes.INT())
    def inc(x):
        return x + 1

    class MaxAdd(AggregateFunction):

        def open(self, function_context):
            pass

        def get_value(self, accumulator):
            return accumulator[0]

        def create_accumulator(self):
            return []

        def accumulate(self, accumulator, *args):
            result = 0
            for arg in args:
                result += arg.max()
            accumulator.append(result)

    t_env.register_function("mean", mean)
    t_env.register_java_function("java_avg", "com.alibaba.flink.function.JavaAvg")
    t_env.create_temporary_system_function("max_add", udaf(MaxAdd(),
                                                           result_type=DataTypes.INT(),
                                                           func_type="pandas"))

    num_rows = 100000

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
          'partition' = '20000'
        )
    """)

    # ------------------------ stream group window agg ---------------------------------------------
    # (true,1970-01-01 00:00:00.0,1970-01-01 01:00:00.0,1.0000402E7)
    # (true,1970-01-01 00:00:00.0,1970-01-01 01:00:00.0,1.0000302E7)
    # (true,1970-01-01 00:00:00.0,1970-01-01 01:00:00.0,1.0000202E7)
    # (true,1970-01-01 00:00:00.0,1970-01-01 01:00:00.0,1.0000102E7)
    # (true,1970-01-01 00:00:00.0,1970-01-01 01:00:00.0,1.0000002E7)
    # (true,1970-01-01 00:00:00.0,1970-01-01 01:00:00.0,9999902.0)
    # (true,1970-01-01 00:00:00.0,1970-01-01 01:00:00.0,9999802.0)
    # (true,1970-01-01 00:00:00.0,1970-01-01 01:00:00.0,9999702.0)
    # (true,1970-01-01 00:00:00.0,1970-01-01 01:00:00.0,9999602.0)
    # (true,1970-01-01 00:00:00.0,1970-01-01 01:00:00.0,9999502.0)
    # PyFlink Pandas stream group window agg consume time: 45.96089720726013

    # (true,1970-01-01 00:00:00.0,1970-01-01 01:00:00.0,1.0000402E7)
    # (true,1970-01-01 00:00:00.0,1970-01-01 01:00:00.0,1.0000302E7)
    # (true,1970-01-01 00:00:00.0,1970-01-01 01:00:00.0,1.0000202E7)
    # (true,1970-01-01 00:00:00.0,1970-01-01 01:00:00.0,1.0000102E7)
    # (true,1970-01-01 00:00:00.0,1970-01-01 01:00:00.0,1.0000002E7)
    # (true,1970-01-01 00:00:00.0,1970-01-01 01:00:00.0,9999902.0)
    # (true,1970-01-01 00:00:00.0,1970-01-01 01:00:00.0,9999802.0)
    # (true,1970-01-01 00:00:00.0,1970-01-01 01:00:00.0,9999702.0)
    # (true,1970-01-01 00:00:00.0,1970-01-01 01:00:00.0,9999602.0)
    # (true,1970-01-01 00:00:00.0,1970-01-01 01:00:00.0,9999502.0)
    # PyFlink Pandas stream group window agg consume time: 14.910825967788696

    # t_env.register_table_sink(
    #     "sink",
    #     PrintTableSink(
    #         ["start", "end", "value"],
    #         [DataTypes.TIMESTAMP(3), DataTypes.TIMESTAMP(3), DataTypes.FLOAT(False)],
    #         100))
    # tumble_window = Tumble.over(expr.lit(1).hours) \
    #     .on(expr.col("rowtime")) \
    #     .alias("w")
    # result = t_env.from_path("source").window(tumble_window) \
    #     .group_by("w, num") \
    #     .select("w.start, w.end, java_avg(id)")
    # result.insert_into("sink")
    # beg_time = time.time()
    # t_env.execute("Python UDF")
    # print("PyFlink Pandas stream group window agg consume time: " + str(time.time() - beg_time))

    # ------------------------ stream over window agg ----------------------------------------------
    # (true,4,5007.5)
    # (true,4,10007.5)
    # (true,4,15007.5)
    # (true,4,20007.5)
    # (true,4,25007.5)
    # (true,4,30007.5)
    # (true,2,35005.5)
    # (true,4,40007.5)
    # (true,0,45003.5)
    # PyFlink Pandas stream group over window agg consume time: 507.26891112327576

    # (true,4,5007.5)
    # (true,4,10007.5)
    # (true,4,15007.5)
    # (true,4,20007.5)
    # (true,4,25007.5)
    # (true,4,30007.498)
    # (true,2,35005.5)
    # (true,4,40007.5)
    # (true,0,45003.5)
    # PyFlink Pandas stream group over window agg consume time: 52.51417398452759
    t_env.register_table_sink(
        "sink",
        PrintTableSink(
            ["num", "a"],
            [DataTypes.INT(), DataTypes.FLOAT()],
            10000))
    beg_time = time.time()
    t_env.execute_sql("""
                insert into sink
                select num,
                 java_avg(id)
                 over (PARTITION BY num ORDER BY rowtime
                 RANGE BETWEEN INTERVAL '20' MINUTE PRECEDING AND CURRENT ROW)
                from source
            """).wait()
    print("PyFlink Pandas stream group over window agg consume time: " +
          str(time.time() - beg_time))


if __name__ == '__main__':
    test_batch_job()
    # test_stream_job()
