
from pyflink.datastream import StreamExecutionEnvironment, TimeCharacteristic
from pyflink.table import StreamTableEnvironment, DataTypes, EnvironmentSettings
from pyflink.table.descriptors import Schema, Kafka, Json, Rowtime
from pyflink.table.udf import udf


def ride_duration():
    s_env = StreamExecutionEnvironment.get_execution_environment()
    s_env.set_stream_time_characteristic(TimeCharacteristic.EventTime)
    s_env.set_parallelism(1)

    # use blink table planner
    st_env = StreamTableEnvironment \
        .create(s_env, environment_settings=EnvironmentSettings
                .new_instance()
                .in_streaming_mode()
                .use_blink_planner().build())

    # register source and sink
    register_rides_source(st_env)
    register_ride_duration_sink(st_env)

    # register java udf (isInNYC, timeDiff)
    # 注：timeDiff对应类的路径是：com.ververica.sql_training.udfs.TimeDiff
    st_env.register_java_function("isInNYC", "com.ververica.sql_training.udfs.IsInNYC")
    ？？？ #注册timeDiff函数

    # query
    source_table = st_env.from_path("Rides")

    left_table = source_table\
        .where("isStart.isTrue && isInNYC(lon, lat)")\
        .select("rideId as startRideId, rideTime as startRideTime")

    right_table = source_table\
        .where("isStart.isFalse && isInNYC(lon, lat)")\
        .select("rideId as endRideId, rideTime as endRideTime")

    left_table.join(right_table,
                    "startRideId == endRideId && endRideTime.between(startRideTime, startRideTime + 2.hours) ")\
        .select("？？？")\
        .insert_into("TempResults")

    # execute
    st_env.execute("ride_duration")


def register_rides_source(st_env):
    st_env \
        .connect(  # declare the external system to connect to
        Kafka()
            .version("0.11")
            .topic("Rides")
            .start_from_earliest()
            .property("zookeeper.connect", "zookeeper:2181")
            .property("bootstrap.servers", "kafka:9092")) \
        .with_format(  # declare a format for this system
        Json()
            .fail_on_missing_field(True)
            .schema(DataTypes.ROW([
            DataTypes.FIELD("rideId", DataTypes.BIGINT()),
            DataTypes.FIELD("isStart", DataTypes.BOOLEAN()),
            DataTypes.FIELD("eventTime", DataTypes.TIMESTAMP()),
            DataTypes.FIELD("lon", DataTypes.FLOAT()),
            DataTypes.FIELD("lat", DataTypes.FLOAT()),
            DataTypes.FIELD("psgCnt", DataTypes.INT()),
            DataTypes.FIELD("taxiId", DataTypes.BIGINT())]))) \
        .with_schema(  # declare the schema of the table
        Schema()
            .field("rideId", DataTypes.BIGINT())
            .field("taxiId", DataTypes.BIGINT())
            .field("isStart", DataTypes.BOOLEAN())
            .field("lon", DataTypes.FLOAT())
            .field("lat", DataTypes.FLOAT())
            .field("psgCnt", DataTypes.INT())
            .field("rideTime", DataTypes.TIMESTAMP())
            .rowtime(
            Rowtime()
                .timestamps_from_field("eventTime")
                .watermarks_periodic_bounded(60000))) \
        .in_append_mode() \
        .register_table_source("Rides")


def register_ride_duration_sink(st_env):
    st_env \
        .connect(  # declare the external system to connect to
        Kafka()
            .version("0.11")
            .topic("TempResults")
            .property("zookeeper.connect", "zookeeper:2181")
            .property("bootstrap.servers", "kafka:9092")) \
        .with_format(  # declare a format for this system
        Json()
            .fail_on_missing_field(True)
            .schema(DataTypes.ROW([
            DataTypes.FIELD("rideId", DataTypes.BIGINT()),
            DataTypes.FIELD("durationMin", DataTypes.BIGINT())
        ]))) \
        .with_schema(  # declare the schema of the table
        Schema()
            .field("rideId", DataTypes.BIGINT())
            .field("durationMin", DataTypes.BIGINT())) \
        .in_append_mode() \
        .register_table_sink("TempResults")


if __name__ == '__main__':
    ride_duration()