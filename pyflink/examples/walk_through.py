
from pyflink.dataset import ExecutionEnvironment
from pyflink.table import BatchTableEnvironment, DataTypes
from pyflink.table.descriptors import Schema, FileSystem, OldCsv

s_env = ExecutionEnvironment.get_execution_environment()
s_env.set_parallelism(1)
st_env = BatchTableEnvironment.create(s_env)

t = st_env.from_elements([(1, 'hi', 'hello'), (2, 'hi', 'hello')], ['a', 'b', 'c'])
st_env.connect(FileSystem().path('/tmp/streaming5.csv')) \
    .with_format(OldCsv()
                 .field_delimiter(',')
                 .field("a", DataTypes.BIGINT())
                 .field("b", DataTypes.STRING())
                 .field("c", DataTypes.STRING())) \
    .with_schema(Schema()
                 .field("a", DataTypes.BIGINT())
                 .field("b", DataTypes.STRING())
                 .field("c", DataTypes.STRING())) \
    .register_table_sink("stream_sink")

t.select("a + 1, b, c").insert_into("stream_sink")

st_env.execute("stream_job")
