from pyspark.sql import SparkSession, Row
from pyspark.streaming import StreamingContext

spark = SparkSession.builder \
    .appName("SocketSQLStreaming") \
    .getOrCreate()

# Load the data
filename = "kddcup.data.gz"
df = spark.read.text(filename).cache()
data = df.rdd.map(lambda d: d.value.split(","))
rows = data.map(lambda d: Row(duration=int(d[0]), protocol_type=d[1], service=d[2], flag=d[3], src_bytes=int(d[4]), dst_bytes=int(d[5]), 
                              land=int(d[6]), wrong_fragment=int(d[7]), urgent=int(d[8]), hot=int(d[9]), num_failed_logins=int(d[10]), 
                              logged_in=int(d[11]), num_compromised=int(d[12]), root_shell=int(d[13]), su_attempted=int(d[14]), 
                              num_root=int(d[15]), num_file_creations=int(d[16]), num_shells=int(d[17]), num_access_files=int(d[18]), 
                              num_outbound_cmds=int(d[19]), is_host_login=int(d[20]), is_guest_login=int(d[21]), count=int(d[22]), 
                              srv_count=int(d[23]), serror_rate=float(d[24]), srv_serror_rate=float(d[25]), rerror_rate=float(d[26]), 
                              srv_rerror_rate=float(d[27]), same_srv_rate=float(d[28]), diff_srv_rate=float(d[29]), 
                              srv_diff_host_rate=float(d[30]), dst_host_count=int(d[31]), dst_host_srv_count=int(d[32]), 
                              dst_host_same_srv_rate=float(d[33]), dst_host_diff_srv_rate=float(d[34]), 
                              dst_host_same_src_port_rate=float(d[35]), dst_host_srv_diff_host_rate=float(d[36]), 
                              dst_host_serror_rate=float(d[37]), dst_host_srv_serror_rate=float(d[38]), 
                              dst_host_rerror_rate=float(d[39]), dst_host_srv_rerror_rate=float(d[40]), attack=d[41]))

df_interaction = spark.createDataFrame(rows)
df_interaction.createOrReplaceTempView("interactions")

# Create a socket stream
ssc = StreamingContext(spark.sparkContext, 10)

# Create a DStream that will connect to hostname:port, like localhost:9999
lines = ssc.socketTextStream("localhost", 9999)

# Process the queries
def process_queries(rdd):
    for query in rdd.collect():
        if query.strip():  # Check if there is something to execute
            try:
                result = spark.sql(query)
                result.show()
            except Exception as e:
                print("Error al ejecutar la consulta:", str(e))
# Execute the queries
lines.foreachRDD(process_queries)

# Start the computation
ssc.start()
ssc.awaitTermination()
