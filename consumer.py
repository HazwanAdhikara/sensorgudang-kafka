from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, expr
from pyspark.sql.types import StructType, StringType, IntegerType

spark = SparkSession.builder \
    .appName('SensorMonitoring') \
    .getOrCreate()

# Schema
suhu_schema = StructType().add('gudang_id', StringType()).add('suhu', IntegerType())
kelembaban_schema = StructType().add('gudang_id', StringType()).add('kelembaban', IntegerType())

# Baca stream suhu
ds_suhu = spark.readStream \
    .format('kafka') \
    .option('kafka.bootstrap.servers', 'localhost:9092') \
    .option('subscribe', 'sensor-suhu-gudang') \
    .load() \
    .selectExpr("CAST(value AS STRING)") \
    .select(from_json(col('value'), suhu_schema).alias('data'), col('timestamp')) \
    .select('data.*', 'timestamp')

# Baca stream kelembaban
ds_kelembaban = spark.readStream \
    .format('kafka') \
    .option('kafka.bootstrap.servers', 'localhost:9092') \
    .option('subscribe', 'sensor-kelembaban-gudang') \
    .load() \
    .selectExpr("CAST(value AS STRING)") \
    .select(from_json(col('value'), kelembaban_schema).alias('data'), col('timestamp')) \
    .select('data.*', 'timestamp')

# Filtering peringatan
ds_suhu_warn = ds_suhu.filter(col('suhu') > 80) \
    .selectExpr("concat('[Peringatan Suhu Tinggi] Gudang ', gudang_id, ': Suhu ', suhu, '°C') as warning")

ds_kelembaban_warn = ds_kelembaban.filter(col('kelembaban') > 70) \
    .selectExpr("concat('[Peringatan Kelembaban Tinggi] Gudang ', gudang_id, ': Kelembaban ', kelembaban, '%') as warning")

# Stream peringatan individual ke console
ds_warn = ds_suhu_warn.union(ds_kelembaban_warn)

warn_query = ds_warn.writeStream \
    .outputMode('append') \
    .format('console') \
    .start()

# Join dua stream dalam window 10 detik untuk peringatan kritis
joined = ds_suhu.join(
    ds_kelembaban,
    expr("ds_suhu.gudang_id = ds_kelembaban.gudang_id AND abs(ds_suhu.timestamp - ds_kelembaban.timestamp) <= interval 10 seconds")
)
crit = joined.filter((col('suhu') > 80) & (col('kelembaban') > 70)) \
    .selectExpr(
        "concat('[PERINGATAN KRITIS] Gudang ', ds_suhu.gudang_id,
               ': - Suhu: ', suhu, '°C - Kelembaban: ', kelembaban,
               '% - Status: Bahaya tinggi! Barang berisiko rusak') as critical_warning"
    )
crit_query = crit.writeStream \
    .outputMode('append') \
    .format('console') \
    .start()

warn_query.awaitTermination()
crit_query.awaitTermination()