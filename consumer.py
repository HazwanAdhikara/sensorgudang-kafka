from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, expr, window, when, lit, coalesce
)
from pyspark.sql.types import StructType, StringType, IntegerType

spark = SparkSession.builder \
    .appName("MonitoringGudang") \
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1"
    ) \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# 1) Definisi schema
schema_suhu = StructType().add("gudang_id", StringType()).add("suhu", IntegerType())
schema_kelembaban = StructType().add("gudang_id", StringType()).add("kelembaban", IntegerType())

# 2) Baca & parse stream suhu
suhu_df = (
    spark.readStream
         .format("kafka")
         .option("kafka.bootstrap.servers", "localhost:9092")
         .option("subscribe", "sensor-suhu-gudang")
         .load()
)

suhu_stream = (
    suhu_df
    .selectExpr("CAST(value AS STRING) AS json")
    .select(from_json("json", schema_suhu).alias("d"))
    .select("d.gudang_id", "d.suhu", expr("current_timestamp() AS ts"))
    .withWatermark("ts", "15 seconds")
    .select("gudang_id", "suhu", window("ts", "10 seconds").alias("window"))
)

# 3) Baca & parse stream kelembaban
kelembaban_df = (
    spark.readStream
         .format("kafka")
         .option("kafka.bootstrap.servers", "localhost:9092")
         .option("subscribe", "sensor-kelembapan-gudang")
         .load()
)

kelembaban_stream = (
    kelembaban_df
    .selectExpr("CAST(value AS STRING) AS json")
    .select(from_json("json", schema_kelembaban).alias("d"))
    .select("d.gudang_id", "d.kelembaban", expr("current_timestamp() AS ts"))
    .withWatermark("ts", "15 seconds")
    .select("gudang_id", "kelembaban", window("ts", "10 seconds").alias("window"))
)

# 4) Full-outer join agar semua gudang muncul
report_stream = suhu_stream.join(
    kelembaban_stream,
    on=["gudang_id", "window"],
    how="full_outer"
)

# 5) Tentukan status berdasarkan kombinasi suhu & kelembaban
status_stream = report_stream.select(
    col("gudang_id").alias("Gudang"),
    coalesce(col("suhu"), lit(0)).alias("Suhu"),
    coalesce(col("kelembaban"), lit(0)).alias("Kelembaban"),
    when(
        (col("Suhu") > 80) & (col("Kelembaban") > 70),
        lit("Bahaya tinggi! Barang berisiko rusak")
    ).when(
        (col("Suhu") > 80) & (col("Kelembaban") <= 70),
        lit("Suhu tinggi, kelembaban normal")
    ).when(
        (col("Suhu") <= 80) & (col("Kelembaban") > 70),
        lit("Kelembaban tinggi, suhu aman")
    ).otherwise(
        lit("Aman")
    ).alias("Status"),
    "window"
)

# 6) Tulis hasil ke console setiap 5 detik
query = status_stream.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .trigger(processingTime="5 seconds") \
    .start()

query.awaitTermination()
