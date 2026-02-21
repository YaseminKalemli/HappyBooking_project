# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "7b5f9c5f-8b18-497f-8d36-e1c26b4a2c82",
# META       "default_lakehouse_name": "HB_Lakehouse",
# META       "default_lakehouse_workspace_id": "33b9db8f-d2f9-4407-a256-598ccfa70964",
# META       "known_lakehouses": [
# META         {
# META           "id": "7b5f9c5f-8b18-497f-8d36-e1c26b4a2c82"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import col, when, trim, to_date

# 1. Bronze tabloyu oku
bronze_df = spark.read.table("bronze_bookings")

# 2. Temizlik İşlemleri
# Hata almamak için subset kısmına senin tablanda kesin olan kolonları yazdım
silver_df = (bronze_df
    # Metin kolonlarını temizle (trim)
    .select([trim(col(c)).alias(c) if t == "string" else col(c) for c, t in bronze_df.dtypes])
    
    # Mükerrer satırları sil
    .dropDuplicates()
    
    # Kritik kolonlarda boş olanları sil 
    # (Hata mesajındaki listeye göre güncelledim: hotel_id, adults, checkin_date)
    .dropna(subset=["hotel_id", "adults", "checkin_date"])
    
    # Silver tablosuna özel: checkin_date'i gerçek tarih formatına çevirelim
    .withColumn("checkin_date", to_date(col("checkin_date")))
)

# 3. Silver Tablo olarak kaydet
silver_df.write.format("delta").mode("overwrite").saveAsTable("silver_bookings")

print("Silver tablo senin kolon isimlerine göre başarıyla oluşturuldu!")
display(silver_df.limit(5))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import col, when, count, isnan, udf
from pyspark.sql.types import DoubleType

# 1. Ham verileri oku
df_batch = spark.read.table("bronze_bookings")
df_stream = spark.read.table("bronze_streaming")

# 2. Batch ve Stream verilerini birleştir (Union)
# Not: Sütun isimlerinin aynı olduğundan emin olun
df_raw = df_batch.unionByName(df_stream, allowMissingColumns=True)

# --- TEMİZLİK BAŞLIYOR ---

# A. Tekilleştirme (Remove Duplicates)
df_cleaned = df_raw.dropDuplicates(["hotel_id", "city"]) # Benzersiz anahtarlarınız

# B. NULL Değer Yönetimi (Handle Missing Values)
# Önemli sütunlardaki boşlukları dolduruyoruz veya siliyoruz
df_cleaned = df_cleaned.na.fill({"star_rating": 0, "total_price": 0})
df_cleaned = df_cleaned.filter(col("hotel_id").isNotNull())

# C. Veri Tiplerini Düzeltme (Fix Data Types)
df_cleaned = df_cleaned.withColumn("total_price", col("total_price").cast("double"))
df_cleaned = df_cleaned.withColumn("star_rating", col("star_rating").cast("int"))

# D. Metin Standardizasyonu (Text Fix)
# Şehir isimlerini büyük harf yapalım ve boşlukları temizleyelim
from pyspark.sql.functions import upper, trim
df_cleaned = df_cleaned.withColumn("city", upper(trim(col("city"))))

# E. Outlier (Aykırı Değer) Analizi
# Örneğin fiyatı 0'dan küçük veya 1 milyon'dan büyük olan mantıksız verileri temizleyelim
df_cleaned = df_cleaned.filter((col("total_price") > 0) & (col("total_price") < 1000000))

# 3. Silver Tabloya Kaydet
df_cleaned.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("silver_bookings_all")

print("Step 5: Silver temizliği tamamlandı!")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 1. Dimension Table: dim_hotel
dim_hotel = df_cleaned.select(
    "hotel_id", "hotel_name", "hotel_type", "star_rating", "city", "country"
).distinct()
dim_hotel.write.format("delta").mode("overwrite").saveAsTable("gold_dim_hotel")

# 2. Dimension Table: dim_weather
df_weather = spark.read.table("bronze_weather_api")

# Hata veren satırı buradaki sütun isimleriyle (temperature, request_time) güncelledik:
dim_weather = df_weather.select(
    "city", 
    col("temperature").alias("temp"), 
    col("windspeed").alias("wind"), 
    col("request_time").alias("time")
).distinct()

dim_weather.write.format("delta").mode("overwrite").saveAsTable("gold_dim_weather")

# 3. Fact Table: fact_bookings
fact_bookings = df_cleaned.alias("b").join(
    dim_weather.alias("w"), 
    col("b.city") == col("w.city"), 
    "left"
).select(
    "b.hotel_id", 
    "b.city", 
    "b.total_price", 
    "w.temp",
    "w.wind",
    "w.time"
)

fact_bookings.write.format("delta").mode("overwrite").saveAsTable("gold_fact_bookings")

print("Step 7: Gold Layer başarıyla oluşturuldu! Tablolar pırıl pırıl.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
