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
