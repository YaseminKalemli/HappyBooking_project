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

# 1. Klasördeki CSV'yi oku
df = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("Files/bronze/hotel_raw_batch.csv")

# 2. Veriyi 'bronze_bookings' adıyla bir tablo olarak kaydet
df.write.format("delta").mode("overwrite").saveAsTable("bronze_bookings")

print("Tebrikler! İlk Bronze tablon oluştu.")
display(df.limit(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
