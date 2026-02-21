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

import requests
import pandas as pd
from pyspark.sql.functions import lit

# 1. API'den veri çek (Örn: Lizbon - En çok rezervasyon olan yerlerden biri)
# latitude=38.71, longitude=-9.13 (Lizbon koordinatları)
url = "https://api.open-meteo.com/v1/forecast?latitude=38.71&longitude=-9.13&current_weather=true"
response = requests.get(url)
data = response.json()

# 2. JSON verisini temizle ve Pandas DataFrame yap
weather_info = {
    "city": "Lisbon",
    "temperature": data['current_weather']['temperature'],
    "windspeed": data['current_weather']['windspeed'],
    "weathercode": data['current_weather']['weathercode'],
    "request_time": data['current_weather']['time']
}
df_weather = pd.DataFrame([weather_info])

# 3. Spark DataFrame'e çevir ve Bronze katmanına yaz
spark_weather = spark.createDataFrame(df_weather)
spark_weather.write.format("delta").mode("overwrite").saveAsTable("bronze_weather_api")

print("API verisi başarıyla çekildi!")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
