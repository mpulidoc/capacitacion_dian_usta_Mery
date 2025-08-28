# Load the total_pedidos from both tables
bronze_df = spark.table("sesion_5.bronze.conteo_pedidos_bronze")
plata_df = spark.table("sesion_5.silver.conteo_pedidos_plata")

# Calculate the total pedidos for each
total_bronze = bronze_df.agg({"total_pedidos": "sum"}).collect()[0][0]
total_plata = plata_df.agg({"total_pedidos": "sum"}).collect()[0][0]

# Determine the alert message
if total_plata > total_bronze:
    alert_message = "Duplicados en las tablas de bronce"
elif total_plata == total_bronze:
    alert_message = "Todo fue perfecto"
else:
    alert_message = "Revisar faltantes de bronce que no entraron a plata"

# Create a DataFrame for the alert
alert_df = spark.createDataFrame([(total_bronze, total_plata, alert_message)], 
                                 ["total_bronze", "total_plata", "alert_message"])
alert_df.write.mode("overwrite").saveAsTable("sesion_5.silver.alerta")

# Display the alert DataFrame
display(alert_df)