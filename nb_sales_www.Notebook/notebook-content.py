# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "8bf74e01-6d02-4ca2-8aaa-0e9bf0f8ea22",
# META       "default_lakehouse_name": "Bronze_Landing",
# META       "default_lakehouse_workspace_id": "c2cdc41a-a16f-43dc-b3c5-239b42c55a8e",
# META       "known_lakehouses": [
# META         {
# META           "id": "8bf74e01-6d02-4ca2-8aaa-0e9bf0f8ea22"
# META         },
# META         {
# META           "id": "b92b6d21-45bf-4a23-9fcb-9471f00e1af1"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# ## Tablas sin tiempo complejo (materializar parquets en Tablas)


# CELL ********************

## Leer los ficheros parquet que son resultado de la importacion anterior

## leer archivos de cada carpeta

df_sales = spark.read.parquet("Files/fsales/")
df_date = spark.read.parquet("Files/f_Date/")
df_stock = spark.read.parquet("Files/d_Stock/")
df_employee = spark.read.parquet("Files/d_employee/")
df_customer = spark.read.parquet("Files/d_Customer/")


display(df_sales)
display(df_date)
display(df_stock)
display(df_employee)
display(df_customer)



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# guardar los dataframes como tablas

df_sales.write.format("delta").mode("overwrite").saveAsTable("Bronze_Landing.retail_sales")
df_date.write.format("delta").mode("overwrite").saveAsTable("Bronze_Landing.retail_date")
df_stock.write.format("delta").mode("overwrite").saveAsTable("Bronze_Landing.retail_stock")
df_employee.write.format("delta").mode("overwrite").saveAsTable("Bronze_Landing.retail_employee")
df_customer.write.format("delta").mode("overwrite").saveAsTable("Bronze_Landing.retail_customer")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
