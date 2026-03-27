import re

# Databricks Python Notebook
files = dbutils.fs.ls("s3://aws-demo-bucket090/source_data")
print(files)
for f in files:
    if f.name.endswith(".csv"):
        table_name = (f.name.replace(".csv", "")
                            .lower()
                            .replace(" ", "_")
                            .replace("-", "_"))
        
        df = spark.read.format("csv") \
                  .option("header", "true") \
                  .option("inferSchema", "true") \
                  .load(f.path)
        
        new_column_names = [re.sub(r'[^a-z0-9]', '_', col.lower()).strip('_') for col in df.columns]
        print(new_column_names)
        df = df.toDF(*new_column_names)
        
        spark.sql("create schema if not exists 01_bronze_catalog.raw_schema")
        df.write.mode("overwrite").saveAsTable(f"`01_bronze_catalog`.raw_schema.{table_name}")
        
        print(f"Processed: {table_name}")
