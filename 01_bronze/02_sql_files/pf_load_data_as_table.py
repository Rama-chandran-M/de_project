import re

# Databricks Python Notebook
files = dbutils.fs.ls("s3://aws-demo-bucket090/source_data")
print(files)
for f in files:
    # Everything inside the loop MUST be indented
    if f.name.endswith(".csv"):
        # 1. Clean Table Name
        table_name = (f.name.replace(".csv", "")
                            .lower()
                            .replace(" ", "_")
                            .replace("-", "_"))
        
        # 2. Read the file into a DataFrame
        df = spark.read.format("csv") \
                  .option("header", "true") \
                  .option("inferSchema", "true") \
                  .load(f.path)
        
        # 3. Clean Column Names
        new_column_names = [re.sub(r'[^a-z0-9]', '_', col.lower()).strip('_') for col in df.columns]
        print(new_column_names)
        df = df.toDF(*new_column_names)
        
        # 4. Write to table
        df.write.mode("overwrite").saveAsTable(f"`01_bronze_catalog`.raw_schema.{table_name}")
        
        print(f"Processed: {table_name}")
