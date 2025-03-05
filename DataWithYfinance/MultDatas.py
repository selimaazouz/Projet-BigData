import json
import random
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, input_file_name, regexp_extract, year, month, dayofweek, round

# Configuration
num_copies = 1000
local_input_path = "/home/augusto-mp/ENSTA/BigData/Projet-BigData/raw_stock_data" 
hdfs_input_path = "hdfs://localhost:9000/user/augusto-mp/raw_stock_data/"
hdfs_merged_path = "hdfs://localhost:9000/user/augusto-mp/merged_stock_data/"

# Initialize Spark session
spark = SparkSession.builder.appName("StockDataProcessing").getOrCreate()

def process_and_store_stock_data(file_path, ticker):
    """Process JSON files and store multiple copies on HDFS."""
    print(f"Processing data for {ticker} from file: {file_path}")

    try:
        # Read the JSON file
        with open(file_path, "r") as file:
            data = json.load(file)

        # Convert JSON data to a DataFrame
        df = spark.createDataFrame(data)

        # Save the original file to HDFS
        original_hdfs_path = f"{hdfs_input_path}{ticker}_original.json"
        df.write.mode("overwrite").json(original_hdfs_path)
        print(f"Original file stored on HDFS: {original_hdfs_path}")

        # Generate multiple copies with variations and store them on HDFS
        for i in range(1, num_copies + 1):
            modified_data = df.rdd.map(lambda row: row.asDict()).collect()

            # Add random noise to differentiate copies
            for record in modified_data:
                for key in ["Open", "High", "Low", "Close"]:
                    if key in record:
                        record[key] *= (1 + random.uniform(-0.02, 0.02))  # ±2%
                if "Volume" in record:
                    record["Volume"] *= (1 + random.uniform(-0.05, 0.05))  # ±5%

            # Save each copy as a separate file 
            hdfs_file_path = f"{hdfs_input_path}{ticker}_{i}.json"
            spark.createDataFrame(modified_data).write.mode("overwrite").json(hdfs_file_path)
            print(f"Copy {i} of {ticker} stored on HDFS: {hdfs_file_path}")

    except Exception as e:
        print(f"⚠️ Error processing file {file_path} for {ticker}: {e}")

# List of tickers to process
tickers = ["AAPL", "MSFT", "GOOGL", "AMZN", "TSLA", "NVDA", "META", "NFLX", "AMD", "BA", "IBM", 
           "INTC", "ORCL", "PYPL", "ADBE", "DIS", "WMT", "JPM", "V", "KO", "PEP", "MCD", "CSCO", "XOM", "CVX"]
 
# Process each ticker's JSON file
for ticker in tickers:
    # Construct the file path for the ticker
    file_path = os.path.join(local_input_path, f"{ticker}.json")
    
    # Check if the file exists
    if os.path.exists(file_path):
        process_and_store_stock_data(file_path, ticker)
    else:
        print(f"⚠️ File not found for {ticker}: {file_path}")

print("All files and their copies have been processed and stored on HDFS!")