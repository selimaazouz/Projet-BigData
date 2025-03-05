import json
import random
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.storagelevel import StorageLevel

# Configuration
num_copies = 500
local_input_path = "/home/augusto-mp/ENSTA/BigData/Projet-BigData/raw_stock_data"  
hdfs_input_path = "hdfs://localhost:9000/user/augusto-mp/raw_stock_data/"
hdfs_merged_path = "hdfs://localhost:9000/user/augusto-mp/merged_stock_data/"

# Initialize Spark session with configuration
spark = SparkSession.builder \
    .appName("StockDataProcessing") \
    .config("spark.executor.memory", "4g") \
    .config("spark.num.executors", "10") \
    .config("spark.executor.cores", "4") \
    .getOrCreate()

def add_noise_to_record(row):
    """Add random noise to the stock data record."""
    modified_row = row.asDict()
    for key in ["Open", "High", "Low", "Close"]:
        if key in modified_row:
            modified_row[key] *= (1 + random.uniform(-0.02, 0.02))  # ±2%
    if "Volume" in modified_row:
        modified_row["Volume"] *= (1 + random.uniform(-0.05, 0.05))  # ±5%
    return modified_row

def process_and_store_stock_data(file_path, ticker):
    """Process JSON files and store multiple copies on HDFS."""
    print(f"Processing data for {ticker} from file: {file_path}")

    try:
        # Read the JSON file
        with open(file_path, "r") as file:
            data = json.load(file)

        # Convert JSON data to a DataFrame
        df = spark.createDataFrame(data)

        # Cache DataFrame if reused
        df.cache()

        # Repartition DataFrame before writing to HDFS for parallelism
        df = df.repartition(10)

        # Save the original file to HDFS
        original_hdfs_path = f"{hdfs_input_path}{ticker}_original.json"
        df.write.mode("overwrite").json(original_hdfs_path)
        print(f"Original file stored on HDFS: {original_hdfs_path}")

        # Generate multiple copies with variations and store them on HDFS
        for i in range(1, num_copies + 1):
            # Apply noise to the DataFrame (without collecting data to the driver)
            modified_data_rdd = df.rdd.map(add_noise_to_record)

            # Save each copy as a separate file
            hdfs_file_path = f"{hdfs_input_path}{ticker}_{i}.json"
            spark.createDataFrame(modified_data_rdd).write.mode("overwrite").json(hdfs_file_path)
            print(f"Copy {i} of {ticker} stored on HDFS: {hdfs_file_path}")

    except Exception as e:
        print(f"⚠️ Error processing file {file_path} for {ticker}: {e}")

# List of tickers to process
tickers = ["AAPL", "MSFT", "GOOGL", "AMZN", "TSLA", "NVDA", "META", "NFLX", "AMD", "BA", "IBM", 
           "INTC", "ORCL", "PYPL", "ADBE", "DIS", "WMT", "JPM", "V", "KO", "PEP", "MCD", "CSCO", "XOM", "CVX"]

# Function to call process_and_store_stock_data for each ticker
def process_ticker(ticker):
    file_path = os.path.join(local_input_path, f"{ticker}.json")
    if os.path.exists(file_path):
        process_and_store_stock_data(file_path, ticker)
    else:
        print(f"⚠️ File not found for {ticker}: {file_path}")

# Move the execution of foreach() and RDD transformations outside of the lambda
for ticker in tickers:
    process_ticker(ticker)

print("All files and their copies have been processed and stored on HDFS!")
