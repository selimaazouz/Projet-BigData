import yfinance as yf
import json
import time
import random
import numpy as np
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, input_file_name, regexp_extract, year, month, dayofweek, round
from pyspark import SparkContext, SparkConf



num_copies = 500
hdfs_input_path = "hdfs://localhost:9000/user/azouz/raw_stock_data/"
hdfs_merged_path = "hdfs://localhost:9000/user/azouz/merged_stock_data/"

#  Liste des tickers à récupérer
tickers = ["AAPL", "MSFT", "GOOGL", "AMZN", "TSLA", "NVDA", "META", "NFLX", "AMD", "BA", "IBM", 
           "INTC", "ORCL", "PYPL", "ADBE", "DIS", "WMT", "JPM", "V", "KO", "PEP", "MCD", "CSCO", "XOM", "CVX"]

spark = SparkSession.builder.appName("StockDataProcessing").getOrCreate()

def download_and_store_stock_data(ticker):
    """Télécharge les données et stocke plusieurs copies sur HDFS, avec un délai pour éviter le blocage."""
    print(f"Récupération des données pour {ticker}...")

    # Pause plus longue pour éviter le blocage (10 secondes)
    time.sleep(10)

    try:
        # Télécharger les données depuis Yahoo Finance
        df = yf.download(ticker, start="2000-01-01", end="2024-01-01", interval="1d")

        if df.empty:
            print(f" Aucune donnée pour {ticker}")
            return

        # Réinitialiser l'index pour inclure "Date"
        df.reset_index(inplace=True)
        df.columns = [col[0] if isinstance(col, tuple) else col for col in df.columns]

        # Convertir en JSON
        original_json = df.to_json(orient="records", date_format="iso")

        #   Sauvegarder le fichier original sur HDFS
        original_hdfs_path = f"{hdfs_input_path}{ticker}_original.json"
        spark.createDataFrame([(original_json,)], ["json_string"]) \
            .write.mode("overwrite").text(original_hdfs_path)
        print(f" Fichier original stocké sur HDFS : {original_hdfs_path}")

        #  Générer plusieurs copies avec des variations et les stocker sur HDFS
        for i in range(1, num_copies + 1):
            modified_data = json.loads(original_json)

            # Ajouter un bruit aléatoire pour différencier les copies
            for record in modified_data:
                for key in ["Open", "High", "Low", "Close"]:
                    if key in record:
                        record[key] *= (1 + random.uniform(-0.02, 0.02))  # ±2%
                if "Volume" in record:
                    record["Volume"] *= (1 + random.uniform(-0.05, 0.05))  # ±5%

            # Stocker chaque copie sous un fichier séparé (ex: `AAPL_1.json`)
            hdfs_file_path = f"{hdfs_input_path}{ticker}_{i}.json"
            spark.createDataFrame([(json.dumps(modified_data),)], ["json_string"]) \
                .write.mode("overwrite").json(hdfs_file_path)

            print(f" Copie {i} de {ticker} stockée sur HDFS : {hdfs_file_path}")

    except Exception as e:
        print(f" Erreur lors du téléchargement pour {ticker}: {e}")

#  Exécution SÉQUENTIELLE 
for ticker in tickers:
    download_and_store_stock_data(ticker)

# Exécution en parallèle avec Spark
sc = SparkContext.getOrCreate(SparkConf().setAppName("StockDataProcessing"))
sc.parallelize(tickers).foreach(download_and_store_stock_data)


print(" Tous les fichiers originaux et leurs copies ont été stockés sur HDFS !")

