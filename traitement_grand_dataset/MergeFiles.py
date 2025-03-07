
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("StockDataMerging").getOrCreate()

hdfs_input_path = "hdfs://localhost:9000/user/azouz/raw_stock_data/"
hdfs_merged_path = "hdfs://localhost:9000/user/azouz/merged_stock_data/"

#  Liste des tickers à fusionner
tickers = ["AAPL", "MSFT", "GOOGL", "AMZN", "TSLA", "NVDA", "META", "NFLX", "AMD", "BA", "IBM", 
           "INTC", "ORCL", "PYPL", "ADBE", "DIS", "WMT", "JPM", "V", "KO", "PEP", "MCD", "CSCO", "XOM", "CVX"]

def merge_stock_data(ticker):
    """Fusionne toutes les copies d'un même ticker en un seul fichier JSON."""
    print(f" Fusion des fichiers JSON pour {ticker}...")

    try:
        # Lire tous les fichiers JSON liés à ce ticker
        df_spark = spark.read.option("multiline", "true").json(f"{hdfs_input_path}{ticker}_*.json")

        # Sauvegarder le fichier fusionné sur HDFS
        merged_hdfs_path = f"{hdfs_merged_path}{ticker}_merged.json"
        df_spark.write.mode("overwrite").json(merged_hdfs_path)

        print(f" Données fusionnées pour {ticker} stockées sur HDFS : {merged_hdfs_path}")

    except Exception as e:
        print(f" Erreur lors de la fusion pour {ticker}: {e}")

#  Exécuter la fusion pour chaque ticker
for ticker in tickers:
    merge_stock_data(ticker)

print(" Tous les fichiers ont été fusionnés et stockés sur HDFS !")
