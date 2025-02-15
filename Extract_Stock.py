import yfinance as yf
import pandas as pd
import os
from concurrent.futures import ThreadPoolExecutor
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, avg

#  Définition des tickers à analyser
tickers = ["AAPL", "MSFT", "GOOGL", "AMZN", "TSLA", "NVDA", "FB", "NFLX"]
parquet_file = "stock_data.parquet"
hdfs_path = "hdfs://localhost:9000/data/stock/stock_data.parquet"  
# Fonction pour récupérer les données d'un ticker spécifique
def fetch_ticker_data(ticker):
    try:
        data = yf.download(ticker, start="2000-01-01", end="2024-01-01", interval="1d")
        data['Ticker'] = ticker
        data.reset_index(inplace=True)  
        return data
    except Exception as e:
        print(f" Erreur lors de la récupération de {ticker}: {e}")
        return None

#   Extraction des données en parallèle
with ThreadPoolExecutor(max_workers=10) as executor:
    results = list(executor.map(fetch_ticker_data, tickers))

#   Concaténation des données valides
all_data = pd.concat([result for result in results if result is not None])

#  Correction du type de Date avant d'enregistrer en Parquet
all_data["Date"] = all_data["Date"].astype(str)

#   Sauvegarde en Parquet (local)
all_data.to_parquet(parquet_file, index=False)

#   Vérification du fichier Parquet local
if os.path.exists(parquet_file):
    print(f" Le fichier {parquet_file} a été créé avec succès !")
    df_test = pd.read_parquet(parquet_file)
    print("Aperçu des données stockées localement :")
    print(df_test.head())
else:
    print(" Erreur : le fichier Parquet n'a pas été généré.")

#   Déplacer les données vers HDFS
os.system("hdfs dfs -mkdir -p /data/stock")
os.system(f"hdfs dfs -put -f {parquet_file} /data/stock/")
print(" Données transférées vers HDFS !")

#   Vérification du fichier dans HDFS
hdfs_check = os.popen("hdfs dfs -ls /data/stock/").read()
print(f" Contenu de HDFS :\n{hdfs_check}")

#   Initialisation de Spark et lecture des données depuis HDFS
spark = SparkSession.builder \
    .appName("StockDataAnalysis") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://localhost:9000") \
    .getOrCreate()

df_spark = spark.read.parquet(hdfs_path)

print(" Aperçu des données lues depuis HDFS avec Spark :")
df_spark.show(5)
df_spark.printSchema()

# Renommer les colonnes pour éviter les erreurs
for col_name in df_spark.columns:
    new_col_name = col_name.replace("(", "").replace(")", "").replace("'", "").replace(",", "").replace(" ", "_")
    df_spark = df_spark.withColumnRenamed(col_name, new_col_name)

#  Vérification après renommage
df_spark.printSchema()
df_spark.show(5)

#   Vérifier que "Date_" est bien une colonne Date
df_spark = df_spark.withColumn("Date_", col("Date_").cast("date"))

#  Sélection dynamique de la colonne "Close" pour l'agrégation
close_columns = [col_name for col_name in df_spark.columns if "Close" in col_name]

if close_columns:
    selected_close_col = close_columns[0]  
    print(f" Utilisation de la colonne : {selected_close_col}")

    #   Calcul d'agrégation avec PySpark : moyenne des prix de clôture par année
    agg_df = df_spark.groupBy(year(col("Date_")).alias("Year")).agg(avg(col(selected_close_col)).alias("Avg_Close"))

    #  Affichage du résultat
    agg_df.show()
else:
    print(" Erreur : Aucune colonne 'Close' trouvée ! Vérifiez les noms des colonnes.")


