import yfinance as yf
import json
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, input_file_name, regexp_extract
from pyspark.sql.functions import avg
import pandas as pd
import matplotlib.pyplot as plt
from pyspark.sql.functions import year, month, dayofweek, col
from pyspark.sql.functions import round
import numpy as np
#  Liste des tickers à récupérer
tickers = [ "AAPL", "MSFT", "GOOGL", "AMZN", "TSLA", "NVDA", "META", "NFLX", "AMD", "BA", "IBM", 
    "INTC", "ORCL", "PYPL", "ADBE", "DIS", "WMT", "JPM", "V", "KO", "PEP", "MCD", "CSCO", "XOM", "CVX"]

#  Dossier de sortie pour les fichiers JSON
output_folder = "raw_stock_data"
os.makedirs(output_folder, exist_ok=True)

#  Télécharger et enregistrer chaque action en JSON
for ticker in tickers:
    print(f" Récupération des données pour {ticker}...")

    # Télécharger les données
    df = yf.download(ticker, start="2000-01-01", end="2024-01-01", interval="1d")

    #  Réinitialiser l'index pour inclure "Date"
    df.reset_index(inplace=True)

    #  Correction des noms de colonnes pour éviter les tuples ("Date", "")
    df.columns = [col[0] if isinstance(col, tuple) else col for col in df.columns]

    #  Convertir en JSON propre
    json_data = df.to_json(orient="records", date_format="iso")

    #  Sauvegarde en fichier JSON
    json_file = os.path.join(output_folder, f"{ticker}.json")
    with open(json_file, "w") as f:
        f.write(json_data)

    print(f" Données stockées dans {json_file}")

#  Vérification en lisant un fichier JSON
sample_ticker = tickers[0]
sample_file = os.path.join(output_folder, f"{sample_ticker}.json")

with open(sample_file, "r") as f:
    sample_data = json.load(f)

#  Aperçu des données JSON après correction
print(f"Aperçu des données brutes de {sample_ticker} :")
print(json.dumps(sample_data[:5], indent=4)) 



#  Initialisation de Spark
spark = SparkSession.builder \
    .appName("StockDataProcessing") \
    .getOrCreate()

#  Chemin du dossier contenant les fichiers JSON
json_folder = "raw_stock_data"

#  Charger tous les fichiers JSON en ajoutant le nom du fichier comme colonne `source`
df_spark = spark.read.option("multiline", "true").json(f"{json_folder}/*.json")

#  Vérifier la structure avant correction
print("Aperçu des données brutes avec PySpark :")
df_spark.show(5)
df_spark.printSchema()

#  Ajouter la colonne `Ticker` en extrayant le nom du fichier JSON
df_spark = df_spark.withColumn("Source", input_file_name())  # Ajoute le chemin du fichier JSON complet
df_spark = df_spark.withColumn("Ticker", regexp_extract(col("Source"), r"([^/]+)\.json$", 1))  # Extrait le Ticker

#  Correction du format de la date
df_spark = df_spark.withColumn("Date", col("Date").cast("date"))

#  Vérifier  l'ajout de `Ticker`
df_spark.show(5)
df_spark.printSchema()

df_spark.describe(["Open", "High", "Low", "Close", "Volume"]).show()
df_spark = df_spark.filter(
    (col("Close") > 0) & (col("Close") < 100000) &
    (col("Open") > 0) & (col("Open") < 100000) &
    (col("High") > 0) & (col("High") < 100000) &
    (col("Low") > 0) & (col("Low") < 100000) &
    (col("Volume") >= 0)
)

#  Vérifier après le filtrage
df_spark.describe(["Open", "High", "Low", "Close", "Volume"]).show()

df_spark = df_spark.withColumn("Year", year(col("Date")))
df_spark = df_spark.withColumn("Month", month(col("Date")))
df_spark = df_spark.withColumn("DayOfWeek", dayofweek(col("Date")))

#  Vérification des nouvelles colonnes
df_spark.select("Date", "Year", "Month", "DayOfWeek").show(5)
df_spark = df_spark.withColumn("Daily_Return", round(((col("Close") - col("Open")) / col("Open")) * 100, 2))
df_spark = df_spark.withColumn("Intra_Day_Volatility", round(((col("High") - col("Low")) / col("Open")) * 100, 2))

#  Vérifier les nouvelles colonnes
df_spark.select("Date", "Ticker", "Open", "Close", "Daily_Return", "Intra_Day_Volatility").show(5)
#  Chemin de stockage
parquet_path = "organized_stock_data.parquet"

#  Sauvegarde des données sous format Parquet
df_spark.write.mode("overwrite").parquet(parquet_path)

print(f" Données nettoyées et organisées stockées en Parquet dans {parquet_path}")
from pyspark.sql.functions import avg

agg_df = df_spark.groupBy("Year", "Ticker").agg(avg("Close").alias("Avg_Close"))
agg_df.orderBy("Year", "Ticker").show(10)

#  Enregistrer les résultats en Parquet
agg_df.write.mode("overwrite").parquet("average_close_per_year.parquet")




#  Calculer la moyenne annuelle du prix de clôture pour chaque entreprise
agg_close_df = df_spark.groupBy("Year", "Ticker").agg(avg("Close").alias("Avg_Close"))

# Calculer la moyenne annuelle de la volatilité intra-journalière
agg_volatility_df = df_spark.groupBy("Year", "Ticker").agg(avg("Intra_Day_Volatility").alias("Avg_Volatility"))

#  Convertir en Pandas pour visualisation avec Matplotlib
df_close_pandas = agg_close_df.toPandas()
df_volatility_pandas = agg_volatility_df.toPandas()

#  Trier les données pour un bon affichage
df_close_pandas = df_close_pandas.sort_values(["Ticker", "Year"])
df_volatility_pandas = df_volatility_pandas.sort_values(["Year", "Ticker"])



plt.figure(figsize=(10, 6))

tickers = df_close_pandas["Ticker"].unique()
colors = plt.cm.viridis(np.linspace(0, 1, len(tickers)))  

for i, ticker in enumerate(tickers):
    subset = df_close_pandas[df_close_pandas["Ticker"] == ticker]
    plt.plot(subset["Year"], subset["Avg_Close"], label=ticker, color=colors[i])


plt.xlabel("Année")
plt.ylabel("Prix de Clôture Moyen")
plt.title("Évolution du Prix de Clôture Moyen par Entreprise")
plt.legend()
plt.grid(True)

plt.savefig("close_evolution.png")
print(" Graphique enregistré sous 'close_evolution.png'")
plt.figure(figsize=(10, 6))


years = df_volatility_pandas["Year"].unique()
colors = plt.cm.coolwarm(np.linspace(0, 1, len(years)))


for i, year in enumerate(years):
    subset = df_volatility_pandas[df_volatility_pandas["Year"] == year]
    plt.plot(subset["Ticker"], subset["Avg_Volatility"], label=year, color=colors[i])


plt.xlabel("Entreprise")
plt.ylabel("Volatilité Moyenne")
plt.title("Évolution de la Volatilité Moyenne par Année")
plt.legend()
plt.grid(True)


plt.savefig("volatility_evolution.png")
print("Graphique enregistré sous 'volatility_evolution.png'")
