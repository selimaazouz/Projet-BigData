from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, month, dayofweek, round, avg, lit
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np

# Initialisation de Spark
spark = SparkSession.builder.appName("StockDataProcessing").getOrCreate()

# Définition des chemins HDFS
hdfs_merged_path = "hdfs://localhost:9000/user/azouz/merged_stock_data/"
parquet_output_path = "hdfs://localhost:9000/user/azouz/processed_stock_data.parquet"
avg_close_output_path = "hdfs://localhost:9000/user/azouz/avg_close_per_year.parquet"
avg_volatility_output_path = "hdfs://localhost:9000/user/azouz/avg_volatility_per_year.parquet"

# Liste des tickers à analyser
tickers = ["AAPL", "MSFT", "GOOGL", "AMZN", "TSLA", "NVDA", "META", "NFLX", "AMD", "BA", "IBM",
           "INTC", "ORCL", "PYPL", "ADBE", "DIS", "WMT", "JPM", "V", "KO", "PEP", "MCD", "CSCO", "XOM", "CVX"]

all_data = []

#  Chargement et transformation des données
for ticker in tickers:
    print(f" Traitement des données pour {ticker}...")
    try:
        # Lire les fichiers JSON fusionnés
        df_spark = spark.read.json(f"{hdfs_merged_path}{ticker}_merged.json")

        # Ajouter une colonne `Ticker`
        df_spark = df_spark.withColumn("Ticker", lit(ticker))

        #  Filtrage des valeurs aberrantes
        df_spark = df_spark.filter(
            (col("Close") > 0) & (col("Close") < 100000) &
            (col("Open") > 0) & (col("Open") < 100000) &
            (col("High") > 0) & (col("High") < 100000) &
            (col("Low") > 0) & (col("Low") < 100000) &
            (col("Volume") >= 0)
        )

        #  Ajout d'informations temporelles
        df_spark = df_spark.withColumn("Year", year(col("Date")))
        df_spark = df_spark.withColumn("Month", month(col("Date")))
        df_spark = df_spark.withColumn("DayOfWeek", dayofweek(col("Date")))

        #  Calcul des indicateurs financiers
        df_spark = df_spark.withColumn("Daily_Return", round(((col("Close") - col("Open")) / col("Open")) * 100, 2))
        df_spark = df_spark.withColumn("Intra_Day_Volatility", round(((col("High") - col("Low")) / col("Open")) * 100, 2))

        #  Aperçu des données transformées
        print(f"Aperçu des données pour {ticker} :")
        df_spark.show(5)  # Affiche les 5 premières lignes

        all_data.append(df_spark)

    except Exception as e:
        print(f" Erreur lors du traitement pour {ticker}: {e}")

#  Fusion des données
if all_data:
    final_df = all_data[0]
    for df in all_data[1:]:
        final_df = final_df.union(df)

    #  Sauvegarde des données traitées en Parquet
    final_df.write.mode("overwrite").parquet(parquet_output_path)
    print(f" Données traitées stockées en Parquet : {parquet_output_path}")

    #  Statistiques descriptives
    print(" Statistiques descriptives après filtrage :")
    final_df.describe(["Open", "High", "Low", "Close", "Volume"]).show()

    #  Calcul des moyennes annuelles
    agg_close_df = final_df.groupBy("Year", "Ticker").agg(avg("Close").alias("Avg_Close"))
    agg_volatility_df = final_df.groupBy("Year", "Ticker").agg(avg("Intra_Day_Volatility").alias("Avg_Volatility"))

    #  Sauvegarde des résultats en Parquet
    agg_close_df.write.mode("overwrite").parquet(avg_close_output_path)
    agg_volatility_df.write.mode("overwrite").parquet(avg_volatility_output_path)
    print(" Analyses enregistrées avec succès !")

    #  Conversion en Pandas pour visualisation
    df_close_pandas = agg_close_df.toPandas()
    df_volatility_pandas = agg_volatility_df.toPandas()

    #  Trier les données pour affichage correct
    df_close_pandas = df_close_pandas.sort_values(["Ticker", "Year"])
    df_volatility_pandas = df_volatility_pandas.sort_values(["Year", "Ticker"])

    #  Graphique de l'évolution des prix de clôture moyens
    plt.figure(figsize=(12, 6))
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
    plt.show()

    #  Graphique de l'évolution de la volatilité moyenne
    plt.figure(figsize=(12, 6))
    years = df_volatility_pandas["Year"].unique()
    colors = plt.cm.coolwarm(np.linspace(0, 1, len(years)))

    for i, year in enumerate(years):
        subset = df_volatility_pandas[df_volatility_pandas["Year"] == year]
        plt.plot(subset["Ticker"], subset["Avg_Volatility"], label=year, color=colors[i])

    plt.xlabel("Entreprise")
    plt.ylabel("Volatilité Moyenne")
    plt.title(" Évolution de la Volatilité Moyenne par Année")
    plt.legend()
    plt.grid(True)
    plt.savefig("volatility_evolution.png")
    plt.show()

    print(" Graphiques générés et enregistrés avec succès !")
