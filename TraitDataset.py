from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, month, dayofweek, round, avg, lit


spark = SparkSession.builder.appName("StockDataProcessing").getOrCreate()


hdfs_merged_path = "hdfs://localhost:9000/user/azouz/merged_stock_data/"
parquet_output_path = "hdfs://localhost:9000/user/azouz/processed_stock_data.parquet"
avg_close_output_path = "hdfs://localhost:9000/user/azouz/avg_close_per_year.parquet"
avg_volatility_output_path = "hdfs://localhost:9000/user/azouz/avg_volatility_per_year.parquet"


tickers = ["AAPL", "MSFT", "GOOGL", "AMZN", "TSLA", "NVDA", "META", "NFLX", "AMD", "BA", "IBM", 
           "INTC", "ORCL", "PYPL", "ADBE", "DIS", "WMT", "JPM", "V", "KO", "PEP", "MCD", "CSCO", "XOM", "CVX"]

all_data = []

for ticker in tickers:
    print(f" Traitement des données pour {ticker}...")
    try:
        #  Lire les fichiers JSON fusionnés
        df_spark = spark.read.json(f"{hdfs_merged_path}{ticker}_merged.json")

        #  Ajouter une colonne `Ticker`
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

        # Calcul des indicateurs financiers
        df_spark = df_spark.withColumn("Daily_Return", round(((col("Close") - col("Open")) / col("Open")) * 100, 2))
        df_spark = df_spark.withColumn("Intra_Day_Volatility", round(((col("High") - col("Low")) / col("Open")) * 100, 2))

        all_data.append(df_spark)

    except Exception as e:
        print(f"⚠️ Erreur lors du traitement pour {ticker}: {e}")

#  Fusion des données de tous les tickers
if all_data:
    final_df = all_data[0]
    for df in all_data[1:]:
        final_df = final_df.union(df)

    # Sauvegarde des données traitées en Parquet
    final_df.write.mode("overwrite").parquet(parquet_output_path)
    print(f" Données traitées stockées en Parquet : {parquet_output_path}")

    #  Calcul des moyennes annuelles
    agg_close_df = final_df.groupBy("Year", "Ticker").agg(avg("Close").alias("Avg_Close"))
    agg_volatility_df = final_df.groupBy("Year", "Ticker").agg(avg("Intra_Day_Volatility").alias("Avg_Volatility"))

    #  Sauvegarde des résultats en Parquet
    agg_close_df.write.mode("overwrite").parquet(avg_close_output_path)
    agg_volatility_df.write.mode("overwrite").parquet(avg_volatility_output_path)

    print(" Analyses enregistrées avec succès !")

