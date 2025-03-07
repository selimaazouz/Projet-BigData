from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, month, dayofweek, round, avg, lit

spark = SparkSession.builder.appName("StockDataProcessing").getOrCreate()

hdfs_merged_path = "hdfs://localhost:9000/user/azouz/merged_stock_data/"
parquet_output_path = "hdfs://localhost:9000/user/azouz/processed_stock_data.parquet"
avg_close_output_path = "hdfs://localhost:9000/user/azouz/avg_close_per_year.parquet"
avg_volatility_output_path = "hdfs://localhost:9000/user/azouz/avg_volatility_per_year.parquet"

tickers = [
    "AAPL", "MSFT", "GOOGL", "AMZN", "TSLA", "NVDA", "META", "NFLX",
    "AMD", "BA", "IBM", "INTC", "ORCL", "PYPL", "ADBE", "DIS", "WMT",
    "JPM", "V", "KO", "PEP", "META", "NFLX", "AMD", "CSCO", "INTC", "XOM", 
    "CVX", "JNJ", "DIS", "PG", "BA", "IBM", "ORCL", "CVS", "PYPL", "HD", 
    "MCD", "WMT", "UNH"
]

spark = SparkSession.builder.appName("StockDataProcessing").getOrCreate()

all_data = []

for ticker in tickers:
    try:
        print(f"Traitement pour : {ticker}")

        # Lire le fichier JSON fusionné depuis HDFS
        df_spark = spark.read.json(f"hdfs://localhost:9000/user/azouz/merged_stock_data/{ticker}_merged.json")

        # Ajouter la colonne Ticker
        df_spark = df_spark.withColumn("Ticker", lit(ticker))

        # Filtrer les valeurs aberrantes
        df_spark = df_spark.filter(
            (col("Close") > 0) & (col("Close") < 100000) & (col("Volume") >= 0) &
            (col("Close") < 100000) & (col("Close") > 0) &
            (col("Volume") >= 0)
        )

        # Ajouter des colonnes temporelles
        df_spark = df_spark \
            .withColumn("Year", year(col("Date"))) \
            .withColumn("Month", month(col("Date"))) \
            .withColumn("DayOfWeek", dayofweek(col("Date")))

        # Ajouter des colonnes calculées
        df_spark = df_spark \
            .withColumn("Intra_Day_Volatility", round((col("High") - col("Low")) / col("Open") * 100, 2)) \
            .withColumn("Intra_Day_Volatility", round(((col("High") - col("Low")) / col("Open")) * 100, 2))

        # Ajouter le DataFrame traité à la liste
        all_data.append(df_spark)

    except Exception as e:
        print(f" Erreur lors du traitement pour {ticker}: {e}")

# Fusionner les données de tous les tickers
final_df = all_data[0]
for df_spark in all_data[1:]:
    final_df = final_df.union(df_spark)

# Enregistrer le DataFrame fusionné en Parquet
final_df.write.parquet("hdfs://localhost:9000/user/azouz/processed_stock_data.parquet")

# Calcul des moyennes annuelles
agg_close_df = final_df.groupBy("Year", "Ticker").agg(avg("Close").alias("Avg_Close"))
agg_volatility_df = final_df.groupBy("Year", "Ticker").agg(avg("Intra_Day_Volatility").alias("Avg_Volatility"))

# Enregistrer les moyennes annuelles en Parquet
agg_close_df.write.parquet("hdfs://localhost:9000/user/azouz/avg_close_per_year.parquet")
agg_volatility_df.write.parquet("hdfs://localhost:9000/user/azouz/avg_volatility_per_year.parquet")
