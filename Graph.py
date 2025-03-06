from pyspark.sql import SparkSession
import matplotlib.pyplot as plt
import pandas as pd

tickers = ["AAPL", "MSFT", "GOOGL", "AMZN", "TSLA", "NVDA", "META", "NFLX", "AMD", "BA", "IBM", 
           "INTC", "ORCL", "PYPL", "ADBE", "DIS", "WMT", "JPM", "V", "KO", "PEP", "MCD", "CSCO", "XOM", "CVX"]



spark = SparkSession.builder.appName("StockDataVisualization").getOrCreate()

hdfs_merged_path = "hdfs://localhost:9000/user/azouz/merged_stock_data/"

def plot_stock_data(ticker):
    """Charge les données fusionnées depuis HDFS et génère une courbe."""
    print(f" Génération du graphique pour {ticker}...")

    try:
        # Lire le fichier fusionné depuis HDFS
        df_spark = spark.read.json(f"{hdfs_merged_path}{ticker}_merged.json")

        # Convertir en Pandas
        df_pandas = df_spark.toPandas()

        # Vérifier si les colonnes essentielles existent
        if "Date" not in df_pandas.columns or "Close" not in df_pandas.columns:
            print(f" Colonnes manquantes pour {ticker}, impossible de tracer.")
            return

        # Trier par date pour un affichage correct
        df_pandas["Date"] = pd.to_datetime(df_pandas["Date"])
        df_pandas.sort_values("Date", inplace=True)

        plt.figure(figsize=(10, 5))
        plt.plot(df_pandas["Date"], df_pandas["Close"], label=f"{ticker} - Prix de Clôture")
        plt.xlabel("Date")
        plt.ylabel("Prix de Clôture (USD)")
        plt.title(f"Évolution du Prix de Clôture - {ticker}")
        plt.legend()
        plt.grid()

       
        output_image = f"{ticker}_plot.png"
        plt.savefig(output_image)
        print(f" Graphique enregistré : {output_image}")
        plt.show()

    except Exception as e:
        print(f" Erreur lors de la génération du graphique pour {ticker}: {e}")

#  Générer un graphique pour chaque entreprise fusionnée
for ticker in tickers:
    plot_stock_data(ticker)

print(" Tous les graphiques ont été générés !")


from pyspark.sql.functions import col, year, avg


spark = SparkSession.builder.appName("StockDataAnalysis").getOrCreate()


parquet_path = "hdfs://localhost:9000/user/azouz/processed_stock_data.parquet"
df_spark = spark.read.parquet(parquet_path)

# Vérifier si les colonnes nécessaires existent
required_columns = ["Date", "Close", "High", "Low", "Open", "Ticker"]
for col_name in required_columns:
    if col_name not in df_spark.columns:
        raise ValueError(f"Colonne manquante : {col_name}")

# Ajouter la colonne Year
df_spark = df_spark.withColumn("Year", year(col("Date")))

# Calculer la moyenne annuelle du prix de clôture pour chaque entreprise
agg_close_df = df_spark.groupBy("Year", "Ticker").agg(avg("Close").alias("Avg_Close"))

# Calculer la moyenne annuelle de la volatilité intra-journalière
agg_volatility_df = df_spark.groupBy("Year", "Ticker").agg(avg(((col("High") - col("Low")) / col("Open") * 100)).alias("Avg_Volatility"))

# Convertir en Pandas pour visualisation avec Matplotlib
df_close_pandas = agg_close_df.toPandas()
df_volatility_pandas = agg_volatility_df.toPandas()


df_close_pandas = df_close_pandas.sort_values(["Ticker", "Year"])
df_volatility_pandas = df_volatility_pandas.sort_values(["Year", "Ticker"])

# Graphique de l'évolution du prix de clôture moyen
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

# Graphique de l'évolution de la volatilité moyenne
plt.figure(figsize=(12, 6))
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
plt.show()

print(" Graphiques générés et enregistrés avec succès !")

