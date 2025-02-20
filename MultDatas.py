import yfinance as yf
import json
import os
import subprocess
from datetime import datetime

#  Chemin HDFS pour stocker les fichiers JSON
hdfs_folder = "/user/azouz/raw_stock_data"

#  Liste des tickers à récupérer
tickers = [
    "AAPL", "MSFT", "GOOGL", "AMZN", "TSLA", "NVDA", "META", "NFLX", "AMD", "BA",
    "IBM", "INTC", "ORCL", "PYPL", "ADBE", "DIS", "WMT", "JPM", "V", "KO",
    "PEP", "MCD", "CSCO", "XOM", "CVX"
]

#  Nombre de copies pour atteindre 1 To (a ajuster)
num_copies = 500  

# Création du dossier HDFS 
print(f" Vérification du dossier HDFS : {hdfs_folder}...")
subprocess.run(["hdfs", "dfs", "-mkdir", "-p", hdfs_folder])

# Télécharger et multiplier les données
for ticker in tickers:
    print(f"\n Récupération des données pour {ticker}...")

    #  Télécharger les données
    try:
        df = yf.download(ticker, start="2022-01-01", end="2024-01-01", interval="1d")
    except Exception as e:
        print(f" Erreur lors du téléchargement de {ticker} : {e}")
        continue

    # Vérifier si des données existent
    if df.empty:
        print(f"⚠️ Aucune donnée récupérée pour {ticker}")
        continue

    df.reset_index(inplace=True)

    #  Convertir en JSON
    json_data = df.to_json(orient="records", date_format="iso")

    #  Multiplier et envoyer sur HDFS
    for i in range(num_copies):
        hdfs_path = f"{hdfs_folder}/{ticker}_copy{i}.json"

        try:
            #  Envoyer directement sur HDFS
            process = subprocess.run(
                ["hdfs", "dfs", "-put", "-", hdfs_path],
                input=json_data,  
                text=True,  
                check=True
            )

            print(f" Copie {i+1}/{num_copies} pour {ticker} stockée sur HDFS à {hdfs_path}")

        except subprocess.CalledProcessError as e:
            print(f" Erreur lors de la copie {i+1} pour {ticker} : {e}")

print("\n Récupération, duplication et stockage terminés !")

#  Vérifier la taille des données sur HDFS
print("\n Vérification de la taille totale sur HDFS :")
subprocess.run(["hdfs", "dfs", "-du", "-h", hdfs_folder])