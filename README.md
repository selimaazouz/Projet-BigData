# Analyse des Données Boursières avec Spark et yFinance

## Description du Projet
Ce projet vise à récupérer, traiter et analyser des données boursières historiques de plusieurs entreprises via l'API `yFinance`. Il utilise `Apache Spark` pour traiter ces données efficacement et `Matplotlib` pour visualiser les tendances boursières.

## Fonctionnalités du Projet
1. **Récupération des Données** :
   - Extraction des données journalières de 25 entreprises depuis 2000 jusqu'en 2024.
   - Stockage des données au format JSON.
2. **Traitement des Données avec Spark** :
   - Chargement des fichiers JSON en un `DataFrame Spark`.
   - Nettoyage des valeurs aberrantes et formatage des dates.
   - Ajout de nouvelles colonnes (`Year`, `Month`, `DayOfWeek`, `Daily_Return`, `Intra_Day_Volatility`).
   - Sauvegarde des données nettoyées en format Parquet.
3. **Analyse et Agrégation** :
   - Calcul de la moyenne annuelle du prix de clôture.
   - Calcul de la volatilité moyenne intra-journalière.
4. **Visualisation des Résultats** :
   - Tracé des évolutions du prix de clôture moyen pour chaque entreprise.
   - Tracé de l'évolution de la volatilité par année.

## Simulation de Données Massives (Big Data)
Au départ, nous avons testé notre code sur les données réelles de 2000 à 2024, permettant de tracer des courbes basées sur des données authentiques. Ensuite, pour tester la scalabilité du code, nous avons multiplié les données et les avons stockées sur **HDFS** afin d'exécuter nos traitements sur un grand volume de données (~1 To).

---

## Installation et Configuration

### 1. Prérequis
Avant d'exécuter ce projet, assurez-vous d'avoir installé :
- **Python 3**
- **pip**
- **Java 17** (Requis pour Apache Spark)
- **Maven**
- **Apache Spark**
- **Hadoop (stockage en HDFS)**

### 2. Installation des Dépendances
Exécutez la commande suivante pour installer les bibliothèques nécessaires :
```sh
pip install yfinance pandas matplotlib pyspark
```

### 3. Install Java
Installez le Java Development Kit (JDK) version 17.

Vérifiez que Java est installé :
```sh
java --version
```

Assurez-vous que la variable d'environnement `JAVA_HOME` est définie :
```sh
echo $JAVA_HOME
```

### 4. Install Maven
Vérifiez que Maven est installé :
```sh
mvn --version
```

### 5. Install Hadoop
Téléchargez et extrayez Hadoop (version 3.4.1) dans `./tmp/` :
```sh
mkdir -p tmp/
cd tmp/
curl https://dlcdn.apache.org/hadoop/common/stable/hadoop-3.4.1.tar.gz -o hadoop-3.4.1.tar.gz
tar -xvzf hadoop-3.4.1.tar.gz
cd ../
```

Définissez `HADOOP_HOME` dans votre shell (à répéter à chaque ouverture de shell) :
```sh
export HADOOP_HOME="$(realpath ./tmp/hadoop-3.4.1)"
```

À ce stade, le script suivant devrait s'exécuter avec succès :
```sh
./scripts/check-system.sh
```

### 6. Démarrer HDFS
Pour démarrer les démons HDFS sur une seule machine, configurez un cluster Hadoop pseudo-distribué où tous les composants (NameNode, DataNode, etc.) s'exécutent sur une seule machine.

#### Configurer Hadoop
Mettez à jour les fichiers de configuration dans le répertoire `$HADOOP_HOME/etc/hadoop` :

`$HADOOP_HOME/etc/hadoop/core-site.xml` :
```xml
<configuration>
  <property>
    <name>fs.defaultFS</name>
    <value>hdfs://localhost:9000</value>
  </property>
</configuration>
```

`$HADOOP_HOME/etc/hadoop/hdfs-site.xml` :
```xml
<configuration>
  <property>
    <name>dfs.replication</name>
    <value>1</value>
  </property>
</configuration>
```

#### Formater le système de fichiers HDFS
Formatez le NameNode avant de démarrer HDFS pour la première fois :
```sh
"$HADOOP_HOME/bin/hdfs" namenode -format
```

#### Démarrer les démons HDFS
Démarrez le NameNode dans un terminal :
```sh
"$HADOOP_HOME/bin/hdfs" namenode
```

Démarrez le DataNode dans un autre terminal (n'oubliez pas de définir `$HADOOP_HOME`) :
```sh
"$HADOOP_HOME/bin/hdfs" datanode
```

Créez votre répertoire "home" dans HDFS :
```sh
"$HADOOP_HOME/bin/hdfs" dfs -mkdir -p "/user/$USER"
```

À ce stade, le script suivant devrait s'exécuter avec succès :
```sh
./scripts/check-hdfs.sh
```

Accédez à l'interface web HDFS en ouvrant cette URL dans votre navigateur :
<http://localhost:9870>

---

## Organisation du Dossier
Le projet crée automatiquement les dossiers suivants :
```
├── raw_stock_data/        # Stocke les fichiers JSON
├── organized_stock_data.parquet/  # Données traitées au format Parquet
├── close_evolution.png    # Graphique de l'évolution des prix de clôture
├── volatility_evolution.png  # Graphique de l'évolution de la volatilité
```

## Exécution du Projet
Lancez le script Python avec :
```sh
python3 Traitement.py
```

## Explication du Fonctionnement du Code
### 1. Récupération des Données avec `yFinance`
- Le script récupère les données boursières pour chaque entreprise de 2000 à 2024.
- Les données sont enregistrées sous format JSON dans `raw_stock_data/`.

### 2. Chargement et Traitement des Données avec Spark
- Apache Spark charge tous les fichiers JSON et les fusionne dans un DataFrame.
- Ajout d'une colonne `Ticker` en extrayant le nom de fichier.
- Transformation des dates en type `date` et ajout de nouvelles colonnes.
- Nettoyage des valeurs aberrantes (prix incohérents, valeurs négatives).
- Sauvegarde des données nettoyées en `Parquet`.

### 3. Analyse des Données avec SparkSQL
- Agrégation des prix de clôture moyens par année et entreprise.
- Calcul de la volatilité intra-journalière moyenne.
- Résultats exportés en `Parquet`.

### 4. Visualisation des Données avec Matplotlib
- **Évolution du prix de clôture moyen par entreprise** : Chaque courbe représente une entreprise.
- **Évolution de la volatilité moyenne par année** : Chaque courbe représente une année.
- Sauvegarde des graphiques sous `close_evolution.png` et `volatility_evolution.png`.

### 5. Multiplication et Stockage des Données sur HDFS
Afin de tester notre traitement sur un grand volume de données, nous avons multiplié les fichiers JSON téléchargés et les avons stockés sur **HDFS**.
- Création du répertoire HDFS :
```sh
hdfs dfs -mkdir -p /user/azouz/raw_stock_data
```
- Multiplication et envoi des données sur HDFS :
```sh
python3 generate_large_dataset.py
```
- Vérification de la taille des données stockées :
```sh
hdfs dfs -du -h /user/azouz/raw_stock_data
```

## Résumé des Fichiers Importants
| Fichier | Description |
|---------|-------------|
| `Traitement.py` | Script principal exécutant toutes les étapes |
| `raw_stock_data/` | Dossier contenant les fichiers JSON téléchargés |
| `organized_stock_data.parquet` | Données nettoyées et transformées |
| `close_evolution.png` | Graphique de l'évolution du prix de clôture moyen |
| `volatility_evolution.png` | Graphique de l'évolution de la volatilité moyenne |
| `generate_large_dataset.py` | Script pour multiplier et stocker les données sur HDFS |

## Comment Vérifier les Résultats ?
1. **Vérifier les données brutes JSON** :
```sh
head raw_stock_data/AAPL.json
```
2. **Vérifier les fichiers Parquet** :
```sh
spark-shell
>>> df = spark.read.parquet("organized_stock_data.parquet")
>>> df.show(10)
```
3. **Ouvrir les graphiques générés** :
- Ouvrir `close_evolution.png`
- Ouvrir `volatility_evolution.png`

## Prédiction
