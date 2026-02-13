
# Projet Big Data - NYC Yellow Taxi

## Youenn Bogaer
## Ewen Dano
## Victor ANDRE
##
## Description

Ce projet implémente une architecture Big Data complète pour le traitement et l'analyse des données de trajets des taxis jaunes de New York. L'objectif est de déployer une pipeline allant de la collecte brute sur un Data Lake jusqu'à la prédiction de prix via un modèle de Machine Learning.


Notre rapport entier se trouve dans reports/
## Architecture Technique

* **Data Lake** : Minio (stockage S3 local).
* **Data Warehouse** : PostgreSQL (modèle en flocon).
* **Traitement de données** : Spark / Scala.
* **Analyse et Visualisation** : Streamlit et Plotly.
* **Machine Learning** : XGBoost.

## Structure du Projet


```text
.
├── docker-compose.yml          # Docker
├── Docker/
├── ex01_data_retrieval/        # Exercice 1 : Récupération des donnée
│   ├── build.sbt
│   └── src/main/scala/
│       └── DownloadToMinio.scala # Streaming URL vers Minio
├── ex02_data_ingestion/        # Exercice 2 : Nettoyage et ingestion SQL
│   ├── build.sbt
│   └── src/main/scala/
│       ├── Branch1.scala       # Nettoyage et stockage Parquet
│       └── Branch2.scala       # Ingestion 
├── ex03_sql_table_creation/    # Exercice 3 : Modélisation SQL
│   ├── creation.sql            # Schéma des tables
│   └── insertion.sql           # Données de référence
├── ex04_dashboard/             # Exercice 4 : Dashboard Streamlit
│   ├── app.py                  # Interface
│   └── queries.py              # Requêtes SQL
├── ex05_ml_prediction_service/ # Exercice 5 : Machine Learning
│   ├── train.py                # Entraînement XGBoost
│   ├── test.py                 # Tests unitaires et validation
│   └── app_taxi.py             # App de prédiction
├── data/                       # Stockage des fichiers locaux
│   └── streamlit/              # Ressources (GeoJSON et lookup zones)
├── models/                     # Sauvegarde du modèle entraîné
├── notebooks/                  # Analyses exploratoires et tests ML
│   ├── 1.0-VA-data-exploration.ipynb
│   └── 2.0-VA-features-exploration-ML.ipynb
├── pyproject.toml              # Gestion des dépendances Python (uv)
├── uv.lock
├── reports/
│   ├──BigD_vAndre_yBogaer_eDano.pdf # Notre rapport
│   └──image_report/            #Contient toutes les images de notre rapport
└── README.md

```

## Installation et Utilisation

### 1. Infrastructure

Lancer les services Docker :

```bash
docker-compose up -d
```

### 2. Traitement des données (Spark)

* Se rendre dans `ex01_data_retrieval` et exécuter le script de récupération des données vers Minio.
* Se rendre dans `ex02_data_ingestion` et exécuter le script de filtrage et d'ingestion.
```bash
sbt run
```
Pour l'exercice après le sbt run sélectionner 1 pour le nettoyage et 2 pour l'ingestion
### 3. Base de données

Exécuter les scripts SQL dans `ex03_sql_table_creation` :

1. `creation.sql` (Structure des tables en flocon).
2. `insertion.sql` (Données de référence).

### 4. Applications Python (avec uv)

Le projet utilise **uv** pour la gestion de l'environnement.

Installer les dépendances :

```bash
uv sync
```

Lancer le dashboard analytique :

```bash
uv run streamlit run ex04_dashboard/app.py
```

Lancer le service de prédiction de prix :

```bash
uv run streamlit run ex05_ml_prediction_service/app_taxi.py
```

Entraîner le modèle :

```bash
uv run python ex05_ml_prediction_service/train.py
```
Tester le modèle :
```bash
uv run python ex05_ml_prediction_service/test.py
```
et rentrer dans le main les données pour le test

