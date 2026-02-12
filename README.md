# 📊 Data Quality Pipeline - Retail Store Sales

## 🎯 Objectif

Pipeline complet de Data Quality pour auditer, nettoyer et monitorer la qualité des données de ventes (**Retail Store Sales**) basé sur les **6 piliers de la qualité des données**.
Le projet traite un dataset brut contenant des anomalies intentionnelles (valeurs manquantes, doublons, incohérences) pour simuler un environnement réel.

---

## 📁 Structure du Projet

```
data-quality-pipeline/
├── docker-compose.yml          # Services (MariaDB, Superset, OpenMetadata)
├── README.md                   # Ce fichier
├── data/
│   ├── raw/                    # Données brutes (Retail_Store_Sales.csv)
│   └── cleaned/                # Données nettoyées (Retail_Cleaned.csv)
├── scripts/
│   ├── generate_dataset.py     # Génération du dataset dirty
│   ├── import_data.py          # Import dans MariaDB (retail_raw)
│   ├── cleaning_pipeline.py    # Nettoyage et transformation
│   ├── great_expectations_validator.py  # Tests de validation
│   ├── sweetviz_profiling.py   # Comparaison Avant/Après
│   └── superset_init.sh        # Init Dashboard Superset
├── reports/
│   ├── sweetviz_compare_report.html # Rapport de profilage interactiv
│   └── cleaning_report.json    # Stats de nettoyage
├── sql/
│   ├── create_tables.sql       # Schéma (retail_raw, retail_cleaned)
│   └── quality_kpis.sql        # Calcul des métriques SQL
└── governance/
    └── asset_catalog.json      # Métadonnées
```

---

## 🚀 Installation et Démarrage

### Prérequis

- Python 3.10+
- Docker & Docker Compose

### 1. Démarrer les services

```bash
docker-compose up -d
```

*Services : MariaDB (3307), Superset (8088), OpenMetadata (8585)*

### 2. Exécuter le Pipeline

```bash
# Générer et importer les données
python scripts/generate_dataset.py
python scripts/import_data.py

# Nettoyer et valider
python scripts/cleaning_pipeline.py
python scripts/great_expectations_validator.py

# Générer le rapport de profilage
python scripts/sweetviz_profiling.py
```

### 3. Visualisation (Superset)

Accéder à [http://localhost:8088](http://localhost:8088) (admin/admin).
Initialiser la connexion :

```bash
docker exec dq_superset bash /app/superset_init.sh
```

---

## 📊 Les 6 Piliers de Qualité

| Pilier | Problème Identifié | Solution Appliquée | Score |
|--------|-------------------|--------------------|-------|
| **Complétude** | Manque Produit/Prix | Imputation (Unknown/Médiane) | 100% |
| **Exactitude** | Format Dates/Villes | Standardisation | 100% |
| **Validité** | Prix négatifs | Conversion Valeur Absolue | 100% |
| **Cohérence** | Total ≠ Prix * Qté | Recalcul strict | 100% |
| **Unicité** | Doublons Transactions | Déduplication | 100% |
| **Actualité** | Dates futures | Validation date | 100% |

---

## 📈 Dataset : Retail Store Sales (Dirty)

- **Source** : Génération synthétique (Python)
- **Volume** : ~15 300 lignes
- **Colonnes** : 11
- **Qualité Initiale** : ~8% d'erreurs injectées

### Colonnes Clés

- `Transaction_ID` : Unique après nettoyage
- `Product_Name` : Produit vendu
- `Total_Amount` : Montant de la transaction
- `City` : Ville du magasin

---

## 👤 Auteur

**ICOM - Master 2 BI & Analytics**  
Année universitaire 2025-2026  
**Étudiants :**  
Idir TABET  
Nassim TABET  
Mohammed ABBAOUI
