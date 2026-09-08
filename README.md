# 🏏 Cricket Data Analytics Pipeline

![Databricks](https://img.shields.io/badge/Databricks-FF3621?style=for-the-badge&logo=Databricks&logoColor=white)
![Apache Spark](https://img.shields.io/badge/Apache_Spark-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white)
![Python](https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white)
![Delta Lake](https://img.shields.io/badge/Delta_Lake-00ADD8?style=for-the-badge&logo=delta&logoColor=white)

> An end-to-end cricket data engineering project built with **Databricks, PySpark, Delta Lake, and Unity Catalog**.

This project ingests cricket match data from the **Cricket API**, processes it through a **Bronze → Silver → Gold medallion architecture**, and produces business-ready datasets for analyzing match formats, teams, venues, and winners.

---

## 📚 Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Key Features](#key-features)
- [Technology Stack](#technology-stack)
- [Project Structure](#project-structure)
- [Data Flow](#data-flow)
- [Getting Started](#getting-started)
  - [Prerequisites](#prerequisites)
  - [Clone the Repository](#1-clone-the-repository)
  - [Import the Notebooks](#2-import-the-notebooks-into-databricks)
  - [Configure the API Key](#3-configure-the-api-key)
  - [Run the Pipeline](#4-run-the-pipeline)
- [Usage](#usage)
- [Analytics](#analytics)
- [Data Schema](#data-schema)
- [Unity Catalog Objects](#unity-catalog-objects)
- [Future Enhancements](#future-enhancements)
- [Contributing](#contributing)
- [License](#license)
- [Author](#author)
- [External Tools](#external-tools)

---

<a id="overview"></a>

## 🎯 Overview

The goal of this project is to demonstrate a practical **data engineering workflow on Databricks** using a cricket data source.

The pipeline follows the **Medallion Architecture**:

| Layer | Purpose |
| --- | --- |
| **Bronze** | Stores raw API responses with ingestion metadata |
| **Silver** | Parses, cleans, validates, and structures match data |
| **Gold** | Produces business-focused aggregations and analytics |

### Highlights

- **Medallion Architecture** — Bronze → Silver → Gold
- **API Pagination** — Supports configurable pagination while fetching match data
- **Schema Enforcement** — Uses explicit schemas and type-safe transformations
- **Delta Lake** — Provides reliable table storage with ACID transactions and time travel
- **Unity Catalog** — Organizes and governs project data objects
- **Business Analytics** — Generates reusable metrics for matches, teams, venues, and winners
- **Error Handling** — Provides graceful handling for API pagination failures

---

<a id="architecture"></a>

## 🏗️ Architecture

```text
┌─────────────────────┐
│     Cricket API     │
│     cricapi.com     │
└──────────┬──────────┘
           │
           │ HTTP GET + pagination
           ▼
┌─────────────────────────────────────────────────────┐
│                    BRONZE LAYER                     │
│                                                     │
│  • Raw JSON response                                │
│  • cricket_bronze_current_matches                  │
│  • Raw data stored in a Databricks Volume           │
└──────────────────────┬──────────────────────────────┘
                       │
                       │ JSON parsing + validation
                       ▼
┌─────────────────────────────────────────────────────┐
│                    SILVER LAYER                     │
│                                                     │
│  • Cleaned and structured match data                │
│  • cricket_silver_matches                           │
│  • Typed match, team, score, date and status fields │
└──────────────────────┬──────────────────────────────┘
                       │
                       │ Transformations + aggregations
                       ▼
┌─────────────────────────────────────────────────────┐
│                     GOLD LAYER                      │
│                                                     │
│  • Match type distribution                          │
│  • Team performance                                 │
│  • Venue utilization                                │
│  • Winner analysis                                  │
└─────────────────────────────────────────────────────┘
```

---

<a id="key-features"></a>

## ✨ Key Features

| Feature | Description |
| --- | --- |
| **Live Data Ingestion** | Fetches current cricket match data from the Cricket API |
| **Pagination Support** | Retrieves multiple pages of API results using configurable offsets |
| **Medallion Architecture** | Separates raw, cleaned, and analytical data into Bronze, Silver, and Gold layers |
| **Delta Lake Storage** | Uses Delta tables for reliable data storage |
| **Unity Catalog** | Stores project tables and volumes under a centralized catalog |
| **Schema Enforcement** | Applies explicit schemas during transformation |
| **Incremental Processing** | Supports append-based processing with timestamps |
| **Business Analytics** | Produces pre-computed aggregations for reporting and analysis |
| **Error Handling** | Handles API pagination failures gracefully |

---

<a id="technology-stack"></a>

## 🛠️ Technology Stack

| Component | Technology |
| --- | --- |
| **Cloud Platform** | Databricks on AWS |
| **Compute** | Serverless Compute (CPU) |
| **Processing Engine** | Apache Spark / PySpark |
| **Storage Format** | Delta Lake |
| **Data Catalog** | Unity Catalog |
| **Language** | Python 3.x |
| **Data Source** | Cricket API (`cricapi.com`) |
| **Version Control** | Git / GitHub |

---

<a id="project-structure"></a>

## 📁 Project Structure

```text
cricket-api-data-project/
│
├── API ingestion and Bronze layer.py
│   └── Fetches data from the Cricket API with pagination
│       and stores raw JSON in the Bronze layer
│
├── BronzetoSilverCleanMatchTable.py
│   └── Parses Bronze JSON into the structured Silver table,
│       validates the schema, and handles type conversions
│
├── Gold Layer Analytics.py
│   └── Generates business analytics for match types,
│       venues, teams, and winners
│
└── README.md
    └── Project documentation
```

---

<a id="data-flow"></a>

## 🔄 Data Flow

```text
Cricket API
    │
    ▼
API Ingestion
    │
    ▼
Bronze ──► Raw JSON + ingestion metadata
    │
    ▼
Silver ──► Cleaned + structured match records
    │
    ▼
Gold ────► Business analytics + aggregations
```

---

<a id="getting-started"></a>

## 🚀 Getting Started

<a id="prerequisites"></a>

### Prerequisites

Before running the project, make sure you have:

- A Databricks workspace
- Unity Catalog enabled
- Access to Serverless Compute or a running Databricks cluster
- A Cricket API key from `cricapi.com`
- Git/GitHub access for the repository

<a id="1-clone-the-repository"></a>

### 1. Clone the Repository

```bash
git clone https://github.com/YOUR_USERNAME/cricket-data-project.git
cd cricket-api-data-project
```

> Replace `YOUR_USERNAME` and the repository name with your actual GitHub repository details.

<a id="2-import-the-notebooks-into-databricks"></a>

### 2. Import the Notebooks into Databricks

1. Open your Databricks workspace.
2. Navigate to **Workspace → Users → your user folder**.
3. Create a project folder named `cricket-api-data-project`.
4. Import the `.py` files as Databricks notebooks.

<a id="3-configure-the-api-key"></a>

### 3. Configure the API Key

Open the **API ingestion and Bronze layer** notebook and configure the API key where the ingestion code expects it.

For a personal project, do **not** commit a real API key to GitHub. Prefer Databricks secrets or another secure secret-management approach when available.

Example placeholder:

```python
API_KEY = "YOUR_API_KEY_HERE"
```

<a id="4-run-the-pipeline"></a>

### 4. Run the Pipeline

Run the notebooks in the following order:

```text
1. API ingestion and Bronze layer
          ↓
2. BronzetoSilverCleanMatchTable
          ↓
3. Gold Layer Analytics
```

---

<a id="usage"></a>

## 📊 Usage

### Running the Complete Pipeline

The notebooks can be executed individually or chained together:

```python
# 1. Bronze Layer - Data Ingestion
%run "./API ingestion and Bronze layer"

# 2. Silver Layer - Data Cleaning
%run "./BronzetoSilverCleanMatchTable"

# 3. Gold Layer - Analytics
%run "./Gold Layer Analytics"
```

### Querying the Silver Data

View all matches:

```sql
SELECT *
FROM workspace.default.cricket_silver_matches;
```

Filter for T20 matches:

```sql
SELECT
    match_name,
    team_1,
    team_2,
    status
FROM workspace.default.cricket_silver_matches
WHERE match_type = 't20';
```

Find active matches:

```sql
SELECT
    match_name,
    venue,
    status
FROM workspace.default.cricket_silver_matches
WHERE match_started = true
  AND match_ended = false;
```

---

<a id="analytics"></a>

## 📈 Analytics

The Gold layer provides business-oriented views of the processed cricket data.

### 1. Match Type Distribution

Analyzes the distribution of cricket formats such as T20, Test, and ODI.

| Match Type | Total Matches |
| --- | ---: |
| test | 8 |
| t20 | 4 |

### 2. Venue Utilization

Identifies venues hosting the highest number of matches.

| Venue | Total Matches |
| --- | ---: |
| Kensington Oval, Bridgetown | 2 |
| Providence Stadium, Guyana | 2 |
| Headingley, Leeds | 1 |

### 3. Team Performance

Counts matches played by each team.

| Team | Total Matches |
| --- | ---: |
| Guyana Amazon Warriors | 2 |
| Barbados Tridents | 2 |
| Lancashire | 1 |

### 4. Winner Analysis

Tracks wins from completed matches.

| Winner | Total Wins |
| --- | ---: |
| Guyana Amazon Warriors | 2 |
| Barbados Tridents | 1 |
| Trinbago Knight Riders | 1 |

### 5. Metadata Metrics

Additional metrics include:

- **Distinct Match Types** — Number of unique match formats
- **Distinct Venues** — Number of unique cricket grounds

> **Note:** The values above are sample outputs from the project data and may change as new API data is ingested.

---

<a id="data-schema"></a>

## 🧱 Data Schema

### Bronze Layer

```text
source_api:      string       # API endpoint URL
raw_data:        string       # Complete JSON response
ingestion_time:  timestamp    # Data ingestion timestamp
```

### Silver Layer

| Column | Type | Description |
| --- | --- | --- |
| `match_id` | string | Unique match identifier |
| `match_name` | string | Full match name |
| `match_type` | string | Match format (`t20`, `test`, `odi`) |
| `status` | string | Match status or result |
| `venue` | string | Cricket ground name |
| `match_date` | date | Match date |
| `date_time_gmt` | string | Match datetime in GMT |
| `team_1` | string | First team |
| `team_2` | string | Second team |
| `score_1` | string | Team 1 score |
| `score_2` | string | Team 2 score |
| `match_started` | boolean | Whether the match has started |
| `match_ended` | boolean | Whether the match has ended |
| `loaded_at` | timestamp | Silver-layer load timestamp |

---

<a id="unity-catalog-objects"></a>

## 🗂️ Unity Catalog Objects

```text
workspace (catalog)
└── default (schema)
    ├── cricket_bronze_current_matches  (table)
    ├── cricket_silver_matches          (table)
    └── cricket_api_data_project        (volume)
        └── current_matches_raw.json
```

---

<a id="future-enhancements"></a>

## 🔮 Future Enhancements

- [ ] Add deduplication logic for repeated API calls
- [ ] Add individual player statistics
- [ ] Add series-level tracking and analytics
- [ ] Convert the pipeline to Delta Live Tables (DLT)
- [ ] Explore real-time streaming ingestion
- [ ] Build a Lakeview dashboard for visualization
- [ ] Add data-quality expectations and validation rules
- [ ] Backfill and process historical match data
- [ ] Explore ML-based match outcome prediction
- [ ] Add alerts for selected match events

---

<a id="contributing"></a>

## 🤝 Contributing

This is primarily a personal project, but suggestions and improvements are welcome.

If you would like to contribute:

1. Fork the repository.
2. Create a feature branch.
   ```bash
   git checkout -b feature/AmazingFeature
   ```
3. Commit your changes.
   ```bash
   git commit -m "Add some AmazingFeature"
   ```
4. Push the branch.
   ```bash
   git push origin feature/AmazingFeature
   ```
5. Open a Pull Request.

---

<a id="license"></a>

## 📝 License

This project is licensed under the **MIT License**.

---

<a id="author"></a>

## 👤 Author

**Harshad Nagpure**

- GitHub: [@harry3201_](https://github.com/harry3201_)

---

<a id="external-tools"></a>

## 🔗 External Tools

This project uses the following external technologies and data sources:

- [Cricket API](https://cricapi.com) — Cricket match data
- [Databricks](https://www.databricks.com/) — Lakehouse platform
- [Apache Spark](https://spark.apache.org/) — Distributed data processing
- [Delta Lake](https://delta.io/) — Reliable data storage

---

<div align="center">

**⭐ If you found this project useful, consider giving the repository a star.**

Built with ❤️ and ☕ using Databricks, PySpark, and Delta Lake.

</div>
