# 🏏 Cricket Data Analytics Pipeline

![Databricks](https://img.shields.io/badge/Databricks-FF3621?style=for-the-badge&logo=Databricks&logoColor=white)
![PySpark](https://img.shields.io/badge/Apache_Spark-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white)
![Python](https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white)
![Delta Lake](https://img.shields.io/badge/Delta_Lake-00ADD8?style=for-the-badge&logo=delta&logoColor=white)

A complete end-to-end data pipeline built on Databricks that ingests live cricket match data from the Cricket API, processes it through Bronze-Silver-Gold layers, and provides business-ready analytics for insights on match types, teams, venues, and winners.

---

## 📋 Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Features](#features)
- [Tech Stack](#tech-stack)
- [Project Structure](#project-structure)
- [Setup Instructions](#setup-instructions)
- [Usage](#usage)
- [Analytics](#analytics)
- [Data Schema](#data-schema)
- [Future Enhancements](#future-enhancements)
- [Contributing](#contributing)
- [License](#license)

---

## 🎯 Overview

This project demonstrates a production-grade data engineering pipeline using **Databricks Lakehouse architecture**. It fetches real-time cricket match data from the Cricket API, stores it in **Unity Catalog**, and transforms it through multiple layers to provide actionable business insights.

**Key Highlights:**
- **Medallion Architecture**: Bronze → Silver → Gold layer pattern
- **Incremental Loading**: API pagination support to fetch multiple pages of matches
- **Schema Evolution**: Type-safe transformations with explicit schema definitions
- **Business Analytics**: Pre-computed aggregations for dashboards and reporting

---

## 🏗️ Architecture

```
┌─────────────────────┐
│   Cricket API       │
│  (cricapi.com)      │
└──────────┬──────────┘
           │
           │ HTTP GET with pagination
           │
           ▼
┌─────────────────────────────────────────────────────────┐
│                    BRONZE LAYER                         │
│  - Raw JSON storage                                     │
│  - Table: cricket_bronze_current_matches                │
│  - Volume: /Volumes/workspace/default/                  │
│            cricket_api_data_project                     │
└──────────┬──────────────────────────────────────────────┘
           │
           │ JSON parsing & schema validation
           │
           ▼
┌─────────────────────────────────────────────────────────┐
│                    SILVER LAYER                         │
│  - Cleaned & structured data                            │
│  - Table: cricket_silver_matches                        │
│  - Schema: match_id, teams, scores, dates, status       │
└──────────┬──────────────────────────────────────────────┘
           │
           │ Business aggregations & transformations
           │
           ▼
┌─────────────────────────────────────────────────────────┐
│                     GOLD LAYER                          │
│  - Business-ready analytics                             │
│  - Match type distribution                              │
│  - Team performance metrics                             │
│  - Venue utilization stats                              │
│  - Winner analysis                                      │
└─────────────────────────────────────────────────────────┘
```

---

## ✨ Features

- ✅ **Real-time Data Ingestion** from Cricket API
- ✅ **Pagination Support** - Fetches multiple pages of matches (configurable offsets)
- ✅ **Medallion Architecture** - Bronze/Silver/Gold data layers
- ✅ **Unity Catalog Integration** - Centralized governance and metadata
- ✅ **Delta Lake Storage** - ACID transactions and time travel
- ✅ **Schema Enforcement** - Type-safe transformations with explicit schemas
- ✅ **Incremental Processing** - Append mode with timestamps
- ✅ **Business Analytics** - Pre-computed KPIs and aggregations
- ✅ **Error Handling** - Graceful fallback for API pagination

---

## 🛠️ Tech Stack

| Component | Technology |
|-----------|------------|
| **Cloud Platform** | Databricks on AWS |
| **Compute** | Serverless Compute (CPU) |
| **Processing Engine** | Apache Spark (PySpark) |
| **Storage Format** | Delta Lake |
| **Data Catalog** | Unity Catalog |
| **Language** | Python 3.x |
| **API** | Cricket API (cricapi.com) |
| **Version Control** | Git/GitHub |

---

## 📁 Project Structure

```
cricket-api-data-project/
│
├── API ingestion and Bronze layer.py
│   └── Fetches data from Cricket API with pagination
│       Stores raw JSON in bronze table & volume
│
├── BronzetoSilverCleanMatchTable.py
│   └── Parses bronze JSON to structured silver table
│       Validates schema and handles type conversions
│
├── Gold Layer Analytics.py
│   └── Business analytics and aggregations
│       Match types, venues, teams, winners
│
└── README.md
    └── This file
```

### Unity Catalog Objects

```
workspace (catalog)
└── default (schema)
    ├── cricket_bronze_current_matches (table)
    ├── cricket_silver_matches (table)
    └── cricket_api_data_project (volume)
        └── current_matches_raw.json
```

---

## 🚀 Setup Instructions

### Prerequisites

- Databricks workspace (AWS, Azure, or GCP)
- Cricket API key from [cricapi.com](https://cricapi.com)
- Access to Serverless compute or a running cluster
- Unity Catalog enabled in your workspace

### Step 1: Clone the Repository

```bash
git clone https://github.com/YOUR_USERNAME/cricket-data-project.git
cd cricket-data-project
```

### Step 2: Import Notebooks to Databricks

1. Navigate to your Databricks workspace
2. Go to **Workspace** → **Users** → Your user folder
3. Create folder: `cricket-api-data-project`
4. Import all `.py` files as notebooks

### Step 3: Configure API Key

1. Open `API ingestion and Bronze layer` notebook
2. Update the API key in Cell 3:
   ```python
   API_KEY = 'YOUR_API_KEY_HERE'
   ```

### Step 4: Run the Pipeline

Execute notebooks in order:

1. **API ingestion and Bronze layer** - Fetches and stores raw data
2. **BronzetoSilverCleanMatchTable** - Cleans and structures data
3. **Gold Layer Analytics** - Generates business insights

---

## 📊 Usage

### Running the Complete Pipeline

```python
# 1. Bronze Layer - Data Ingestion
%run "./API ingestion and Bronze layer"

# 2. Silver Layer - Data Cleaning
%run "./BronzetoSilverCleanMatchTable"

# 3. Gold Layer - Analytics
%run "./Gold Layer Analytics"
```

### Querying the Data

```sql
-- View all matches
SELECT * FROM workspace.default.cricket_silver_matches;

-- Get T20 matches only
SELECT match_name, team_1, team_2, status 
FROM workspace.default.cricket_silver_matches
WHERE match_type = 't20';

-- Active matches
SELECT match_name, venue, status
FROM workspace.default.cricket_silver_matches
WHERE match_started = true AND match_ended = false;
```

---

## 📈 Analytics

### 1. Match Type Distribution
Analyzes the distribution of cricket formats (T20, Test, ODI).

**Sample Output:**
| Match Type | Total Matches |
|------------|---------------|
| test       | 8             |
| t20        | 4             |

### 2. Venue Utilization
Tracks which cricket grounds host the most matches.

**Sample Output:**
| Venue | Total Matches |
|-------|---------------|
| Kensington Oval, Bridgetown | 2 |
| Providence Stadium, Guyana | 2 |
| Headingley, Leeds | 1 |

### 3. Team Performance
Counts total matches played by each team.

**Sample Output:**
| Team | Total Matches |
|------|---------------|
| Guyana Amazon Warriors | 2 |
| Barbados Tridents | 2 |
| Lancashire | 1 |

### 4. Winner Analysis
Tracks team success rates from completed matches.

**Sample Output:**
| Winner | Total Wins |
|--------|------------|
| Guyana Amazon Warriors | 2 |
| Barbados Tridents | 1 |
| Trinbago Knight Riders | 1 |

### 5. Metadata Metrics
- **Distinct Match Types**: Count of unique formats
- **Distinct Venues**: Count of unique cricket grounds

---

## 📋 Data Schema

### Bronze Layer Schema

```python
├── source_api: string          # API endpoint URL
├── raw_data: string            # Complete JSON response
└── ingestion_time: timestamp   # Data ingestion timestamp
```

### Silver Layer Schema

```python
├── match_id: string            # Unique match identifier
├── match_name: string          # Full match name
├── match_type: string          # Format (t20, test, odi)
├── status: string              # Match status/result
├── venue: string               # Cricket ground name
├── match_date: date            # Match date
├── date_time_gmt: string       # Match datetime (GMT)
├── team_1: string              # First team name
├── team_2: string              # Second team name
├── score_1: string             # Team 1 score
├── score_2: string             # Team 2 score
├── match_started: boolean      # Whether match has started
├── match_ended: boolean        # Whether match has ended
└── loaded_at: timestamp        # Silver layer load time
```

---

## 🔮 Future Enhancements

- [ ] **Incremental Loading**: Add deduplication logic for repeated API calls
- [ ] **Player Statistics**: Extend to include individual player performance
- [ ] **Series Tracking**: Add series-level analytics
- [ ] **Delta Live Tables**: Convert to DLT pipeline for automated orchestration
- [ ] **Real-time Streaming**: Switch to streaming ingestion for live updates
- [ ] **Dashboard Integration**: Build Lakeview dashboard for visualization
- [ ] **Data Quality Checks**: Add expectations and validation rules
- [ ] **Historical Data**: Backfill historical match data
- [ ] **ML Models**: Predict match outcomes using historical patterns
- [ ] **Alerting**: Set up alerts for specific match events

---

## 🤝 Contributing

Contributions are welcome! Please follow these steps:

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/AmazingFeature`)
3. Commit your changes (`git commit -m 'Add some AmazingFeature'`)
4. Push to the branch (`git push origin feature/AmazingFeature`)
5. Open a Pull Request

---

## 📝 License

This project is licensed under the MIT License 

---

## 👤 Author

**Harshad Nagpure**
- GitHub: [@harry3201](https://github.com/harry3201_)
- 

---

## External Tools :

- [Cricket API](https://cricapi.com) for providing cricket match data
- [Databricks](https://databricks.com) for the Lakehouse platform
- [Apache Spark](https://spark.apache.org) for distributed processing
- [Delta Lake](https://delta.io) for reliable data storage

---

---

<div align="center">

**⭐ If you found this project helpful, please give it a star! ⭐**

Made with ❤️ and ☕ using Databricks

</div>
