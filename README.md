# 🏀 NBA Betting Dashboard

This project is a data-driven NBA dashboard built to analyze player performance trends for smarter parlay decisions. It automatically pulls player stats from the NBA API, stores the data locally, and displays it in an interactive Streamlit dashboard.

---

## 🚀 Features

- 🔁 Pulls recent game logs for every active NBA player
- 🧠 Calculates average points, rebounds, assists over the last 5 games
- 📅 Supports pulling data by date (e.g., yesterday's games)
- 📊 Streamlit dashboard to view and filter players
- 📝 Logs all failed/successful fetches with full visibility
- 🔧 Pluggable architecture — ready for AI predictions or prop line overlays

---

## 📁 Project Structure

```
nba_odds/
├── src/
│   ├── data_pipeline.py         # Main ETL script
│   ├── dashboard.py             # Streamlit dashboard
│   ├── logger.py                # Custom logging module
│   └── nba_utils.py             # NBA API helper functions
├── data/
│   └── player_logs/             # CSVs saved daily with player stats
├── .venv/                       # Virtual environment
├── requirements.txt
└── README.md
```

---

## ⚙️ Setup Instructions

### 1. Clone the repo
```bash
git clone https://github.com/your-username/nba-odds-dashboard.git
cd nba-odds-dashboard
```

### 2. Create a virtual environment
```bash
python -m venv .venv
source .venv/bin/activate        # On Windows: .venv\Scripts\activate
```

### 3. Install dependencies
```bash
pip install -r requirements.txt
```

---

## 📈 Run the Data Pipeline

### Pull stats for yesterday’s games:
```bash
python src/data_pipeline.py
```

This will fetch and save data into `data/player_logs/`.

---

## 🖥️ Launch the Dashboard

```bash
streamlit run src/dashboard.py
```

Open [http://localhost:8501](http://localhost:8501) to explore the dashboard.

---

## 🧪 Example Use Cases

- Identify players averaging 20+ points over their last 5 games
- Spot value picks for parlays by comparing trends vs props
- Export recent player logs for deeper custom analysis

---

## 🛠️ Coming Soon

- 🧮 Prop line prediction overlay
- 🔍 Trend graphs with rolling averages
- 🤖 AI-powered summaries of each player's trajectory
- 📬 Slack alerts for standout player performances

---

## 🤝 Contributing

Want to add odds scraping, deploy to the cloud, or build an AI model on top? Let’s collaborate.

Feel free to fork, raise issues, or drop suggestions in pull requests.

---

## 📅 Current Season

The pipeline pulls from the active NBA season (`2024-25`). During the offseason, it will gracefully fall back to the most recent available data.

---

## 🧠 Credits

Built with:
- `nba_api` for stat scraping
- `pandas` + `streamlit` for analytics and UI
- `black`, `flake8`, `ruff` for code quality

---

## 🏁 License

MIT — free to use, adapt, and build on.