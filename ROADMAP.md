# 🛣️ Project Roadmap: NBA Betting Dashboard

This roadmap outlines the major milestones and features planned for the NBA Betting Dashboard. The goal is to evolve from a simple data pipeline and dashboard into a smart, AI-powered parlay recommendation system.

---

## ✅ Phase 1: Foundation (Complete)

- [x] Pull daily player stats from the NBA API
- [x] Log successful/failed API calls
- [x] Save daily results into structured CSVs
- [x] Basic Streamlit dashboard for viewing stats
- [x] Filter dashboard by player and show averages

---

## 🧪 Phase 2: Exploration & Analysis

- [ ] Add date selector to load historical CSVs
- [ ] Add trendline charts for PTS, REB, AST
- [ ] Add filters for team, position, opponent
- [ ] Add sort and conditional formatting (e.g., highlight players averaging 20+ pts)
- [ ] Allow user to "bookmark" players for parlay planning

---

## 🤖 Phase 3: AI Prediction Insights

- [ ] Add basic model (RandomForest/XGBoost) for over/under predictions
- [ ] Predict likelihood of player hitting key stat lines (PTS > 20, REB > 6, etc.)
- [ ] Show model confidence % in dashboard
- [ ] Save predictions alongside raw stats for future training

---

## 📡 Phase 4: Live Odds Integration

- [ ] Pull in real-time player prop lines (FanDuel, Bet365, OddsAPI)
- [ ] Match odds with model predictions
- [ ] Compute expected value (EV) per leg
- [ ] Highlight bets with +EV

---

## 🧠 Phase 5: Smart Parlay Builder

- [ ] Ask user for risk profile: Conservative / Balanced / Aggressive
- [ ] Auto-generate 2–5 leg parlays using model confidence + EV
- [ ] Show overall expected value and payout estimate
- [ ] Allow manual tweaks to final parlay ticket

---

## 🏈 Phase 6: Multi-Sport Expansion

- [ ] Add support for NFL, Soccer, MLB props
- [ ] Normalize data formats for cross-sport analysis
- [ ] Allow dashboard toggling by league

---

## 🧱 Phase 7: Productization & Infra

- [ ] Move data from CSVs to PostgreSQL or SQLite
- [ ] Schedule backfills + daily pulls via Airflow or Prefect
- [ ] Deploy dashboard via Streamlit Cloud or Docker
- [ ] Explore replacing Streamlit with Vue + FastAPI backend for full-stack flexibility

---

## 📌 Bookmarked Ideas

- Expand beyond just today's/yesterday's players for wider parlay options
- Store more historical games per player (e.g. last 15 or full season)
- Add Slack or email alerts when high-confidence value bets are detected

---

## 💡 Final Vision

A fully automated betting assistant that:
- Pulls fresh stats + odds daily
- Uses AI to find high-value legs
- Asks your risk tolerance
- Spits out a smart, profitable parlay

Built to scale across sports and personalized to your betting style.