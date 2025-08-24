# 📈 Tesla Stock Movement Based on Musk’s Tweets  

🚀 *San Jose State University – DATA226 Group Project*  

[![Python](https://img.shields.io/badge/Python-3776AB?logo=python&logoColor=white)]() 
[![Airflow](https://img.shields.io/badge/Apache%20Airflow-017CEE?logo=apache-airflow&logoColor=white)]() 
[![Snowflake](https://img.shields.io/badge/Snowflake-29B5E8?logo=snowflake&logoColor=white)]() 
[![dbt](https://img.shields.io/badge/dbt-FF694B?logo=dbt&logoColor=white)]() 
[![Superset](https://img.shields.io/badge/Apache%20Superset-3F6FFF?logo=apache&logoColor=white)]()  

---

## 📌 Abstract  
This project explores the relationship between **Elon Musk’s tweet sentiment** and **Tesla’s stock price fluctuations**.  
We built an **end-to-end data pipeline** using:  
- **Airflow** for orchestration  
- **Snowflake** for storage and transformations  
- **dbt** for ELT and modeling  
- **Superset** for visualization  

A **Linear Regression model** was developed to predict Tesla’s short-term stock price movements based on tweet sentiment and market features.  

---

## 🧐 Problem Statement  
Tesla’s stock often reacts to Elon Musk’s tweets, but investors lack a structured framework to measure this impact.  
This project integrates tweet sentiment data and stock market data into a **data-driven pipeline** to:  
- Detect correlations between sentiment and stock prices  
- Predict short-term stock movements  
- Provide **BI dashboards** for actionable insights  

---

## 📊 Dataset  
- **Tesla Stock Data (Yahoo Finance, via yFinance API)**  
  - Open, Close, High, Low, Volume (daily)  
- **Elon Musk Tweets (2023–2025)**  
  - Sentiment scored with **VADER**  
  - Features: sentiment score, tweet count, likes  

---

## ⚙️ System Architecture  

![System Architecture](docs/system_architecture.png)  

**Pipeline Layers:**  
1. **ETL Layer** → Extract Tesla stock + tweet sentiment → store in Snowflake staging  
2. **ELT Layer** → dbt models for feature engineering (lag, rolling averages, sentiment features)  
3. **Modeling Layer** → Linear regression for stock prediction  
4. **Visualization Layer** → Superset dashboards  

---

## 🛠️ Technical Stack  
- **Languages**: Python, SQL  
- **Frameworks**: dbt, VADER Sentiment Analysis  
- **Pipelines**: Apache Airflow, Snowflake  
- **Visualization**: Superset  
- **Versioning & Automation**: GitHub, SQL transactions  

---

## 📈 Predictive Model  
- Algorithm: **Linear Regression**  
- R² Score: **0.916**  
- RMSE: **19.03** | MAE: **14.58**  
- Key Features: `sentiment_score`, `tweet_count`, `lag_1_sentiment`, `avg_3d_sentiment`, `avg_7d_close`  
- Insight: **Short-term sentiment trends significantly influence Tesla’s stock price**  

---

## 📊 Visualizations  
- **Sentiment Trend vs Stock Price**  
- **3-day Sentiment Avg vs 7-day Stock Price**  
- **Tweet Volume vs Market Activity**  
- **Predicted vs Actual Closing Prices**  

*(Example Superset dashboards available in `/visuals`)*  

---

## 🔍 Key Findings  
- 📉 **Negative sentiment** → stock price tends to decline next day  
- 📈 **Positive sentiment** → leads to modest increases  
- ⏳ **Delayed reaction** → strongest impact seen the day after sentiment change  

---

## 📢 Recommendations  
- Sentiment is a **useful short-term signal** but not sufficient alone → combine with market indicators  
- **Snowflake + dbt** proved efficient for scalable modeling  
- Future work: real-time pipelines, multi-stock expansion, advanced ML models  

---

## 👩‍💻 Team Members  
- Jie Heng  
- Savitha Vijayarangan  
- **Lakshmi Bharathy Kumar**  
- Daniel Kim  
- Andreah Cruz  

---

## 🔗 Repository  
➡️ [GitHub Repo](https://github.com/Lakshmibharathy11/Elon_musk-s_tweet_impact)  

---
