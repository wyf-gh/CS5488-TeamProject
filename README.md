# CS5488-TeamProject
Parallel Data Computing Solutions to Currency Exchange Rate Data

📖 Overview

In the contemporary globalized economy, currency exchange rates serve as vital indicators for trade, investment, and geopolitical stability. Traditional econometric models often face significant bottlenecks when processing the sheer volume and velocity of modern financial data.

This project investigates major currency pairs (GBP/USD, USD/CNY, and USD/JPY) by integrating high-frequency exchange rate data with macroeconomic metrics. We employ a parallel computing framework utilizing Hadoop, Pig, and Spark to execute complex data aggregation and regression modeling. Furthermore, we apply Deep Learning techniques (LSTM) to forecast future trends, providing actionable insights for risk management.

🚀 Key Features

Scalable Parallel Processing: Leverages the Hadoop ecosystem and Apache Spark's in-memory capabilities to process massive financial datasets, achieving a 5-10x speedup compared to sequential methods.

Hybrid Feature Engineering: Correlates high-frequency market ticks with lower-frequency macroeconomic indicators to decode the drivers of exchange rate volatility.

Deep Learning Forecasting: Implements Long Short-Term Memory (LSTM) networks to predict exchange rate trends with high accuracy.

Data Visualization: Includes a Python-based visualization suite to analyze the correlation between macroeconomic indicators and exchange rate fluctuations.

🛠 Tech Stack

Languages: Python (PySpark, TensorFlow/Keras, Pandas, Matplotlib)

Big Data Infrastructure:

Apache Hadoop (HDFS)

Apache Spark (In-Memory Computing)

Apache Pig (Data Flow Scripting)

Machine Learning: LSTM (Long Short-Term Memory) Neural Networks

📊 Results & Performance

According to our final report (November 2025), the project achieved the following milestones:

1. Computational Efficiency

By migrating from traditional sequential processing to a parallel architecture using Spark, we successfully accelerated data processing speeds by 5-10x, validating the efficiency of distributed computing for financial analytics.

2. Prediction Accuracy (LSTM)

USD/CNY: The model achieved high predictive accuracy with a Mean Absolute Percentage Error (MAPE) of 0.67%, proving that integrating Big Data processing with Deep Learning yields robust forecasting tools.

GBP/USD: The model showed a higher error rate (MAPE 11.11%). Analysis suggests this is because the current feature set relies heavily on structured macroeconomic data, which may miss short-term shocks driven by political events (e.g., Brexit policy shifts).

🔮 Future Work

Real-time Processing: Integrate Spark Streaming to handle real-time API feeds instead of the current batch processing mode.

NLP & Sentiment Analysis: Incorporate Natural Language Processing (NLP) to analyze unstructured sentiment data from news sources. This aims to capture market psychology and improve predictions for volatile pairs like GBP/USD.

Attention Mechanisms: Implement Attention mechanisms within the LSTM architecture to further enhance prediction performance by weighing critical time steps.

📝 Citation

If you find this project useful for your research, please cite our team report:

Team Project. (2025). Parallel Data Computing Solutions to Currency Exchange Rate Data.

Created by the Project Team, November 2025.
