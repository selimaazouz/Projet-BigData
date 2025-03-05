import os
os.environ['TF_CPP_MIN_LOG_LEVEL'] = '3'  

import pandas as pd
import numpy as np
from sklearn.preprocessing import MinMaxScaler
from sklearn.model_selection import train_test_split
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import LSTM, Dense, Dropout
import matplotlib.pyplot as plt
from datetime import datetime
import json
from pyspark.sql import SparkSession

# Initialize Spark Session
spark = SparkSession.builder.appName("StockDataAnalysis").getOrCreate()

# Load the data from HDFS using PySpark
def load_data(hdfs_path):
    df_spark = spark.read.json(hdfs_path)  # Read JSON from HDFS
    df_pandas = df_spark.toPandas()  # Convert to Pandas DataFrame
    return df_pandas

# Data preprocessing
def preprocess_data(df):
    df['Date'] = pd.to_datetime(df['Date'])
    df = df.sort_values('Date')
    df.set_index('Date', inplace=True)
    
    df['MA5'] = df['Close'].rolling(window=5).mean()
    df['MA20'] = df['Close'].rolling(window=20).mean()
    df['Price_Change'] = df['Close'].pct_change()
    df['Price_Change_5d'] = df['Close'].pct_change(periods=5)
    df['Volume_Change'] = df['Volume'].pct_change()
    df['Volatility'] = (df['High'] - df['Low']) / df['Open']
    
    delta = df['Close'].diff()
    gain = delta.where(delta > 0, 0).rolling(window=14).mean()
    loss = -delta.where(delta < 0, 0).rolling(window=14).mean()
    rs = gain / loss
    df['RSI'] = 100 - (100 / (1 + rs))
    
    df.dropna(inplace=True)
    
    return df

# Feature engineering and sequence creation
def create_sequences(data, target_col, seq_length=60):
    xs, ys = [], []
    for i in range(len(data) - seq_length):
        x = data.iloc[i:(i + seq_length)].values
        y = data.iloc[i + seq_length][target_col]
        xs.append(x)
        ys.append(y)
    return np.array(xs), np.array(ys)

# Build and train the LSTM model
def build_lstm_model(input_shape):
    model = Sequential([
        LSTM(50, return_sequences=True, input_shape=input_shape),
        Dropout(0.2),
        LSTM(50, return_sequences=False),
        Dropout(0.2),
        Dense(1)
    ])
    
    model.compile(optimizer='adam', loss='mean_squared_error')
    return model

# Save the predictions
def plot_predictions(actual, predicted, title):
    plt.figure(figsize=(12, 6))
    plt.plot(actual, label='Actual')
    plt.plot(predicted, label='Predicted')
    plt.title(title)
    plt.legend()
    plt.savefig('META_prediction.png')
    plt.close()

# Main execution
def predict_stock_prices(hdfs_path, target_column='Close', forecast_days=30):
    df = preprocess_data(load_data(hdfs_path))
    
    features = df.columns.tolist()
    target_idx = features.index(target_column)
    scaler = MinMaxScaler(feature_range=(0, 1))
    scaled_data = scaler.fit_transform(df)
    
    seq_length = min(60, int(len(df) * 0.2))
    X, y = create_sequences(pd.DataFrame(scaled_data, columns=features), target_idx, seq_length)
    
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, shuffle=False)
    
    model = build_lstm_model((X_train.shape[1], X_train.shape[2]))
    model.fit(X_train, y_train, epochs=10, batch_size=32, validation_split=0.1, verbose=1)
    
    y_pred = model.predict(X_test)
    
    y_test_scaled = np.zeros((len(y_test), len(features)))
    y_pred_scaled = np.zeros((len(y_pred), len(features)))
    
    y_test_scaled[:, target_idx] = y_test
    y_pred_scaled[:, target_idx] = y_pred.flatten()
    
    y_test_orig = scaler.inverse_transform(y_test_scaled)[:, target_idx]
    y_pred_orig = scaler.inverse_transform(y_pred_scaled)[:, target_idx]
    
    plot_predictions(y_test_orig, y_pred_orig, f'Stock Price Prediction META - {target_column}')
    
    last_sequence = scaled_data[-seq_length:].reshape(1, seq_length, len(features))
    future_predictions = []
    
    for _ in range(forecast_days):
        next_pred = model.predict(last_sequence)
        next_point = np.zeros(len(features))
        next_point[target_idx] = next_pred[0, 0]
        future_pred_orig = scaler.inverse_transform(next_point.reshape(1, -1))[0, target_idx]
        future_predictions.append(future_pred_orig)
        next_point = next_point.reshape(1, 1, len(features))
        last_sequence = np.append(last_sequence[:, 1:, :], next_point, axis=1)
    
    last_date = df.index[-1]
    future_dates = pd.date_range(start=last_date + pd.Timedelta(days=1), periods=forecast_days)
    
    future_df = pd.DataFrame({
        'Date': future_dates,
        f'Predicted_{target_column}': future_predictions
    })
    future_df.set_index('Date', inplace=True)
    
    print("\nFuture predictions:")
    print(future_df)
    
    return {
        'model': model,
        'scaler': scaler,
        'future_predictions': future_df,
        'features': features,
        'target_idx': target_idx,
        'seq_length': seq_length
    }

if __name__ == "__main__":
    hdfs_path = "hdfs://localhost:9000/user/augusto-mp/raw_stock_data/META_original.json"
    results = predict_stock_prices(hdfs_path)
