import joblib
import numpy as np
import pandas as pd
from sklearn.metrics import mean_squared_error
from sklearn.model_selection import train_test_split
from xgboost import XGBRegressor

# Config minio

url = "http://localhost:9000"
key = "minio"
secret = "minio123"
bucket = "nyc-raw-branch-1"
file = "data"


def load_minio():
    """Load the minio data from S3 storage."""
    storage_option = {
        "endpoint_url": url,
        "key": key,
        "secret": secret,
    }
    df = pd.read_parquet(
        f"s3://{bucket}/{file}",
        storage_options=storage_option
    )
    return df


def preprocess(df):
    """Preprocess the data for training.

    Parameters
    ----------
    df : pandas.DataFrame
        The raw input dataframe.

    Returns
    -------

    """
    df = df.copy()
    df = df[
        (df['trip_distance'] > 0) &
        df['trip_distance'].notnull() &
        (df['total_amount'] > 0) &
        (df['tpep_dropoff_datetime'] -
         df['tpep_pickup_datetime']).dt.total_seconds() > 0 &
        (df['DOLocationID'] < 264) &
        (df['PULocationID'] < 264)
        ]

    df["hour"] = df['tpep_pickup_datetime'].dt.hour
    df["day"] = df['tpep_pickup_datetime'].dt.day_of_week
    df["duration"] = (df['tpep_dropoff_datetime'] - df[
        'tpep_pickup_datetime']).dt.total_seconds() / 60

    features = [
        "hour", "day", "duration", "PULocationID", "DOLocationID",
        "passenger_count", "RatecodeID", "VendorID", "trip_distance"
    ]

    df["final_amount"] = df["total_amount"] - df["tip_amount"]
    target = "final_amount"
    categorical_features = [
        "PULocationID", "DOLocationID", "RatecodeID", "VendorID", "hour", "day"
    ]

    for col in categorical_features:
        df[col] = df[col].astype('category')
    return df[features], df[target]


def train():
    """Run the training pipeline, evaluate the model, and save artifacts."""
    df = load_minio()
    X, y = preprocess(df)
    X_tmp, X_test, y_tmp, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42
    )

    X_train, X_val, y_train, y_val = train_test_split(
        X_tmp, y_tmp, test_size=0.1, random_state=42
    )

    model = XGBRegressor(
        n_jobs=-1,
        tree_method='hist',
        n_estimators=1000,
        learning_rate=0.1,
        max_depth=8,
        early_stopping_rounds=10,
        enable_categorical=True
    )

    model.fit(
        X_train,
        y_train,
        eval_set=[(X_val, y_val)],
        verbose=0
    )

    predictions = model.predict(X_test)
    rmse = np.sqrt(mean_squared_error(y_test, predictions))

    print(f"Test rmse: {rmse:.2f}")

    # On a un problème lors de l'inférence le modèle ne se souvient plus
    # des structiures des donnnées alors il faut sauvegarder dans le modèle
    artifacts = {
        "model": model,
        "dtypes": X_train.dtypes,
        "columns": X_train.columns.tolist()  # Pour garantir l'ordre
    }

    joblib.dump(artifacts, "model.pkl")
    print("Saved model to mdoels/model.pkl")


if __name__ == "__main__":
    train()
