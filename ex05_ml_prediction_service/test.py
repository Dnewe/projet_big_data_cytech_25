import joblib
import pandas as pd
from pathlib import Path

BASE_DIR = Path(__file__).parent
MODEL_PATH = BASE_DIR / "../models/nyc_taxi_model_notips.pkl"


def load_model():
    """Load the model artifacts from a pickle file."""
    model = joblib.load(MODEL_PATH)
    return model


def inferencence(X_dict, artifacts):
    """Perform model inference on a single data point.

    Parameters
    ----------
    X_dict : dict
        A dictionary containing input features.
    artifacts : dict
        A dictionary containing the trained model, dtypes, and columns.

    Returns
    -------

    """
    model = artifacts['model']

    train_dtypes = artifacts["dtypes"]
    train_columns = artifacts["columns"]

    # On va utiliser un df pour ê sur
    # d'avir le même format que pdt l'entrainement
    df = pd.DataFrame([X_dict])

    df = df[train_columns]

    for col, dtype in train_dtypes.items():
        if str(dtype) == 'category':
            df[col] = df[col].astype(dtype)

    print(f"--debug format data :\n{df.dtypes}")

    prediction = model.predict(df)
    return prediction


def check_data(X):
    """Validate the input data types and ranges.

    Parameters
    ----------
    X : dict
        The input data dictionary to validate.

    Returns
    -------
    bool
        True if all data is valid.

    """
    expected_type = {
        'hour': int,
        'day': int,
        'duration': float,
        'PULocationID': int,
        'DOLocationID': int,
        'passenger_count': int,
        'RatecodeID': int,
        'VendorID': int,
        'trip_distance': float
    }
    valid_vendors_id = [1, 2]
    valid_ratecode_id = [1, 2, 3, 4, 5, 99]

    for key, value in X.items():
        # 1. Vérification des types
        if not isinstance(value, expected_type[key]):
            raise TypeError(
                f"Erreur sur '{key}': attendu {expected_type[key]}, "
                f"reçu {type(value)}"
            )

        # 2. Comparaisons spécifiques
        if key == 'hour' and not (0 <= value <= 23):
            raise ValueError("L'heure doit être entre 0 et 23.")

        if key == 'day' and not (0 <= value <= 6):
            raise ValueError("Le jour doi être entre 1 et 6")

        if key in ['PULocationID', 'DOLocationID'] and not (1 <= value <= 265):
            raise ValueError(f"{key} doit être compris entre 1 et 265.")

        if key == 'VendorID' and value not in valid_vendors_id:
            raise ValueError(f"VendorID Doit être dans {valid_vendors_id}")

        if key == 'RatecodeID' and value not in valid_ratecode_id:
            raise ValueError(f"RatecodeID oit être dans {valid_ratecode_id}")

        if key == 'passenger_count' and not (1 <= value <= 7):
            raise ValueError("Le nombre de passagers doit être entre 0 et 7.")

        if key in ['duration', 'trip_distance'] and value < 0:
            raise ValueError(f"{key} ne peut pas être négatif.")

    return True


if __name__ == "__main__":
    X = {
        'hour': 3, 'day': 5, 'duration': 5.0,
        'PULocationID': 100, 'DOLocationID': 100, 'passenger_count': 1,
        'RatecodeID': 1, 'VendorID': 1, 'trip_distance': 1.0
    }

    try:
        check_data(X)
        artifact = load_model()
        x = inferencence(X, artifact)
        print(f"resulats : {x[0]:.2f}$")
    except Exception as e:
        print(e)
