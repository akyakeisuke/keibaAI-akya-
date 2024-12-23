import pickle
from pathlib import Path

import pandas as pd
import yaml
import lightgbm as lgb

DATA_DIR = Path("..", "data")
MODEL_DIR = DATA_DIR / "03_train"


def predict(
    features: pd.DataFrame,
    model_filepath: Path = MODEL_DIR / "model.pkl",
    config_filepath: Path = "config.yaml",
):
    with open(config_filepath, "r") as f:
        feature_cols = yaml.safe_load(f)["features"]
        selected_features = features[feature_cols]
    # with open(model_filepath, "rb") as f:
    #     model = pickle.load(f)
    model = lgb.Booster(model_file=Path("..", "model", "modelafter20240101.txt"))
    prediction_df = features[["race_id", "umaban", "tansho_odds", "popularity"]].copy()
    # prediction_df["pred"] = model.predict(features[feature_cols])

    # 1. Features が空でないか確認
    if features.empty:
        raise ValueError("Features dataframe is empty.")

    # 2. feature_cols の確認と修正
    valid_feature_cols = [col for col in feature_cols if col in features.columns]
    if not valid_feature_cols:
        raise ValueError("No valid feature columns found in features dataframe.")

    # 3. 特徴量選択と予測
    selected_features = features[valid_feature_cols].fillna(0).values  # 欠損値の埋め合わせ
    prediction_df["pred"] = model.predict(selected_features)
    
    return prediction_df.sort_values("pred", ascending=False)
