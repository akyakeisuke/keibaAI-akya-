from pathlib import Path

import pandas as pd

DATA_DIR = Path("..", "data")
INPUT_DIR = DATA_DIR / "01_preprocessed"
OUTPUT_DIR = DATA_DIR / "02_features"
OUTPUT_DIR.mkdir(exist_ok=True, parents=True)


class FeatureCreator:
    def __init__(
        self,
        results_filepath: Path = INPUT_DIR / "results.csv",
        race_info_filepath: Path = INPUT_DIR / "race_info.csv",
        horse_results_filepath: Path = INPUT_DIR / "horse_results.csv",
        output_dir: Path = OUTPUT_DIR,
        output_filename: str = "features.csv",
    ):
        self.results = pd.read_csv(results_filepath, sep="\t")
        self.race_info = pd.read_csv(race_info_filepath, sep="\t")
        self.horse_results = pd.read_csv(horse_results_filepath, sep="\t")
        # 学習母集団の作成
        self.population = self.results.merge(self.race_info, on="race_id")[
            ["race_id", "horse_id", "date"]
        ]
        self.output_dir = output_dir
        self.output_filename = output_filename

    def agg_horse_n_races(self, n_races: list[int] = [1, 3, 5, 10, 1000]) -> None:
        """
        直近nレースの着順と賞金の平均を集計する関数。
        """
        grouped_df = (
            self.population.merge(
                self.horse_results, on="horse_id", suffixes=("", "_horse")
            )
            .query("date_horse < date")
            .sort_values("date_horse", ascending=False)
            .groupby(["race_id", "horse_id"])
        )
        merged_df = self.population.copy()
        for n_race in n_races:
            if n_race == 1:
                df = (
                    grouped_df.head(n_race)
                    .groupby(["race_id", "horse_id"])[["rank", "prize"]]
                    .mean()
                    .add_suffix(f"_{n_race}race")
                )
            else:
                df = (
                    grouped_df.head(n_race)
                    .groupby(["race_id", "horse_id"])[["rank", "prize"]]
                    .mean()
                    .add_suffix(f"_{n_race}races")
                )
            merged_df = merged_df.merge(df, on=["race_id", "horse_id"])
        self.agg_horse_n_races_df = merged_df

    def create_features(self) -> pd.DataFrame:
        """
        特徴量作成処理を実行し、populationテーブルに全ての特徴量を結合する。
        """
        # 馬の過去成績集計
        self.agg_horse_n_races()
        # 全ての特徴量を結合
        features = (
            self.population.merge(self.results, on=["race_id", "horse_id"])
            .merge(self.race_info, on=["race_id", "date"])
            .merge(
                self.agg_horse_n_races_df,
                on=["race_id", "date", "horse_id"],
                how="left",
            )
        )
        features.to_csv(self.output_dir / self.output_filename, sep="\t", index=False)
        return features

class PredictionFeatureCreator:
    # def __init__(
    #         self,
    #         results_filepath: Path = INPUT_DIR / "results.csv",
    # ):          
    #         self.population = pd.read_csv(results_filepath, sep="\t")
            # pass
    def init(
        self,
        population_dir: Path = POPULATION_DIR,
        population_filename: str = "population.csv",
        input_dir: Path = INPUT_DIR,
        horse_results_filename: Path = "horse_results_prediction.csv",
        jockey_leading_filename: Path = "jockey_leading.csv",
        trainer_leading_filename: Path = "trainer_leading.csv",
        peds_filename: str = "peds_prediction.csv",
        sire_leading_filename: str = "sire_leading.csv",
        output_dir: Path = OUTPUT_DIR,
        output_filename: str = "features_prediction.csv",
    ):
        self.population = pd.read_csv(population_dir / population_filename, sep="\t")
        self.horse_results = pd.read_csv(input_dir / horse_results_filename, sep="\t")
        self.jockey_leading = pd.read_csv(input_dir / jockey_leading_filename, sep="\t")
        self.trainer_leading = pd.read_csv(
            input_dir / trainer_leading_filename, sep="\t"
        )
        self.peds = pd.read_csv(input_dir / peds_filename, sep="\t")
        self.sire_leading = pd.read_csv(input_dir / sire_leading_filename, sep="\t")
        self.output_dir = output_dir
        self.output_filename = output_filename
        self.htmls = {}
        self.agg_horse_per_group_cols_dfs = {}        
    def agg_horse_n_races(self, n_races: list[int] = [1, 3, 5, 10, 1000]) -> None:
        """
        直近nレースの着順と賞金の平均を集計する関数。
        """
        grouped_df = (
            self.population.merge(
                self.horse_results, on="horse_id", suffixes=("", "_horse")
            )
            .query("date_horse < date")
            .sort_values("date_horse", ascending=False)
            .groupby(["race_id", "horse_id"])
        )
        merged_df = self.population.copy()
        for n_race in n_races:
            if n_race == 1:
                df = (
                    grouped_df.head(n_race)
                    .groupby(["race_id", "horse_id"])[["rank", "prize"]]
                    .mean()
                    .add_suffix(f"_{n_race}race")
                )
            else:
                df = (
                    grouped_df.head(n_race)
                    .groupby(["race_id", "horse_id"])[["rank", "prize"]]
                    .mean()
                    .add_suffix(f"_{n_race}races")
                )
            merged_df = merged_df.merge(df, on=["race_id", "horse_id"])
        self.agg_horse_n_races_df = merged_df

    def fetch_shutuba_table_html(self, race_id: str) -> str:   
        """
        レースidを指定すると、出馬表ページのhtmlをスクレイピングする関数。
        """
        self.html = html

    def fetch_results(self, html: str) -> pd.DataFrame:   
        """
        出馬表ページのhtmlを受け取ると、
        「レース結果テーブル」を取得して、学習時と同じ形式に前処理する関数。
        """
        df = pd.read_html(html)[0]
        # 前処理
        self.results = results

    def fetch_race_info(self, html: str) -> pd.DataFrame:   
        """
        出馬表ページのhtmlを受け取ると、
        「レース情報テーブル」を取得して、学習時と同じ形式に前処理する関数。
        """
        soup = BeautifulSoup(html, "lxml")
        # 前処理
        self.race_info = df

    def fetch_race_info(self, html: str) -> pd.DataFrame:   
        """
        出馬表ページのhtmlを受け取ると、
        「レース情報テーブル」を取得して、学習時と同じ形式に前処理する関数。
        """
        soup = BeautifulSoup(html, "lxml")
        # 前処理
        self.race_info = df 

    def create_features(self, skip_agg_horse: bool = False) -> pd.DataFrame:
        """
        特徴量作成処理を実行し、populationテーブルにすべての特徴量を結合する。
        """
        # 馬の過去成績集計
        # 先に実行しておいた場合は、スキップできる
        if not skip_agg_horse:
            self.sgg_horse_n_races()
        # 各種テーブルの取得
        self.fetch_shutuba_table_html()
        self.fetch_results()
        self.fetch_race_info()
        # 全ての特徴量を結合
        features = (
            self.population.merge(self.results, on=["race_id", "horse_id"])
            .merge(self.race_info, on=["race_id", "date"])
            .merge(
                self.agg_horse_n_races_df,
                on=["race_id", "date", "horse_id"],
                how="left",
            )
        )
        features.to_csv(self.output_dir / self.output_filename, sep="\t", index=False)
        return features       