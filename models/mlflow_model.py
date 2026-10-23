"""The frozen detector as an MLflow pyfunc model, so the registry holds a loadable,
servable model (mlflow.pyfunc.load_model("models:/gmpdi-fault-detector/1")).

predict() takes readings (batch_id, seq, time_h, step, signals) and returns, per
reading, the monitoring statistics and the alarm flag.
"""

from __future__ import annotations

import mlflow.pyfunc
import pandas as pd


class DetectorModel(mlflow.pyfunc.PythonModel):
    def load_context(self, context):
        from models.detector import Detector

        self.detector = Detector.load(context.artifacts["detector"])

    def predict(self, context, model_input: pd.DataFrame, params=None) -> pd.DataFrame:
        frame = model_input.sort_values(["batch_id", "seq"]).reset_index(drop=True)
        if "step" not in frame:
            frame["step"] = (frame["time_h"] / 0.2).round().astype(int)
        stats = self.detector.statistics(frame)
        out = frame[["batch_id", "seq", "time_h"]].copy()
        for k, v in stats.items():
            out[k] = v
        out["alarm"] = self.detector.alarm(frame, stats)
        return out
