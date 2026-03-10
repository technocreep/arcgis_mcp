"""Data Cube tools — доступ к артефактам ML-модели из MinIO.

Артефакты хранятся в бакете MINIO_CUBE_BUCKET по пути:
  {project_id}/scores.csv
  {project_id}/blocks.csv
  {project_id}/features.csv
  {project_id}/eval_report.json
  {project_id}/model_meta.json
  {project_id}/interpretability/global_importance_features.csv
  {project_id}/interpretability/dominant_driver_group.csv
  {project_id}/interpretability/shap_values.csv
  {project_id}/interpretability/ale_1d.csv
"""

from __future__ import annotations

import csv
import io
import json
from typing import Callable

from ..project_store import ProjectStore


# ---------------------------------------------------------------------------
# MinIO helpers
# ---------------------------------------------------------------------------

_cube_client = None


def _get_cube_client():
    """Синглтон MinIO-клиента для куб-бакета."""
    global _cube_client
    if _cube_client is not None:
        return _cube_client
    try:
        from arcgis_mcp.config import MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY
        from minio import Minio
        _cube_client = Minio(
            MINIO_ENDPOINT,
            access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY,
            secure=False,
        )
    except Exception:
        return None
    return _cube_client


def _read_object(project_id: str, path: str) -> str | None:
    """Прочитать объект из MinIO как строку. Возвращает None при ошибке."""
    client = _get_cube_client()
    if client is None:
        return None
    try:
        from arcgis_mcp.config import MINIO_CUBE_BUCKET
        resp = client.get_object(MINIO_CUBE_BUCKET, f"{project_id}/{path}")
        return resp.read().decode("utf-8")
    except Exception:
        return None


def _parse_csv(text: str) -> list[dict]:
    """Разобрать CSV-текст в список словарей с автоматическим приведением типов."""
    if not text:
        return []
    reader = csv.DictReader(io.StringIO(text))
    rows = []
    for row in reader:
        clean = {}
        for k, v in row.items():
            v = v.strip()
            if v in ("", "nan", "NaN", "None", "null"):
                clean[k] = None
            else:
                try:
                    clean[k] = float(v) if "." in v or "e" in v.lower() else int(v)
                except ValueError:
                    clean[k] = v
        rows.append(clean)
    return rows


def _parse_json(text: str | None) -> dict:
    if not text:
        return {}
    try:
        return json.loads(text)
    except Exception:
        return {}


# ---------------------------------------------------------------------------
# Factory
# ---------------------------------------------------------------------------

def make_tools(store: ProjectStore, state: dict) -> list[Callable]:
    """Вернуть список Data Cube инструментов."""

    def _resolve_project(project_id: str | None) -> str:
        pid = project_id or state.get("current_project_id")
        if not pid:
            raise ValueError(
                "Проект не выбран. Сначала вызовите list_projects() и "
                "get_project_summary(project_id=...) чтобы установить контекст."
            )
        return pid

    # ── Tool 1 ───────────────────────────────────────────────────────────────

    def datacube_overview(project_id: str | None = None) -> str:
        """Краткий обзор результатов Data Cube ML-модели для проекта.

        Возвращает:
        - наличие ключевых артефактов в MinIO
        - метрики модели (PR-AUC на тесте и CV, x*)
        - распределение скоров проспективности по блокам
        - топ-3 наиболее важных признака

        Используй этот инструмент первым при любом вопросе о Data Cube,
        перед datacube_block_scores и datacube_block_detail.
        """
        try:
            pid = _resolve_project(project_id)
        except ValueError as e:
            return json.dumps({"error": str(e)}, ensure_ascii=False)

        warnings: list[str] = []

        # --- Eval report ---
        eval_txt = _read_object(pid, "eval_report.json")
        eval_rep = _parse_json(eval_txt)
        if not eval_rep:
            warnings.append("eval_report.json не найден в MinIO")

        # --- Model meta ---
        meta_txt = _read_object(pid, "model_meta.json")
        model_meta = _parse_json(meta_txt)
        if not model_meta:
            warnings.append("model_meta.json не найден в MinIO")

        # --- Scores ---
        scores_txt = _read_object(pid, "scores.csv")
        scores_rows = _parse_csv(scores_txt or "")
        if not scores_rows:
            warnings.append("scores.csv не найден или пуст")

        # --- Feature importance (top-3) ---
        imp_txt = _read_object(pid, "interpretability/global_importance_features.csv")
        imp_rows = _parse_csv(imp_txt or "")

        # --- Dominant driver groups ---
        drv_txt = _read_object(pid, "interpretability/dominant_driver_group.csv")
        drv_rows = _parse_csv(drv_txt or "")

        # Build response
        ce = eval_rep.get("capture_efficiency", {})
        cv = model_meta.get("cv", {})

        model_info = {
            "type": model_meta.get("model_type"),
            "feature_count": len(model_meta.get("feature_names", [])),
            "cv_mean_pr_auc": cv.get("mean_pr_auc"),
            "cv_effective_splits": cv.get("effective_splits"),
        } if model_meta else None

        eval_info = {
            "pr_auc": eval_rep.get("metrics", {}).get("pr_auc"),
            "x_star": ce.get("x_star"),
            "score_threshold_at_x_star": ce.get("score_threshold_at_x_star"),
        } if eval_rep else None

        score_vals = [float(r["score"]) for r in scores_rows if r.get("score") is not None]
        score_dist = None
        if score_vals:
            threshold = 0.5
            score_dist = {
                "n_blocks": len(score_vals),
                "min": round(min(score_vals), 4),
                "max": round(max(score_vals), 4),
                "mean": round(sum(score_vals) / len(score_vals), 4),
                "high_confidence_count": sum(1 for s in score_vals if s >= threshold),
                "high_confidence_threshold": threshold,
            }

        # Feature importance top-3
        feat_col = next((k for k in (imp_rows[0].keys() if imp_rows else [])
                         if k.lower() in ("feature", "name")), None)
        imp_col = next((k for k in (imp_rows[0].keys() if imp_rows else [])
                        if k.lower() in ("mean", "importance", "score")), None)
        top3 = []
        if imp_rows and feat_col and imp_col:
            sorted_imp = sorted(imp_rows, key=lambda r: abs(r.get(imp_col) or 0), reverse=True)
            top3 = [
                {"feature": r[feat_col], "mean_importance": r.get(imp_col)}
                for r in sorted_imp[:3]
            ]

        # Driver groups count
        driver_counts: dict = {}
        if drv_rows:
            grp_col = next((k for k in drv_rows[0] if "group" in k.lower()), None)
            if grp_col:
                for r in drv_rows:
                    g = r.get(grp_col)
                    if g:
                        driver_counts[g] = driver_counts.get(g, 0) + 1

        result: dict = {"project_id": pid}
        if warnings:
            result["warnings"] = warnings
        result["artifacts_present"] = {
            "scores": bool(scores_rows),
            "eval_report": bool(eval_rep),
            "model_meta": bool(model_meta),
            "feature_importance": bool(imp_rows),
            "dominant_driver": bool(drv_rows),
        }
        result["model"] = model_info
        result["eval"] = eval_info
        result["score_distribution"] = score_dist
        result["top3_features_by_importance"] = top3 or None
        result["dominant_driver_groups"] = driver_counts or None
        result["hint"] = (
            "Используй datacube_block_scores() чтобы увидеть топ блоков, "
            "datacube_block_detail(block_id=...) для детального анализа блока."
        )
        return json.dumps(result, ensure_ascii=False, indent=2)

    # ── Tool 2 ───────────────────────────────────────────────────────────────

    def datacube_block_scores(
        project_id: str | None = None,
        top_n: int = 20,
        min_score: float | None = None,
    ) -> str:
        """Список блоков, отсортированных по скору проспективности (убывание).

        Параметры:
          top_n      — сколько блоков вернуть (1–200, по умолчанию 20)
          min_score  — фильтровать блоки с score < min_score

        Каждая запись содержит: block_id, rank, score, lon, lat,
        dominant_driver_group. Используй для поиска наиболее перспективных
        территорий или блоков с низким скором.
        """
        try:
            pid = _resolve_project(project_id)
        except ValueError as e:
            return json.dumps({"error": str(e)}, ensure_ascii=False)

        top_n = max(1, min(200, top_n))
        warnings: list[str] = []

        scores_txt = _read_object(pid, "scores.csv")
        if not scores_txt:
            return json.dumps(
                {"error": "scores.csv не найден в MinIO для проекта " + pid},
                ensure_ascii=False,
            )
        scores_rows = _parse_csv(scores_txt)

        # Coords from blocks.csv
        blocks_txt = _read_object(pid, "blocks.csv")
        blocks_rows = _parse_csv(blocks_txt or "")
        coords: dict[str, dict] = {}
        if blocks_rows:
            for r in blocks_rows:
                bid = str(r.get("block_id", ""))
                if bid:
                    coords[bid] = {
                        "lon": r.get("lon"),
                        "lat": r.get("lat"),
                    }
        else:
            warnings.append("blocks.csv не найден, координаты недоступны")

        # Driver group
        drv_txt = _read_object(pid, "interpretability/dominant_driver_group.csv")
        drv_rows = _parse_csv(drv_txt or "")
        driver: dict[str, str | None] = {}
        if drv_rows:
            for r in drv_rows:
                bid = str(r.get("block_id", ""))
                grp_col = next((k for k in r if "group" in k.lower()), None)
                if bid and grp_col:
                    driver[bid] = r.get(grp_col)
        else:
            warnings.append("dominant_driver_group.csv не найден")

        # Sort and filter
        all_scores = [
            (str(r.get("block_id", "")), float(r["score"]))
            for r in scores_rows
            if r.get("score") is not None
        ]
        all_scores.sort(key=lambda x: x[1], reverse=True)

        if min_score is not None:
            all_scores = [(bid, s) for bid, s in all_scores if s >= min_score]

        blocks_out = []
        for rank, (bid, score) in enumerate(all_scores[:top_n], start=1):
            entry: dict = {"rank": rank, "block_id": bid, "score": round(score, 4)}
            c = coords.get(bid, {})
            entry["lon"] = c.get("lon")
            entry["lat"] = c.get("lat")
            entry["dominant_driver_group"] = driver.get(bid)
            blocks_out.append(entry)

        result: dict = {
            "project_id": pid,
            "total_blocks": len(all_scores),
            "returned": len(blocks_out),
            "filters_applied": {"min_score": min_score},
            "blocks": blocks_out,
        }
        if warnings:
            result["warnings"] = warnings
        return json.dumps(result, ensure_ascii=False, indent=2)

    # ── Tool 3 ───────────────────────────────────────────────────────────────

    def datacube_block_detail(block_id: str, project_id: str | None = None) -> str:
        """Детальный профиль одного блока: координаты, скор, признаки, SHAP-значения.

        Параметры:
          block_id — идентификатор блока (например "block_2_0");
                     используй datacube_block_scores() чтобы получить список блоков

        Возвращает:
        - location: lon, lat, x_m, y_m, row, col, cell_size_m, crs
        - score и ранг среди всех блоков
        - features: значения всех признаков
        - shap_values: вклад каждого признака (отсортирован по |значению|)
        - dominant_driver и dominant_driver_group
        """
        try:
            pid = _resolve_project(project_id)
        except ValueError as e:
            return json.dumps({"error": str(e)}, ensure_ascii=False)

        warnings: list[str] = []

        # --- Scores (for rank) ---
        scores_txt = _read_object(pid, "scores.csv")
        if not scores_txt:
            return json.dumps(
                {"error": "scores.csv не найден в MinIO для проекта " + pid},
                ensure_ascii=False,
            )
        scores_rows = _parse_csv(scores_txt)
        all_sorted = sorted(
            [(str(r.get("block_id", "")), float(r["score"]))
             for r in scores_rows if r.get("score") is not None],
            key=lambda x: x[1], reverse=True,
        )
        score_map = {bid: s for bid, s in all_sorted}
        rank_map = {bid: i + 1 for i, (bid, _) in enumerate(all_sorted)}

        if block_id not in score_map:
            available = [bid for bid, _ in all_sorted[:10]]
            return json.dumps({
                "error": f"Блок '{block_id}' не найден в scores.csv",
                "hint": "Используй datacube_block_scores() чтобы увидеть список блоков",
                "first_10_block_ids": available,
            }, ensure_ascii=False)

        # --- Location ---
        blocks_txt = _read_object(pid, "blocks.csv")
        blocks_rows = _parse_csv(blocks_txt or "")
        location = None
        if blocks_rows:
            brow = next((r for r in blocks_rows if str(r.get("block_id")) == block_id), None)
            if brow:
                location = {
                    "lon": brow.get("lon"),
                    "lat": brow.get("lat"),
                    "x_m": brow.get("x_m"),
                    "y_m": brow.get("y_m"),
                    "row": brow.get("row"),
                    "col": brow.get("col"),
                    "cell_size_m": brow.get("cell_size_m"),
                    "crs": brow.get("metric_crs"),
                }
        else:
            warnings.append("blocks.csv не найден")

        # --- Features ---
        feat_txt = _read_object(pid, "features.csv")
        feat_rows = _parse_csv(feat_txt or "")
        features = None
        if feat_rows:
            frow = next((r for r in feat_rows if str(r.get("block_id")) == block_id), None)
            if frow:
                features = {k: v for k, v in frow.items() if k != "block_id"}
        else:
            warnings.append("features.csv не найден")

        # --- SHAP values (wide format) ---
        shap_txt = _read_object(pid, "interpretability/shap_values.csv")
        shap_rows = _parse_csv(shap_txt or "")
        shap_values = None
        if shap_rows:
            srow = next((r for r in shap_rows if str(r.get("block_id")) == block_id), None)
            if srow:
                raw = {k: v for k, v in srow.items() if k != "block_id"}
                # Sort by |shap| descending
                shap_values = dict(
                    sorted(raw.items(), key=lambda kv: abs(kv[1] or 0), reverse=True)
                )
        else:
            warnings.append("shap_values.csv не найден")

        # --- Dominant driver ---
        drv_txt = _read_object(pid, "interpretability/dominant_driver_group.csv")
        drv_rows = _parse_csv(drv_txt or "")
        dominant_driver = None
        dominant_driver_group = None
        if drv_rows:
            drow = next((r for r in drv_rows if str(r.get("block_id")) == block_id), None)
            if drow:
                drv_col = next((k for k in drow if "driver" in k.lower() and "group" not in k.lower()), None)
                grp_col = next((k for k in drow if "group" in k.lower()), None)
                dominant_driver = drow.get(drv_col) if drv_col else None
                dominant_driver_group = drow.get(grp_col) if grp_col else None
        else:
            warnings.append("dominant_driver_group.csv не найден")

        result: dict = {
            "project_id": pid,
            "block_id": block_id,
            "score": round(score_map[block_id], 4),
            "rank_in_dataset": rank_map[block_id],
            "total_blocks": len(all_sorted),
            "location": location,
            "features": features,
            "shap_values": shap_values,
            "dominant_driver": dominant_driver,
            "dominant_driver_group": dominant_driver_group,
        }
        if warnings:
            result["warnings"] = warnings
        return json.dumps(result, ensure_ascii=False, indent=2)

    return [datacube_overview, datacube_block_scores, datacube_block_detail]
