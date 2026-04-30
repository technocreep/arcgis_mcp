"""Data Cube tools — доступ к артефактам ML-модели.

Режимы работы:
  * Мультисценарный (report mode) — артефакты на локальном диске:
      PROJECTS_DIR/{pid}/datacube/report_dataset/
    Активируется автоматически при наличии report_manifest.json.
  * Одиночный запуск (legacy mode) — артефакты в MinIO:
      {project_id}/scores.csv, blocks.csv, ...

Пути в report mode:
  report_dataset/scenarios/{scenario_id}/output/
    blocks.csv, features.csv, scores.csv, eval_report.json,
    model_meta.json, interpretability/...
  report_dataset/report_visualizations/scenarios/{scenario_id}/labels/{label_profile_id}/
    models/{model_profile_id}/
      mask_dynamics_blocks_q90.csv  (и q95, q99)
      contour_blocks_q90.csv        (и q95, q99)
"""

from __future__ import annotations

import csv
import io
import json
import time
from pathlib import Path
from typing import Callable

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np

from ..project_store import ProjectStore


# ---------------------------------------------------------------------------
# MinIO helpers (legacy single-job mode)
# ---------------------------------------------------------------------------

_cube_client = None


def _get_cube_client():
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
    """Прочитать объект из MinIO. Возвращает None при ошибке."""
    client = _get_cube_client()
    if client is None:
        return None
    try:
        from arcgis_mcp.config import MINIO_CUBE_BUCKET
        resp = client.get_object(MINIO_CUBE_BUCKET, f"{project_id}/{path}")
        return resp.read().decode("utf-8")
    except Exception:
        return None


# ---------------------------------------------------------------------------
# Local disk helpers (report mode)
# ---------------------------------------------------------------------------

def _dc_dir(pid: str) -> Path:
    """Путь к директории datacube для проекта."""
    from arcgis_mcp.config import PROJECTS_DIR
    return Path(PROJECTS_DIR) / pid / "datacube"


def _read_local(pid: str, rel_path: str) -> str | None:
    """Прочитать файл из PROJECTS_DIR/{pid}/datacube/{rel_path}."""
    target = _dc_dir(pid) / rel_path
    try:
        return target.read_text(encoding="utf-8")
    except Exception:
        return None


def _detect_report_mode(pid: str) -> bool:
    """Вернуть True если для проекта есть мультисценарный отчёт."""
    return (_dc_dir(pid) / "report_dataset" / "report_manifest.json").exists()


def _best_scenario(pid: str, scenario_id: str | None) -> str:
    """Выбрать scenario_id: явный > лучший по PR-AUC > 'balanced_reference'."""
    if scenario_id:
        return scenario_id
    # Parse scenario_index.csv for best PR-AUC
    txt = _read_local(pid, "report_dataset/scenario_index.csv")
    if txt:
        rows = _parse_csv(txt)
        valid = [r for r in rows if r.get("pr_auc") is not None]
        if valid:
            best = max(valid, key=lambda r: float(r["pr_auc"] or 0))
            return str(best.get("scenario_id", "balanced_reference"))
    return "balanced_reference"


def _scenario_prefix(pid: str, scenario_id: str | None) -> str:
    """Вернуть относительный путь к output директории сценария."""
    sid = _best_scenario(pid, scenario_id)
    return f"report_dataset/scenarios/{sid}/output"


# ---------------------------------------------------------------------------
# CSV / JSON helpers
# ---------------------------------------------------------------------------

def _parse_csv(text: str) -> list[dict]:
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


def _read_artifact(pid: str, rel_path: str, report_mode: bool) -> str | None:
    """Единая точка чтения: локальный диск в report mode, MinIO иначе."""
    if report_mode:
        return _read_local(pid, rel_path)
    return _read_object(pid, rel_path)


# ---------------------------------------------------------------------------
# Factory
# ---------------------------------------------------------------------------

def make_tools(store: ProjectStore, state: dict) -> list[Callable]:

    def _resolve_project(project_id: str | None) -> str:
        pid = project_id or state.get("current_project_id")
        if not pid:
            raise ValueError(
                "Проект не выбран. Сначала вызовите list_projects() и "
                "get_project_summary(project_id=...) чтобы установить контекст."
            )
        return pid

    # ── Tool 1 ───────────────────────────────────────────────────────────────

    def datacube_overview(
        project_id: str | None = None,
        scenario_id: str | None = None,
    ) -> str:
        """Краткий обзор результатов Data Cube ML-модели для проекта.

        Возвращает:
        - метрики модели (PR-AUC на тесте и CV, x*)
        - распределение скоров проспективности по блокам
        - топ-3 наиболее важных признака

        В мультисценарном режиме (report mode) указывай scenario_id.
        Если scenario_id не указан — выбирается сценарий с лучшим PR-AUC.
        Используй datacube_report_overview() чтобы увидеть доступные сценарии.
        Используй этот инструмент первым при любом вопросе о Data Cube.
        """
        try:
            pid = _resolve_project(project_id)
        except ValueError as e:
            return json.dumps({"error": str(e)}, ensure_ascii=False)

        report_mode = _detect_report_mode(pid)
        warnings: list[str] = []

        if report_mode:
            prefix = _scenario_prefix(pid, scenario_id)
            active_scenario = _best_scenario(pid, scenario_id)
        else:
            prefix = ""
            active_scenario = None

        def _read(rel: str) -> str | None:
            path = f"{prefix}/{rel}".lstrip("/") if prefix else rel
            return _read_artifact(pid, path, report_mode)

        eval_rep = _parse_json(_read("eval_report.json"))
        if not eval_rep:
            warnings.append("eval_report.json не найден")

        meta_txt = _read("model_meta.json")
        model_meta = _parse_json(meta_txt)
        if not model_meta:
            warnings.append("model_meta.json не найден")

        scores_txt = _read("scores.csv")
        scores_rows = _parse_csv(scores_txt or "")
        if not scores_rows:
            warnings.append("scores.csv не найден или пуст")

        imp_rows = _parse_csv(_read("interpretability/global_importance_features.csv") or "")
        drv_rows = _parse_csv(_read("interpretability/dominant_driver_group.csv") or "")

        # Label profiles: what mineral types were modeled
        label_summary_rows = _parse_csv(
            _read_local(pid, f"{prefix}/labels/label_profile_summary.csv") or ""
            if report_mode else ""
        )
        modeled_profiles = [
            str(r.get("label_profile_id") or r.get("profile_id", ""))
            for r in label_summary_rows
            if r.get("label_profile_id") or r.get("profile_id")
        ] if label_summary_rows else []

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
            score_dist = {
                "n_blocks": len(score_vals),
                "min": round(min(score_vals), 4),
                "max": round(max(score_vals), 4),
                "mean": round(sum(score_vals) / len(score_vals), 4),
                "high_confidence_count": sum(1 for s in score_vals if s >= 0.5),
                "high_confidence_threshold": 0.5,
            }

        feat_col = next((k for k in (imp_rows[0].keys() if imp_rows else [])
                         if k.lower() in ("feature", "name")), None)
        imp_col = next((k for k in (imp_rows[0].keys() if imp_rows else [])
                        if k.lower() in ("mean", "importance", "score")), None)
        top3 = []
        if imp_rows and feat_col and imp_col:
            sorted_imp = sorted(imp_rows, key=lambda r: abs(r.get(imp_col) or 0), reverse=True)
            top3 = [{"feature": r[feat_col], "mean_importance": r.get(imp_col)}
                    for r in sorted_imp[:3]]

        driver_counts: dict = {}
        if drv_rows:
            grp_col = next((k for k in drv_rows[0] if "group" in k.lower()), None)
            if grp_col:
                for r in drv_rows:
                    g = r.get(grp_col)
                    if g:
                        driver_counts[g] = driver_counts.get(g, 0) + 1

        result: dict = {"project_id": pid}
        if report_mode:
            result["mode"] = "report"
            result["active_scenario"] = active_scenario
            result["hint_scenarios"] = "Используй datacube_report_overview() для списка сценариев"
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
        result["modeled_label_profiles"] = modeled_profiles or None
        result["score_distribution"] = score_dist
        result["top3_features_by_importance"] = top3 or None
        result["dominant_driver_groups"] = driver_counts or None
        result["hint"] = (
            "Используй datacube_block_scores() чтобы увидеть топ блоков, "
            "datacube_block_detail(block_id=...) для детального анализа блока, "
            "datacube_score_overlay() для карты проспективности."
        )
        return json.dumps(result, ensure_ascii=False, indent=2)

    # ── Tool 2 ───────────────────────────────────────────────────────────────

    def datacube_block_scores(
        project_id: str | None = None,
        top_n: int = 20,
        min_score: float | None = None,
        scenario_id: str | None = None,
    ) -> str:
        """Список блоков, отсортированных по скору проспективности (убывание).

        Параметры:
          top_n       — сколько блоков вернуть (1–200, по умолчанию 20)
          min_score   — фильтровать блоки с score < min_score
          scenario_id — ID сценария в report mode (если не указан — лучший по PR-AUC)

        Каждая запись содержит: block_id, rank, score, lon, lat, dominant_driver_group.
        """
        try:
            pid = _resolve_project(project_id)
        except ValueError as e:
            return json.dumps({"error": str(e)}, ensure_ascii=False)

        top_n = max(1, min(200, top_n))
        report_mode = _detect_report_mode(pid)
        warnings: list[str] = []

        if report_mode:
            prefix = _scenario_prefix(pid, scenario_id)
            active_scenario = _best_scenario(pid, scenario_id)
        else:
            prefix = ""
            active_scenario = None

        def _read(rel: str) -> str | None:
            path = f"{prefix}/{rel}".lstrip("/") if prefix else rel
            return _read_artifact(pid, path, report_mode)

        scores_txt = _read("scores.csv")
        if not scores_txt:
            return json.dumps(
                {"error": "scores.csv не найден для проекта " + pid}, ensure_ascii=False
            )
        scores_rows = _parse_csv(scores_txt)

        blocks_txt = _read("blocks.csv")
        blocks_rows = _parse_csv(blocks_txt or "")
        coords: dict[str, dict] = {}
        if blocks_rows:
            for r in blocks_rows:
                bid = str(r.get("block_id", ""))
                if bid:
                    coords[bid] = {"lon": r.get("lon"), "lat": r.get("lat")}
        else:
            warnings.append("blocks.csv не найден, координаты недоступны")

        drv_txt = _read("interpretability/dominant_driver_group.csv")
        drv_rows = _parse_csv(drv_txt or "")
        driver: dict[str, str | None] = {}
        if drv_rows:
            for r in drv_rows:
                bid = str(r.get("block_id", ""))
                grp_col = next((k for k in r if "group" in k.lower()), None)
                if bid and grp_col:
                    driver[bid] = r.get(grp_col)

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

        result: dict = {"project_id": pid}
        if report_mode:
            result["active_scenario"] = active_scenario
        result.update({
            "total_blocks": len(all_scores),
            "returned": len(blocks_out),
            "filters_applied": {"min_score": min_score},
            "blocks": blocks_out,
        })
        if warnings:
            result["warnings"] = warnings
        return json.dumps(result, ensure_ascii=False, indent=2)

    # ── Tool 3 ───────────────────────────────────────────────────────────────

    def datacube_block_detail(
        block_id: str,
        project_id: str | None = None,
        scenario_id: str | None = None,
    ) -> str:
        """Детальный профиль одного блока: координаты, скор, признаки, SHAP-значения.

        Параметры:
          block_id    — идентификатор блока (например "block_2_0")
          scenario_id — ID сценария в report mode

        Возвращает location, score, ранг, features, shap_values, dominant_driver.
        Используй datacube_block_scores() чтобы получить список блоков.
        """
        try:
            pid = _resolve_project(project_id)
        except ValueError as e:
            return json.dumps({"error": str(e)}, ensure_ascii=False)

        report_mode = _detect_report_mode(pid)
        warnings: list[str] = []

        if report_mode:
            prefix = _scenario_prefix(pid, scenario_id)
            active_scenario = _best_scenario(pid, scenario_id)
        else:
            prefix = ""
            active_scenario = None

        def _read(rel: str) -> str | None:
            path = f"{prefix}/{rel}".lstrip("/") if prefix else rel
            return _read_artifact(pid, path, report_mode)

        scores_txt = _read("scores.csv")
        if not scores_txt:
            return json.dumps(
                {"error": "scores.csv не найден для проекта " + pid}, ensure_ascii=False
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
            return json.dumps({
                "error": f"Блок '{block_id}' не найден в scores.csv",
                "hint": "Используй datacube_block_scores() чтобы увидеть список блоков",
                "first_10_block_ids": [bid for bid, _ in all_sorted[:10]],
            }, ensure_ascii=False)

        blocks_rows = _parse_csv(_read("blocks.csv") or "")
        location = None
        if blocks_rows:
            brow = next((r for r in blocks_rows if str(r.get("block_id")) == block_id), None)
            if brow:
                location = {k: brow.get(k) for k in ("lon", "lat", "x_m", "y_m", "row", "col", "cell_size_m")}
                location["crs"] = brow.get("metric_crs")
        else:
            warnings.append("blocks.csv не найден")

        feat_rows = _parse_csv(_read("features.csv") or "")
        features = None
        if feat_rows:
            frow = next((r for r in feat_rows if str(r.get("block_id")) == block_id), None)
            if frow:
                features = {k: v for k, v in frow.items() if k != "block_id"}
        else:
            warnings.append("features.csv не найден")

        shap_rows = _parse_csv(_read("interpretability/shap_values.csv") or "")
        shap_values = None
        if shap_rows:
            srow = next((r for r in shap_rows if str(r.get("block_id")) == block_id), None)
            if srow:
                raw = {k: v for k, v in srow.items() if k != "block_id"}
                shap_values = dict(sorted(raw.items(), key=lambda kv: abs(kv[1] or 0), reverse=True))
        else:
            warnings.append("shap_values.csv не найден")

        drv_rows = _parse_csv(_read("interpretability/dominant_driver_group.csv") or "")
        dominant_driver = dominant_driver_group = None
        if drv_rows:
            drow = next((r for r in drv_rows if str(r.get("block_id")) == block_id), None)
            if drow:
                drv_col = next((k for k in drow if "driver" in k.lower() and "group" not in k.lower()), None)
                grp_col = next((k for k in drow if "group" in k.lower()), None)
                dominant_driver = drow.get(drv_col) if drv_col else None
                dominant_driver_group = drow.get(grp_col) if grp_col else None
        else:
            warnings.append("dominant_driver_group.csv не найден")

        result: dict = {"project_id": pid}
        if report_mode:
            result["active_scenario"] = active_scenario
        result.update({
            "block_id": block_id,
            "score": round(score_map[block_id], 4),
            "rank_in_dataset": rank_map[block_id],
            "total_blocks": len(all_sorted),
            "location": location,
            "features": features,
            "shap_values": shap_values,
            "dominant_driver": dominant_driver,
            "dominant_driver_group": dominant_driver_group,
        })
        if warnings:
            result["warnings"] = warnings
        return json.dumps(result, ensure_ascii=False, indent=2)

    # ── Tool 4 ───────────────────────────────────────────────────────────────

    def datacube_report_overview(project_id: str | None = None) -> str:
        """Обзор мультисценарного отчёта Data Cube.

        Возвращает:
        - список сценариев (scenario_id, title, step_m, pos_radius_m, pr_auc, статус)
        - лучший сценарий по PR-AUC
        - доступные label_profile_id и model_profile_id
        - число артефактов визуализации

        Используй этот инструмент перед datacube_score_overlay() чтобы узнать
        какие scenario_id, label_profile_id, model_profile_id доступны.
        """
        try:
            pid = _resolve_project(project_id)
        except ValueError as e:
            return json.dumps({"error": str(e)}, ensure_ascii=False)

        if not _detect_report_mode(pid):
            return json.dumps({
                "error": "report_dataset не найден для проекта",
                "hint": "Запустите мультисценарный пайплайн через UI (кнопка 'Run Report')."
            }, ensure_ascii=False)

        manifest = _parse_json(_read_local(pid, "report_dataset/report_manifest.json"))
        scenario_txt = _read_local(pid, "report_dataset/scenario_index.csv")
        scenario_rows = _parse_csv(scenario_txt or "")

        viz_index_txt = _read_local(
            pid, "report_dataset/report_visualizations/visualization_index.csv"
        )
        viz_rows = _parse_csv(viz_index_txt or "")

        # Summarise scenarios
        scenarios_out = []
        best_scenario = None
        best_pr_auc = -1.0
        for row in scenario_rows:
            sid = str(row.get("scenario_id", ""))
            pr_auc = row.get("pr_auc")
            entry = {
                "scenario_id": sid,
                "title": row.get("title"),
                "step_m": row.get("step_m"),
                "pos_radius_m": row.get("pos_radius_m"),
                "status": row.get("status"),
                "pr_auc": pr_auc,
                "x_star": row.get("x_star"),
                "block_count": row.get("block_count"),
            }
            scenarios_out.append(entry)
            if pr_auc is not None and float(pr_auc or 0) > best_pr_auc:
                best_pr_auc = float(pr_auc)
                best_scenario = sid

        # Collect unique label / model profile IDs from viz_index
        label_profiles = sorted({str(r.get("label_profile_id", "")) for r in viz_rows if r.get("label_profile_id")})
        model_profiles = sorted({str(r.get("model_profile_id", "")) for r in viz_rows if r.get("model_profile_id")})
        viz_exists_count = sum(1 for r in viz_rows if r.get("status") == "exists")

        result = {
            "project_id": pid,
            "report_mode": True,
            "contract_version": manifest.get("contract_version"),
            "scenarios": scenarios_out,
            "best_scenario_by_pr_auc": best_scenario,
            "available_label_profiles": label_profiles,
            "available_model_profiles": model_profiles,
            "visualization_artifacts_ready": viz_exists_count,
            "hint": (
                "Используй datacube_score_overlay(scenario_id=..., label_profile_id=..., "
                "model_profile_id=..., quantile='q90') для карты проспективности. "
                "Используй datacube_overview(scenario_id=...) для метрик конкретного сценария."
            ),
        }
        return json.dumps(result, ensure_ascii=False, indent=2)

    # ── Tool 5 ───────────────────────────────────────────────────────────────

    def datacube_score_overlay(
        project_id: str | None = None,
        scenario_id: str | None = None,
        label_profile_id: str | None = None,
        model_profile_id: str | None = None,
        quantile: str = "q90",
        visualization_type: str = "mask",
        layer_names: str | None = None,
    ) -> str:
        """Карта проспективности Data Cube с наложением картографических слоёв.

        Строит изображение из CSV блоков (вычисленных report_visualization_runner.py),
        окрашивает блоки по score и накладывает выбранные ГИС-слои из проекта.

        Параметры:
          scenario_id       — ID сценария (regional_fast, balanced_reference, detailed_skeptical).
                              Если None — лучший по PR-AUC.
          label_profile_id  — ID профиля руды (например "any_occurrence").
                              Если None — первый доступный.
          model_profile_id  — datacube_only | rs_only | combined.
                              Если None — combined.
          quantile          — q90 | q95 | q99. Уровень отсечения проспективности.
          visualization_type — mask (mask_dynamics) | contour (contour_narrowing).
          layer_names       — JSON-массив ID слоёв ГИС для наложения.
                              Пример: '["DrudP_R_42","fault_layer"]'.
                              Если None — только блоки + контур лицензии.

        Перед вызовом используй datacube_report_overview() чтобы узнать доступные
        scenario_id, label_profile_id, model_profile_id.
        """
        try:
            pid = _resolve_project(project_id)
        except ValueError as e:
            return json.dumps({"error": str(e)}, ensure_ascii=False)

        if not _detect_report_mode(pid):
            return json.dumps({
                "error": "report_dataset не найден. Запустите мультисценарный пайплайн.",
            }, ensure_ascii=False)

        # ── Resolve parameters ────────────────────────────────────────────────
        sid = _best_scenario(pid, scenario_id)
        mid = model_profile_id or "combined"
        valid_quantiles = ("q90", "q95", "q99")
        q = quantile if quantile in valid_quantiles else "q90"

        # Auto-discover label_profile_id from visualization_index
        if label_profile_id is None:
            viz_idx = _parse_csv(
                _read_local(pid, "report_dataset/report_visualizations/visualization_index.csv") or ""
            )
            matching = [
                r for r in viz_idx
                if str(r.get("scenario_id")) == sid
                and str(r.get("model_profile_id", "")) == mid
                and r.get("status") == "exists"
            ]
            if matching:
                label_profile_id = str(matching[0].get("label_profile_id", ""))

        lid = label_profile_id or "any_occurrence"

        # ── Read visualization CSV ─────────────────────────────────────────────
        viz_type = "mask_dynamics" if visualization_type == "mask" else "contour"
        csv_rel = (
            f"report_dataset/report_visualizations/scenarios/{sid}"
            f"/labels/{lid}/models/{mid}/{viz_type}_blocks_{q}.csv"
        )
        csv_txt = _read_local(pid, csv_rel)
        used_fallback = False
        if not csv_txt:
            # Visualization CSV не сгенерирован — строим из сырых артефактов сценария
            scores_rel = f"report_dataset/scenarios/{sid}/output/models/{mid}/scores.csv"
            blocks_rel = f"report_dataset/scenarios/{sid}/output/blocks.csv"
            scores_txt = _read_local(pid, scores_rel)
            blocks_txt = _read_local(pid, blocks_rel)
            if not scores_txt or not blocks_txt:
                return json.dumps({
                    "error": f"Visualization CSV не найден: {csv_rel}",
                    "hint": (
                        f"Сырые артефакты также не найдены "
                        f"(scores: {bool(scores_txt)}, blocks: {bool(blocks_txt)}). "
                        "Используй datacube_report_overview() для списка доступных артефактов."
                    ),
                }, ensure_ascii=False)
            scores_rows = {str(r["block_id"]): float(r["score"])
                           for r in _parse_csv(scores_txt)
                           if r.get("block_id") is not None and r.get("score") is not None}
            all_scores = list(scores_rows.values())
            quantile_val = {"q90": 0.90, "q95": 0.95, "q99": 0.99}.get(q, 0.90)
            threshold = float(np.nanquantile(all_scores, quantile_val)) if all_scores else 0.0
            blocks_raw = _parse_csv(blocks_txt)
            blocks = [
                {**r, "score": scores_rows[str(r["block_id"])]}
                for r in blocks_raw
                if str(r.get("block_id")) in scores_rows
                and scores_rows[str(r["block_id"])] >= threshold
            ]
            used_fallback = True
        else:
            blocks = _parse_csv(csv_txt)

        if not blocks:
            return json.dumps({"error": "CSV файл пуст"}, ensure_ascii=False)

        lons = [float(r["lon"]) for r in blocks if r.get("lon") is not None]
        lats = [float(r["lat"]) for r in blocks if r.get("lat") is not None]
        scores = [float(r["score"]) for r in blocks if r.get("score") is not None]

        if not lons:
            return json.dumps({"error": "Нет координат в CSV файле"}, ensure_ascii=False)

        # ── Build figure ───────────────────────────────────────────────────────
        try:
            from .viz_utils import (
                get_license_boundary,
                get_license_view_bounds,
                draw_license_boundary,
                load_and_reproject,
                prepare_for_plot,
                clip_to_view,
                save_figure,
                upload_to_minio,
                find_elevation_field,
                label_isolines,
                auto_colormap,
            )
        except ImportError as e:
            return json.dumps({"error": f"viz_utils import failed: {e}"}, ensure_ascii=False)

        lic_gdf = get_license_boundary(pid, store)
        bounds = get_license_view_bounds(lic_gdf, margin=0.20)

        # Fallback extent from data
        if bounds is None:
            pad_lon = (max(lons) - min(lons)) * 0.1 or 0.05
            pad_lat = (max(lats) - min(lats)) * 0.1 or 0.05
            bounds = (min(lons) - pad_lon, min(lats) - pad_lat,
                      max(lons) + pad_lon, max(lats) + pad_lat)

        minx, miny, maxx, maxy = bounds
        aspect = (maxx - minx) / max(maxy - miny, 1e-9)
        fig_w = min(14, max(8, aspect * 8))
        fig_h = fig_w / aspect
        fig, ax = plt.subplots(figsize=(fig_w, fig_h), dpi=150)

        # Score layer — semi-transparent rectangles sized by cell_size_m
        import math
        from matplotlib.patches import Rectangle
        from matplotlib.collections import PatchCollection

        patches = []
        patch_scores = []
        default_cell = float(blocks[0].get("cell_size_m") or 500) if blocks else 500.0
        for b in blocks:
            lo = b.get("lon")
            la = b.get("lat")
            sc_val = b.get("score")
            if lo is None or la is None or sc_val is None:
                continue
            lo, la, sc_val = float(lo), float(la), float(sc_val)
            cell_m = float(b.get("cell_size_m") or default_cell)
            half_lat = (cell_m / 111_000) / 2
            half_lon = (cell_m / (111_000 * math.cos(math.radians(la)))) / 2
            patches.append(Rectangle((lo - half_lon, la - half_lat), 2 * half_lon, 2 * half_lat))
            patch_scores.append(sc_val)

        patch_scores_arr = np.array(patch_scores)
        col = PatchCollection(patches, cmap="viridis", alpha=0.55, linewidths=0)
        col.set_array(patch_scores_arr)
        ax.add_collection(col)
        plt.colorbar(col, ax=ax, label="Prospectivity Score", shrink=0.7)

        # Optional GIS layers
        layers_rendered: list[str] = []
        warnings_out: list[str] = []
        if layer_names:
            try:
                requested = json.loads(layer_names)
            except Exception:
                requested = []

            try:
                gdb_path = store.get_gdb_path(pid)
                manifest = store.get_manifest(pid)
                layer_index = {lyr["layer_id"]: lyr for lyr in manifest.get("layers", [])}

                colors = ["#e63946", "#2196F3", "#4CAF50", "#FF9800", "#9C27B0",
                          "#00BCD4", "#795548", "#607D8B"]
                for i, lid_req in enumerate(requested):
                    lyr_meta = layer_index.get(lid_req)
                    if lyr_meta is None:
                        warnings_out.append(f"Слой не найден: {lid_req}")
                        continue
                    try:
                        gdf = load_and_reproject(gdb_path, lid_req)
                        gdf, _ = prepare_for_plot(gdf)
                        gdf = clip_to_view(gdf, bounds)
                        if gdf.empty:
                            continue
                        color = colors[i % len(colors)]
                        geom_type = gdf.geometry.geom_type.mode().iloc[0].lower() if len(gdf) else "point"
                        label = lyr_meta.get("display_name", lid_req)
                        if "point" in geom_type:
                            # Auto-detect density rendering for geophysical point fields
                            import pandas as _pd
                            from .viz_utils import clip_quantiles as _clip_q
                            _skip = {"objectid", "fid", "shape_area", "shape_length",
                                     "x_m", "y_m", "lon", "lat", "row", "col", "block_id"}
                            _num_fields = [
                                c for c in gdf.columns
                                if c.lower() not in _skip
                                and not c.lower().startswith(("shape", "fid"))
                                and _pd.api.types.is_numeric_dtype(gdf[c])
                                and gdf[c].notna().any()
                            ]
                            # Prefer anomaly/delta fields; then fields with both +/- values
                            _anomaly_kw = ("delta", "дельта", "anomal", "аномал")
                            val_field = (
                                next((c for c in _num_fields
                                      if any(kw in c.lower() for kw in _anomaly_kw)), None)
                                or next((c for c in _num_fields
                                         if gdf[c].min() < 0 < gdf[c].max()), None)
                                or (_num_fields[0] if _num_fields else None)
                            )
                            if val_field and len(gdf) >= 200:
                                from scipy.interpolate import griddata as _griddata
                                col_vals = _pd.to_numeric(gdf[val_field], errors="coerce")
                                _x = gdf.geometry.x.values
                                _y = gdf.geometry.y.values
                                _mask = ~np.isnan(col_vals.values)
                                if _mask.sum() >= 10:
                                    _xm, _ym = _x[_mask], _y[_mask]
                                    _zm = col_vals.values[_mask]
                                    _ig = 200
                                    gx, gy = np.mgrid[
                                        _xm.min():_xm.max():complex(_ig),
                                        _ym.min():_ym.max():complex(_ig),
                                    ]
                                    gz = _griddata((_xm, _ym), _zm, (gx, gy), method="linear")
                                    dn = lyr_meta.get("display_name", lid_req)
                                    cmap_d = auto_colormap(val_field, lyr_meta.get("units"), dn)
                                    # Normalize with clip_quantiles; symmetrize diverging cmaps
                                    vmin_q, vmax_q = _clip_q(col_vals.dropna())
                                    _div_cmaps = ("RdBu", "RdYlBu", "bwr", "seismic",
                                                  "coolwarm", "PiYG", "PRGn")
                                    if any(d in cmap_d for d in _div_cmaps):
                                        _abs = max(abs(vmin_q), abs(vmax_q))
                                        vmin_q, vmax_q = -_abs, _abs
                                    cf = ax.contourf(gx, gy, gz, levels=20, cmap=cmap_d,
                                                     vmin=vmin_q, vmax=vmax_q,
                                                     alpha=0.65, zorder=3)
                                    ax.contour(gx, gy, gz, levels=10, colors="black",
                                               linewidths=0.3, alpha=0.35, zorder=3)
                                    plt.colorbar(cf, ax=ax, label=dn, shrink=0.6)
                                    layers_rendered.append(lid_req)
                                    continue
                            gdf.plot(ax=ax, color=color, markersize=3, alpha=0.7,
                                     label=label, zorder=5)
                        elif "line" in geom_type:
                            # Relief contours: render like plot_relief (gray + elevation labels)
                            elev_col = find_elevation_field(gdf)
                            if elev_col:
                                gdf.plot(ax=ax, color="#888888", linewidth=0.5,
                                         alpha=0.5, zorder=4, label=label)
                                label_isolines(ax, gdf, elev_col, bounds, target=50)
                            else:
                                gdf.plot(ax=ax, color=color, linewidth=0.8, alpha=0.8,
                                         label=label, zorder=5)
                        else:
                            gdf.plot(ax=ax, facecolor="none", edgecolor=color,
                                     linewidth=0.8, alpha=0.8, label=label, zorder=5)
                        layers_rendered.append(lid_req)
                    except Exception as exc:
                        warnings_out.append(f"Ошибка рендеринга слоя {lid_req}: {exc}")
            except Exception as exc:
                warnings_out.append(f"Ошибка загрузки ГИС данных: {exc}")

        draw_license_boundary(ax, lic_gdf, zorder=10)

        ax.set_xlim(minx, maxx)
        ax.set_ylim(miny, maxy)
        ax.set_xlabel("Longitude")
        ax.set_ylabel("Latitude")

        title_parts = [f"Prospectivity — {sid}", f"{lid} / {mid} / {q}"]
        ax.set_title("\n".join(title_parts), fontsize=10)

        if layers_rendered or lic_gdf is not None:
            ax.legend(loc="upper right", fontsize=7, framealpha=0.8)

        fig.tight_layout()

        ts = int(time.time())
        local_path = save_figure(fig, pid, f"score_overlay_{sid}_{mid}_{q}_{ts}")
        plt.close(fig)

        url = upload_to_minio(local_path, pid)

        result: dict = {"project_id": pid}
        if url:
            result["markdown"] = f"![Score overlay — {sid} / {mid} / {q}]({url})"
            result["url"] = url
            result["hint_render"] = "Вставь значение поля markdown дословно в ответ — это готовая Markdown-ссылка на изображение."
        else:
            result["local_path"] = local_path
            result["warning"] = "MinIO недоступен, изображение сохранено локально"
        result.update({
            "scenario_id": sid,
            "label_profile_id": lid,
            "model_profile_id": mid,
            "quantile": q,
            "visualization_type": visualization_type,
            "blocks_rendered": len(lons),
            "layers_rendered": layers_rendered,
            "score_range": {
                "min": round(min(scores), 4),
                "max": round(max(scores), 4),
            },
        })
        if used_fallback:
            result["fallback"] = (
                f"Visualization CSV не найден, использованы сырые артефакты: "
                f"output/models/{mid}/scores.csv + output/blocks.csv"
            )
        if warnings_out:
            result["warnings"] = warnings_out
        return json.dumps(result, ensure_ascii=False, indent=2)

    return [
        datacube_overview,
        datacube_block_scores,
        datacube_block_detail,
        datacube_report_overview,
        datacube_score_overlay,
    ]
