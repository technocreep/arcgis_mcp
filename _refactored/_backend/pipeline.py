from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, Optional


def run_pipeline(project_dir: Path | str, manifest_path: Path | str, *, options: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """Run the ingestion pipeline for a prepared project.

    This is a thin wrapper: if the original `ingestion.pipeline.run_pipeline`
    function is importable, it will be used. Otherwise NotImplementedError is raised.

    Args:
        project_dir: path to project folder
        manifest_path: path to manifest.json
        options: optional dict of options forwarded to the underlying runner

    Returns:
        dict result from the pipeline runner.
    """
    try:
        # Import lazily to avoid hard dependency at module import time
        from ingestion.pipeline import run_pipeline as _run
    except Exception as exc:  # pragma: no cover - environment dependent
        raise NotImplementedError(
            "ingestion.pipeline.run_pipeline is not importable in this environment"
        ) from exc

    # Call the pipeline runner and return its result
    return _run(project_dir=project_dir, manifest_path=manifest_path, options=options)
