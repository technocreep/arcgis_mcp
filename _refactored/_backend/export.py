from __future__ import annotations

from pathlib import Path
import json
from typing import Dict, Any, Optional
import os
import time


def collect_basic_stats(project_dir: Path | str) -> Dict[str, Any]:
    """Collect basic file and manifest statistics for a project.

    Returns a dict with counts, sizes, and manifest summary suitable for frontend consumption.
    """
    p = Path(project_dir)
    if not p.exists():
        raise FileNotFoundError(str(p))

    stats: Dict[str, Any] = {}
    total_size = 0
    file_count = 0
    for root, dirs, files in os.walk(p):
        for fn in files:
            fp = Path(root) / fn
            try:
                total_size += fp.stat().st_size
                file_count += 1
            except Exception:
                pass

    stats['file_count'] = file_count
    stats['total_size'] = total_size
    stats['created_at'] = time.ctime(p.stat().st_ctime)

    # Try to read manifest.json if present
    manifest_file = p / 'manifest.json'
    manifest = None
    if manifest_file.exists():
        try:
            with open(manifest_file, 'r', encoding='utf-8') as fh:
                manifest = json.load(fh)
        except Exception:
            manifest = None

    if isinstance(manifest, dict):
        # provide small manifest summary
        stats['manifest_summary'] = {
            'id': manifest.get('id'),
            'title': manifest.get('title') or manifest.get('name'),
            'layers': len(manifest.get('layers', [])) if isinstance(manifest.get('layers'), list) else None,
        }
    else:
        stats['manifest_summary'] = None

    # detect gdb
    gdbs = [d.name for d in p.iterdir() if d.is_dir() and d.name.lower().endswith('.gdb')]
    stats['gdbs'] = gdbs

    return stats


def generate_simple_histogram(values, out_path: Path | str) -> Optional[str]:
    """Generate a simple histogram image (PNG) from a list of numeric values.

    Returns path to generated image or None if plotting libraries are unavailable.
    """
    try:
        import matplotlib.pyplot as plt
    except Exception:
        return None

    out = Path(out_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    plt.figure(figsize=(6, 4))
    plt.hist(values, bins=30)
    plt.tight_layout()
    plt.savefig(str(out), dpi=150)
    plt.close()
    return str(out)
