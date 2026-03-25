from __future__ import annotations

import json
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Optional


INDEX_FILENAME = '_index.json'


def _index_path(projects_root: Path | str) -> Path:
    return Path(projects_root) / INDEX_FILENAME


def list_projects(projects_root: Path | str) -> List[Dict]:
    p = _index_path(projects_root)
    if not p.exists():
        return []
    with open(p, 'r', encoding='utf-8') as fh:
        try:
            data = json.load(fh)
            # Support either {"projects": [...]} or a plain list
            if isinstance(data, dict):
                return data.get('projects', []) or []
            if isinstance(data, list):
                return data
            return []
        except Exception:
            return []


def write_index(projects_root: Path | str, entries: List[Dict]):
    p = _index_path(projects_root)
    # Preserve existing shape if possible (dict with 'projects' vs plain list)
    if p.exists():
        try:
            with open(p, 'r', encoding='utf-8') as fh:
                existing = json.load(fh)
            if isinstance(existing, dict):
                out = {**existing}
                out['projects'] = entries
                with open(p, 'w', encoding='utf-8') as fh:
                    json.dump(out, fh, ensure_ascii=False, indent=2)
                return
        except Exception:
            # fallback to canonical dict shape
            pass

    # Default: write entries as canonical dict with 'projects' key
    out = {'projects': entries}
    with open(p, 'w', encoding='utf-8') as fh:
        json.dump(out, fh, ensure_ascii=False, indent=2)


def add_or_update_project(projects_root: Path | str, entry: Dict) -> None:
    """Add or update a project entry in _index.json.

    `entry` should include at least an `id` field. Missing fields will be added
    from defaults (e.g., `created_at`). This function is idempotent for the same id.
    """
    entries = list_projects(projects_root)
    project_id = entry.get('id')
    if not project_id:
        raise ValueError('entry must contain id')

    # Ensure canonical fields exist
    if 'created_at' not in entry:
        entry['created_at'] = datetime.utcnow().isoformat() + 'Z'
    entry.setdefault('name', project_id)
    entry.setdefault('layers_count', 0)
    entry.setdefault('has_attachments', False)
    entry.setdefault('gdb_file', None)
    entry.setdefault('primary_crs', None)
    entry.setdefault('metadata_completeness', None)

    # Replace existing entry or append
    replaced = False
    for i, e in enumerate(entries):
        if e.get('id') == project_id:
            merged = {**e, **entry}
            # keep created_at from existing if present
            if not merged.get('created_at'):
                merged['created_at'] = datetime.utcnow().isoformat() + 'Z'
            entries[i] = merged
            replaced = True
            break
    if not replaced:
        entries.append(entry)

    write_index(projects_root, entries)


def remove_project(projects_root: Path | str, project_id: str) -> bool:
    entries = list_projects(projects_root)
    new = [e for e in entries if e.get('id') != project_id]
    if len(new) == len(entries):
        return False
    write_index(projects_root, new)
    return True
