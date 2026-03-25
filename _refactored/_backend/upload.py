from __future__ import annotations

from pathlib import Path
import shutil
import zipfile
import tempfile
from typing import Iterable


def save_and_extract_project(projects_root: Path | str, project_id: str, archives: Iterable, aprx=None, atbx=None) -> dict:
    """Save uploaded archive(s) and optional files into a new project folder.

    Args:
        projects_root: path to projects directory
        project_id: slug for project
        archives: iterable of objects with `.filename` and `.file` (UploadFile-like)
        aprx, atbx: optional UploadFile-like objects

    Returns:
        dict with keys: project_dir (Path), extracted (bool), gdb_name (str|None)

    Raises HTTP-like exceptions (ValueError) for invalid inputs.
    """
    projects_root = Path(projects_root)
    project_dir = projects_root / project_id
    if project_dir.exists():
        raise ValueError(f"Project '{project_id}' already exists")

    tmp = Path(tempfile.mkdtemp(prefix='upload_'))
    try:
        project_dir.mkdir(parents=True, exist_ok=False)

        any_extracted = False
        # Normalize archives into a list
        arch_list = list(archives) if not hasattr(archives, 'read') else [archives]
        for idx, af in enumerate(arch_list):
            if af is None:
                continue
            filename = getattr(af, 'filename', f'archive_{idx}.zip')
            saved = project_dir / filename
            with open(saved, 'wb') as f:
                shutil.copyfileobj(af.file, f)

            # If it's a zip archive, extract into project_dir and remove the zip
            try:
                if zipfile.is_zipfile(saved):
                    with zipfile.ZipFile(saved, 'r') as zf:
                        zf.extractall(project_dir)
                    try:
                        saved.unlink()
                    except Exception:
                        pass
                    any_extracted = True
            except zipfile.BadZipFile:
                # leave the archive as-is
                pass

        # Save optional files into project root
        if aprx:
            aprx_filename = getattr(aprx, 'filename', None) or 'project.aprx'
            aprx_path = project_dir / aprx_filename
            with open(aprx_path, 'wb') as f:
                shutil.copyfileobj(aprx.file, f)
        if atbx:
            atbx_filename = getattr(atbx, 'filename', None) or 'toolbox.atbx'
            atbx_path = project_dir / atbx_filename
            with open(atbx_path, 'wb') as f:
                shutil.copyfileobj(atbx.file, f)

        # Detect .gdb folder
        gdb_name = None
        for d in project_dir.iterdir():
            if d.is_dir() and d.name.lower().endswith('.gdb'):
                gdb_name = d.name
                break

        return {"project_dir": project_dir, "extracted": any_extracted, "gdb_name": gdb_name}
    except Exception:
        # cleanup on error
        try:
            if project_dir.exists():
                shutil.rmtree(project_dir)
        except Exception:
            pass
        raise
    finally:
        shutil.rmtree(tmp, ignore_errors=True)
