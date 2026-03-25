Prototype UI and backend

This folder contains a small FastAPI prototype backend and a Vite/React frontend prototype.

Backend
- Files: `_main_server.py`, `app.py`.
- Purpose: accept an uploaded file (`/upload`), save it to `_UI/tmp/uploads`, and serve the existing static dashboard found in the repository at `output_lekyn/dashboard`.

Quick start (from repository root):

1. Create a virtualenv and install dependencies:

```bash
cd _UI
python3 -m venv .venv
source .venv/bin/activate
pip install fastapi uvicorn
```

2. Run the backend:

```bash
python _main_server.py
```

The backend listens on `http://0.0.0.0:8000`. Endpoints:
- `POST /upload` — accepts a file upload (multipart/form-data, field name `file`).
- `GET /dashboard/index.html` — serves the dashboard `index.html` from `output_lekyn/dashboard`.
- `GET /dashboard/list` — lists files in the dashboard folder.

If your dashboard directory is in a different place, set the `DASHBOARD_PATH` environment variable to the folder containing `index.html`.

Frontend
- The frontend is under `_front_end` (Vite app). Start it with `npm install` and `npm run dev`.
- The prototype frontend posts uploaded files to `http://localhost:8000/upload` and, on success, opens `http://localhost:8000/dashboard/index.html` in an iframe for preview.

Notes
- This is a prototype: the backend currently does not run the analysis pipeline on uploaded files. It demonstrates the upload flow and previewing the existing dashboard output. I can wire the upload handler to invoke your pipeline (e.g. `data.dashboard.lekyn_pipeline_example`) if you want — that requires specifying how the pipeline should be invoked and what inputs it expects.
