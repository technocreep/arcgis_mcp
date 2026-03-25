"""Unified FastAPI entrypoint for the package.

This module exposes the single FastAPI `app` used by the project.
Other server scripts should import `app` from here when they need to
start the HTTP service.
"""
from __future__ import annotations

from arcgis_mcp.api_server.server import app  # re-export the FastAPI app


if __name__ == "__main__":
    import os
    import uvicorn

    port = int(os.getenv("PORT", os.getenv("MCP_PORT", "8000")))
    uvicorn.run("arcgis_mcp.server:app", host="0.0.0.0", port=port)
