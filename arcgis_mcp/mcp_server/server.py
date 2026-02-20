"""GIS Agent MCP Server.

Поддерживает два режима запуска (определяется через MCP_TRANSPORT):

  stdio  (по умолчанию) — для локального run_agent.py / pydantic-ai
  http                  — HTTP/SSE для Open WebUI и других клиентов;
                          слушает на MCP_PORT (по умолчанию 8000)

Запуск локально:
    python -m mcp_server.server

В Docker (Open WebUI):
    MCP_TRANSPORT=http MCP_PORT=8000 python -m mcp_server.server
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from fastmcp import FastMCP

from config import PROJECTS_DIR
from mcp_server.project_store import ProjectStore
from mcp_server.tools.attachments import make_tools as make_attachment_tools
from mcp_server.tools.inventory import make_tools as make_inventory_tools
from mcp_server.tools.izuchennost import make_tools as make_izuch_tools
from mcp_server.tools.query import make_tools as make_query_tools
from mcp_server.tools.viz_plot_layer import make_tools as make_plot_layer_tools
from mcp_server.tools.viz_plot_overlay import make_tools as make_plot_overlay_tools
from mcp_server.tools.viz_histogram import make_tools as make_plot_histogram_tools
from mcp_server.tools.viz_interactive import make_tools as make_plot_interactive_tools


mcp = FastMCP(
    name="GIS Agent Service",
    instructions=(
        "Ты — геоинформационный агент, работающий с данными геологических проектов. "
        "Всегда начинай с list_projects() → get_project_summary(project_id) — это установит "
        "контекст проекта для последующих вызовов. "
        "При общении с пользователем используй display_name слоёв, а не технические layer_id. "
        "Слои с needs_review=true требуют предупреждения: название может быть неточным. "
        "P0-инструменты (inventory) работают быстро — из manifest. "
        "P1-инструменты (query, search, attachments) читают .gdb напрямую — использовать "
        "только когда manifest не даёт достаточно информации. "
        "После вызова plot_layer, plot_overlay или plot_histogram вставь поле `markdown` из ответа "
        "напрямую в свой ответ — это отобразит изображение прямо в чате. "
        "После вызова plot_interactive вставь поле `link`. "
        "Если поля `markdown`/`link` отсутствуют (MinIO недоступен) — сообщи путь из поля `file`. "
        "СТИЛИ СЛОЁВ: "
        "Для геофизических слоёв (display_name содержит единицы в скобках: мГал, нТл, Э): "
        "  передавай color_field=<первое числовое поле из manifest fields для данного слоя>. "
        "Для геологических слоёв (базовая геология, стратиграфия): "
        "  передавай color_field='INDEX' в plot_layer. "
        "Для скважин: передавай color_field='POINT_Z'. "
        "Для линеаментов: используй color='#00FF00' (лайм) в plot_overlay. "
        "Для экстремумов (полож./отриц.): используй color='red'/'blue' и marker='^'/'v'. "
        "Для тектоники: в plot_overlay задавай разные color/linewidth для надвигов и разломов. "
        "show_license=True ВСЕГДА — контур лицензии задаёт границы карты."
    ),
)

store = ProjectStore(str(PROJECTS_DIR))


_state: dict = {"current_project_id": None}


_all_tools = (
    make_inventory_tools(store, _state)
    + make_query_tools(store, _state)
    + make_izuch_tools(store, _state)
    + make_attachment_tools(store, _state)
    + make_plot_layer_tools(store, _state)
    + make_plot_overlay_tools(store, _state)
    + make_plot_histogram_tools(store, _state)
    + make_plot_interactive_tools(store, _state)
)

for _tool_fn in _all_tools:
    mcp.add_tool(_tool_fn)


if __name__ == "__main__":
    transport = os.getenv("MCP_TRANSPORT", "stdio")
    port = int(os.getenv("MCP_PORT", "8000"))

    if transport == "stdio":
        mcp.run(transport="stdio")
    else:
        mcp.run(transport=transport, host="0.0.0.0", port=port)
