"""
GIS Agent — pydantic-ai агент, работающий через OpenRouter
и использующий GIS MCP Server как источник инструментов.

Режимы подключения к MCP (задаются через .env или переменные окружения):

  MCP_MODE=http  (по умолчанию)
      Подключается к уже запущенному контейнеру gis-mcp.
      URL задаётся через MCP_URL (по умолчанию http://localhost:10002/mcp).

  MCP_MODE=stdio
      Запускает сервер как локальный subprocess (без Docker).

Запуск: python run_agent.py
Переменные окружения: LLM, API_KEY, [MCP_MODE], [MCP_URL]
"""

import asyncio

from pydantic_ai import Agent
from pydantic_ai.mcp import MCPServerStdio, MCPServerStreamableHTTP
from pydantic_ai.models.openrouter import OpenRouterModel
from pydantic_ai.providers.openrouter import OpenRouterProvider

import os
from dotenv import load_dotenv

load_dotenv()

MODEL = os.environ['LLM']
API_KEY = os.environ["API_KEY"]
MCP_MODE = os.getenv("MCP_MODE", "http")
MCP_URL = os.getenv("MCP_URL", "http://localhost:10002/mcp")

async def main():
    if MCP_MODE == "stdio":
        server = MCPServerStdio(
            "/Users/technocreep/miniconda3/envs/arcgis-env/bin/python",
            args=["-m", "mcp_server.server"],
            cwd="/Users/technocreep/Desktop/working-folder/arcgis-mcp/arcgis_mcp",
        )
        print(f"GIS Agent | MCP: stdio (локальный subprocess)")
    else:
        server = MCPServerStreamableHTTP(MCP_URL)
        print(f"GIS Agent | MCP: {MCP_URL}")

    model = OpenRouterModel(MODEL, provider=OpenRouterProvider(api_key=API_KEY))

    agent = Agent(
        model,
        toolsets=[server],
        system_prompt=(
            "Ты — ГИС-аналитик. Используй доступные инструменты для работы "
            "с геобазой: список слоёв, описание, запросы, статистика, вложения. "
            "Отвечай на русском. Будь кратким и точным."
        ),
    )

    print(f"GIS Agent | модель: {MODEL}")
    print("Введите запрос (пустая строка = выход):\n")

    async with agent:
        history = []
        while True:
            try:
                user_input = input("> ")
            except (KeyboardInterrupt, EOFError):
                print("\nВыход.")
                break

            if not user_input.strip():
                continue

            result = await agent.run(user_input, message_history=history)
            history = result.all_messages()
            print(f"\n{result.output}\n")


if __name__ == "__main__":

    asyncio.run(main())
