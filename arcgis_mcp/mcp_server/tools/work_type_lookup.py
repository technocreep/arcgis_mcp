"""MCP инструмент lookup_work_types — расшифровка кодов видов геологических работ."""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Callable

logger = logging.getLogger(__name__)

_SPEC_PATH = Path(__file__).parent.parent.parent / "rag" / "pdf_spec.json"


def _load_spec() -> tuple[dict[str, str], dict[str, str]]:
    """Вернуть (code→desc, not_code→desc). Молча возвращает пустые если файл недоступен."""
    try:
        spec = json.loads(_SPEC_PATH.read_text(encoding="utf-8"))
    except Exception as e:
        logger.warning("pdf_spec.json недоступен: %s", e)
        return {}, {}

    lookup: dict[str, str] = {}
    skip = {"_meta", "суффиксы_составных_кодов", "не_являются_кодами_видов_работ"}
    for key, section in spec.items():
        if key in skip:
            continue
        if isinstance(section, dict):
            for sub in section.values():
                if isinstance(sub, dict):
                    for code, desc in sub.items():
                        lookup[code] = desc

    not_codes: dict[str, str] = {
        k: v
        for k, v in spec.get("не_являются_кодами_видов_работ", {}).items()
        if not k.startswith("_")
    }
    return lookup, not_codes


def make_tools(state: dict) -> list[Callable]:

    def lookup_work_types(codes: list[str]) -> str:
        """Расшифровать аббревиатуры видов геологических работ (поле 8 карточки изученности).

        Принимает список кодов и возвращает их официальные наименования
        по справочнику Росгеолфонда 1995 г. (Приложение 1 к Инструкции по учёту
        геологической изученности территории РФ).

        Примеры кодов: ГС, ГДП, ТЕМ, ТЕМ-гф, ПР, ПО-НМ, АМС, ГХ, ЭГ.
        Составные коды формируются через дефис (ТЕМ-гх, ПР-Ц, ПО-Б).
        Также распознаёт аббревиатуры, которые НЕ являются кодами вида работ
        (ТГФ, НТС, ГКЗ, ТКЗ) — они встречаются в других полях карточки.

        Args:
            codes: список аббревиатур для расшифровки.
        """
        lookup, not_codes = _load_spec()
        results = {}
        for code in codes:
            if code in lookup:
                results[code] = lookup[code]
            elif code in not_codes:
                results[code] = f"[не является кодом вида работ] {not_codes[code]}"
            else:
                results[code] = "не найден в справочнике"

        return json.dumps(results, ensure_ascii=False, indent=2)

    return [lookup_work_types]
