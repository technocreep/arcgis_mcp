"""Парсер карточек изученности из PDF-вложений ГИС.

Структура карточки соответствует форме УЧЕТНАЯ КАРТОЧКА ИЗУЧЕННОСТИ
(поля 1-28, Росгеолфонд). Пример: АГ-R42-42.pdf.

Режимы работы:
  - Vision LLM (по умолчанию, если KG_LLM_MODEL задан):
      fitz рендерит страницы в PNG → отправляет в vLLM (OpenAI vision API) →
      LLM возвращает структурированный JSON.
  - Regex fallback (если KG_LLM_MODEL не задан):
      fitz извлекает текст → regex-паттерны по полям 1-28.
"""

from __future__ import annotations

import base64
import json
import logging
import re
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)


@dataclass
class InvestigationCardData:
    reg_number: str = ""             # поле 1:  номер госрегистрации — PRIMARY KEY
    inventory_rosgeolfond: str = ""  # поле 4.1: номер в Росгеолфонде
    inventory_tgf: str = ""          # поле 4.2: номер в ТГФ
    title: str = ""                  # поле 7:  название отчёта
    authors: str = ""                # поле 6:  авторы
    organization: str = ""           # поле 12: организация
    year_start: int | None = None    # поле 10.1: год начала
    year_end: int | None = None      # поле 10.2: год окончания
    purpose: str = ""                # поле 13: целевое назначение
    minerals: list[str] = field(default_factory=list)  # поле 14
    reserves_calculated: bool = False  # поле 15.1
    resources_calculated: bool = False  # поле 15.2
    work_type: str = ""              # поле 8:  индекс вида работ (ГС, ГДП и т.д.)
    scale: str = ""                  # поле 9:  масштаб
    abstract_methods: str = ""       # поле 17.1
    abstract_results: str = ""       # поле 17.2
    abstract_conclusions: str = ""   # поле 17.3
    keywords: list[str] = field(default_factory=list)  # поле 18
    area_km2: float | None = None    # поле 23: площадь
    bbox: dict | None = None         # поле 22: {n, s, e, w} WGS84
    sheet_nomenclature: str = ""     # поле 3:  номенклатура (R-42)
    region_okrug: str = ""           # поле 11.3
    region_oblast: str = ""          # поле 11
    completion_status: str = ""      # поле 27: "завершены" / "в работе"


# ---------------------------------------------------------------------------
# Промпт для vision LLM
# ---------------------------------------------------------------------------

_EXTRACTION_PROMPT = """\
Перед тобой страницы PDF «УЧЁТНАЯ КАРТОЧКА ИЗУЧЕННОСТИ» (форма Росгеолфонда, поля 1-28).
Извлеки данные и верни ТОЛЬКО валидный JSON без markdown-обёртки и пояснений:

{
  "reg_number": "номер госрегистрации (поле 1), только цифры",
  "inventory_rosgeolfond": "поле 4.1",
  "inventory_tgf": "поле 4.2",
  "sheet_nomenclature": "номенклатура листов (поле 3), формат R-42 или Q-43",
  "authors": "поле 6, строка с авторами",
  "title": "название отчёта (поле 7)",
  "work_type": "индекс вида работ (поле 8), например ГС, ГДП, АМС",
  "scale": "масштаб (поле 9), формат 1:200000",
  "year_start": <целое число, поле 10.1, или null>,
  "year_end": <целое число, поле 10.2, или null>,
  "region_oblast": "область (поле 11)",
  "region_okrug": "авт. округ (поле 11.3)",
  "organization": "поле 12",
  "purpose": "целевое назначение (поле 13)",
  "minerals": ["список", "полезных", "ископаемых", "поле 14"],
  "reserves_calculated": <true или false, поле 15.1>,
  "resources_calculated": <true или false, поле 15.2>,
  "abstract_methods": "поле 17.1 — методика и объёмы",
  "abstract_results": "поле 17.2 — основные результаты",
  "abstract_conclusions": "поле 17.3 — выводы",
  "keywords": ["поле 18"],
  "bbox": {"n": <float>, "s": <float>, "e": <float>, "w": <float>},
  "area_km2": <float или null, поле 23>,
  "completion_status": "поле 27, например завершены"
}

bbox — координаты в десятичных градусах WGS84 (север, юг, восток, запад).
Если поле отсутствует — null. Не добавляй ничего вне JSON.
"""


# ---------------------------------------------------------------------------
# Публичный API
# ---------------------------------------------------------------------------

def parse_investigation_card(pdf_bytes: bytes) -> InvestigationCardData | None:
    """Извлечь структурированные данные из PDF карточки изученности.

    Использует vision LLM если KG_LLM_MODEL задан, иначе regex-fallback.
    """
    try:
        import config
        use_vision = bool(config.KG_LLM_MODEL)
    except Exception:
        use_vision = False

    if use_vision:
        try:
            return _parse_vision(pdf_bytes)
        except Exception as e:
            logger.warning("Vision-парсер не справился (%s), fallback на regex", e)

    return _parse_regex(pdf_bytes)


# ---------------------------------------------------------------------------
# Vision LLM парсер
# ---------------------------------------------------------------------------

def _parse_vision(pdf_bytes: bytes) -> InvestigationCardData | None:
    """Рендерит страницы PDF в PNG и отправляет в vLLM (OpenAI vision API)."""
    try:
        import fitz
    except ImportError:
        logger.warning("pymupdf не установлен")
        return None

    import config
    from openai import OpenAI

    # --- Рендер страниц в PNG (150 DPI) ---
    try:
        doc = fitz.open(stream=pdf_bytes, filetype="pdf")
        images_b64: list[str] = []
        mat = fitz.Matrix(150 / 72, 150 / 72)
        for page in doc:
            pix = page.get_pixmap(matrix=mat)
            images_b64.append(base64.b64encode(pix.tobytes("png")).decode())
        doc.close()
    except Exception as e:
        logger.warning("Ошибка рендеринга PDF: %s", e)
        return None

    if not images_b64:
        return None

    # --- Формируем multimodal сообщение ---
    # ВАЖНО: для Pixtral/Mistral изображения должны идти ПЕРЕД текстом.
    # Mistral-tokenizer внутренне конвертирует image_url в tool-like токены,
    # и если после них стоит текст в той же роли user — валидатор падает с
    # "Unexpected role 'user' after role 'tool'".
    content: list[dict] = []
    for img in images_b64:
        content.append({
            "type": "image_url",
            "image_url": {"url": f"data:image/png;base64,{img}"},
        })
    content.append({"type": "text", "text": _EXTRACTION_PROMPT})

    # --- Вызов LLM ---
    client = OpenAI(base_url=config.KG_LLM_BASE_URL, api_key=config.KG_LLM_API_KEY)
    resp = client.chat.completions.create(
        model=config.KG_LLM_MODEL,
        messages=[{"role": "user", "content": content}],
        max_tokens=2000,
        temperature=0,
    )

    raw = resp.choices[0].message.content or ""
    # Убрать markdown code fences если LLM всё-таки добавил
    raw = re.sub(r"```(?:json)?\s*", "", raw).strip("`").strip()

    try:
        data = json.loads(raw)
    except json.JSONDecodeError as e:
        logger.warning("LLM вернул невалидный JSON: %s\n%s", e, raw[:300])
        return None

    card = _dict_to_card(data)
    if not card.reg_number:
        logger.debug("Vision-парсер: reg_number пустой, карточка не распознана")
        return None
    return card


def _dict_to_card(data: dict) -> InvestigationCardData:
    """Конвертировать dict (от LLM) в InvestigationCardData."""
    def _str(v) -> str:
        return str(v).strip() if v is not None else ""

    def _int(v) -> int | None:
        try:
            return int(v) if v is not None else None
        except (TypeError, ValueError):
            return None

    def _float(v) -> float | None:
        try:
            return float(v) if v is not None else None
        except (TypeError, ValueError):
            return None

    def _strlist(v) -> list[str]:
        if isinstance(v, list):
            return [str(x).strip().lower() for x in v if x]
        if isinstance(v, str) and v:
            return [s.strip().lower() for s in re.split(r"[,;]+", v) if s.strip()]
        return []

    card = InvestigationCardData()
    card.reg_number             = _str(data.get("reg_number"))
    card.inventory_rosgeolfond  = _str(data.get("inventory_rosgeolfond"))
    card.inventory_tgf          = _str(data.get("inventory_tgf"))
    card.sheet_nomenclature     = _str(data.get("sheet_nomenclature"))
    card.authors                = _str(data.get("authors"))
    card.title                  = _str(data.get("title"))[:500]
    card.work_type              = _str(data.get("work_type"))[:100]
    card.scale                  = _str(data.get("scale"))
    card.year_start             = _int(data.get("year_start"))
    card.year_end               = _int(data.get("year_end"))
    card.region_oblast          = _str(data.get("region_oblast"))[:100]
    card.region_okrug           = _str(data.get("region_okrug"))
    card.organization           = _str(data.get("organization"))[:300]
    card.purpose                = _str(data.get("purpose"))[:500]
    card.minerals               = _strlist(data.get("minerals"))
    card.reserves_calculated    = bool(data.get("reserves_calculated"))
    card.resources_calculated   = bool(data.get("resources_calculated"))
    card.abstract_methods       = _str(data.get("abstract_methods"))[:2000]
    card.abstract_results       = _str(data.get("abstract_results"))[:2000]
    card.abstract_conclusions   = _str(data.get("abstract_conclusions"))[:2000]
    card.keywords               = [s for s in _strlist(data.get("keywords")) if len(s) > 2][:30]
    card.area_km2               = _float(data.get("area_km2"))
    card.completion_status      = _str(data.get("completion_status"))[:50]

    bbox = data.get("bbox")
    if isinstance(bbox, dict) and all(k in bbox for k in ("n", "s", "e", "w")):
        try:
            card.bbox = {k: round(float(bbox[k]), 4) for k in ("n", "s", "e", "w")}
        except (TypeError, ValueError):
            card.bbox = None

    return card


# ---------------------------------------------------------------------------
# Regex fallback парсер (оригинальный)
# ---------------------------------------------------------------------------

def _parse_regex(pdf_bytes: bytes) -> InvestigationCardData | None:
    """Извлечь данные из PDF через fitz text + regex-паттерны (fallback)."""
    try:
        import fitz
    except ImportError:
        logger.warning("pymupdf не установлен, пропуск PDF парсинга")
        return None

    try:
        doc = fitz.open(stream=pdf_bytes, filetype="pdf")
        text = "\n".join(page.get_text() for page in doc)
        doc.close()
    except Exception as e:
        logger.warning("Ошибка чтения PDF: %s", e)
        return None

    if "КАРТОЧКА ИЗУЧЕННОСТИ" not in text.upper():
        return None

    card = InvestigationCardData()

    # --- Поле 1: Номер госрегистрации ---
    m = re.search(r'1\.\s*Номер\s+госрегистрации\s+(\d+)', text, re.IGNORECASE)
    if m:
        card.reg_number = m.group(1).strip()

    # --- Поле 4.1 / 4.2: Инвентарные номера ---
    m = re.search(r'4\.1\.\s*Росгеолфонда\s+(\S+)', text, re.IGNORECASE)
    if m:
        card.inventory_rosgeolfond = m.group(1).strip()
    m = re.search(r'4\.2\.\s*Т[ГТ]Ф\s+[№N]?\s*(\S+)', text, re.IGNORECASE)
    if m:
        card.inventory_tgf = m.group(1).strip()

    # --- Поле 3: Номенклатура миллионных листов ---
    m = re.search(r'3\.\s*Номенклатура.*?листов\s*\n\s*([A-Z]-\d+\S*)', text, re.IGNORECASE)
    if m:
        card.sheet_nomenclature = m.group(1).strip()
    else:
        m = re.search(r'\b([A-Z]-\d{2}(?:-\d+)?)\b', text)
        if m:
            card.sheet_nomenclature = m.group(1)

    # --- Поле 6: Авторы ---
    m = re.search(r'6\.\s*Авторы.*?\n\s*(.+?)(?:\n|7\.)', text, re.DOTALL | re.IGNORECASE)
    if m:
        authors_raw = m.group(1).strip().split('\n')[0]
        card.authors = re.sub(r'\s+', ' ', authors_raw).strip()

    # --- Поле 7: Название отчёта ---
    m = re.search(
        r'7\.\s*Название\s+отчета\s*\n(.*?)(?:\n\s*8\.|10\.Год)',
        text, re.DOTALL | re.IGNORECASE
    )
    if m:
        raw = re.sub(r'\s+', ' ', m.group(1)).strip()
        card.title = raw[:500]

    # --- Поле 8: Индекс вида/стадии/метода ---
    m = re.search(r'8\.\s*Индекс.*?метода\s*/\s*\n?\s*([^\n]+)', text, re.IGNORECASE)
    if m:
        card.work_type = m.group(1).strip()[:100]

    # --- Поле 9: Масштаб ---
    m = re.search(r'9\.\s*Масштаб\s*\n?\s*(1:\d[\d\s.]+)', text, re.IGNORECASE)
    if m:
        card.scale = re.sub(r'\s', '', m.group(1)).strip()
    else:
        m = re.search(r'(1:\s*\d{2,3}\s*[\d.]*\s*000)', text)
        if m:
            card.scale = re.sub(r'\s', '', m.group(1))

    # --- Поля 10.1 / 10.2: Годы ---
    m = re.search(r'10\.1\.начала работ\s*\n\s*(\d{4})', text, re.IGNORECASE)
    if m:
        card.year_start = int(m.group(1))
    m = re.search(r'10\.2\.окончания\s+работ\s*\n\s*(\d{4})', text, re.IGNORECASE)
    if m:
        card.year_end = int(m.group(1))
    if not card.year_start or not card.year_end:
        years = re.findall(r'\b(19\d{2}|20\d{2})\b', text)
        if len(years) >= 2:
            card.year_start = card.year_start or int(years[0])
            card.year_end = card.year_end or int(years[-1])

    # --- Поле 11 / 11.3: Регион ---
    m = re.search(r'11\.3\s+Авт\.\s+округ\s+([^\n]+)', text, re.IGNORECASE)
    if m:
        card.region_okrug = m.group(1).strip()
    m = re.search(r'Область\s+([^\n]+)', text, re.IGNORECASE)
    if m:
        card.region_oblast = m.group(1).strip()[:100]

    # --- Поле 12: Организация ---
    m = re.search(
        r'12\.\s*Организация.*?\n\s*(.+?)(?:\n\s*13\.|\n\s*14\.)',
        text, re.DOTALL | re.IGNORECASE
    )
    if m:
        raw = re.sub(r'\s+', ' ', m.group(1)).strip()
        card.organization = raw[:300]

    # --- Поле 13: Целевое назначение ---
    m = re.search(
        r'13\.\s*Целевое\s+назначение\s*\n(.*?)(?:\n\s*14\.)',
        text, re.DOTALL | re.IGNORECASE
    )
    if m:
        raw = re.sub(r'\s+', ' ', m.group(1)).strip()
        card.purpose = raw[:500]

    # --- Поле 14: Полезные ископаемые ---
    m = re.search(
        r'14\.\s*Полезные\s+ископаемые\s*\n\s*(.+?)(?:\n\s*15\.)',
        text, re.DOTALL | re.IGNORECASE
    )
    if m:
        minerals_raw = m.group(1).strip().split('\n')[0]
        card.minerals = [
            s.strip().lower() for s in re.split(r'[,;]+', minerals_raw) if s.strip()
        ]

    # --- Поля 15.1 / 15.2: Запасы/ресурсы ---
    m = re.search(r'15\.1\.запасов\s*\n\s*(Да|Нет|да|нет)', text, re.IGNORECASE)
    if m:
        card.reserves_calculated = m.group(1).lower() == "да"
    m = re.search(r'15\.2\.ресурсов\s*\n\s*(Да|Нет|да|нет)', text, re.IGNORECASE)
    if m:
        card.resources_calculated = m.group(1).lower() == "да"

    # --- Поля 17.1-17.3: Реферат ---
    m = re.search(
        r'17\.1\.Методика и объемы\s*(.*?)17\.2\.Основные результаты',
        text, re.DOTALL | re.IGNORECASE
    )
    if m:
        card.abstract_methods = re.sub(r'\s+', ' ', m.group(1)).strip()[:2000]

    m = re.search(
        r'17\.2\.Основные результаты\s*(.*?)17\.3\.Выводы',
        text, re.DOTALL | re.IGNORECASE
    )
    if m:
        card.abstract_results = re.sub(r'\s+', ' ', m.group(1)).strip()[:2000]

    m = re.search(
        r'17\.3\.Выводы и рекомендации\s*(.*?)(?:18\.|Ключевые слова)',
        text, re.DOTALL | re.IGNORECASE
    )
    if m:
        card.abstract_conclusions = re.sub(r'\s+', ' ', m.group(1)).strip()[:2000]

    # --- Поле 18: Ключевые слова ---
    m = re.search(
        r'18\.\s*Ключевые\s+слова\s*\n(.*?)(?:\n\s*19\.)',
        text, re.DOTALL | re.IGNORECASE
    )
    if m:
        kw_raw = m.group(1).strip()
        card.keywords = [
            s.strip() for s in re.split(r'[,;]+', re.sub(r'\s+', ' ', kw_raw))
            if s.strip() and len(s.strip()) > 2
        ][:30]

    # --- Поле 23: Площадь ---
    m = re.search(r'23\.\s*Величина\s+изученной\s+площади.*?(\d+[\.,]\d+|\d+)\s*(?:км|$)', text, re.IGNORECASE)
    if m:
        try:
            card.area_km2 = float(m.group(1).replace(',', '.'))
        except ValueError:
            pass

    # --- Поле 22: Координаты → bbox ---
    card.bbox = _parse_bbox(text)

    # --- Поле 27: Завершённость ---
    m = re.search(r'27\.\s*Завершенность\s+работ\s*\n\s*(.+?)(?:\n|28\.)', text, re.IGNORECASE)
    if m:
        card.completion_status = m.group(1).strip()[:50]
    else:
        if re.search(r'завершен', text, re.IGNORECASE):
            card.completion_status = "завершены"

    return card if card.reg_number else None


def _parse_bbox(text: str) -> dict | None:
    """Распарсить таблицу координат → {n, s, e, w} в градусах WGS84."""
    m = re.search(r'22\.\s*Координаты(.*?)(?:23\.|$)', text, re.DOTALL | re.IGNORECASE)
    coord_text = m.group(1) if m else text

    rows = re.findall(r'(\d{1,3})\s+(\d{1,2})\s+(\d{1,3})\s+(\d{1,2})', coord_text)
    if len(rows) < 2:
        return None

    lats, lons = [], []
    for lat_d, lat_m, lon_d, lon_m in rows:
        lat = int(lat_d) + int(lat_m) / 60
        lon = int(lon_d) + int(lon_m) / 60
        if 30 <= lat <= 90 and 20 <= lon <= 200:
            lats.append(lat)
            lons.append(lon)

    if not lats or not lons:
        return None

    return {
        "n": round(max(lats), 4),
        "s": round(min(lats), 4),
        "e": round(max(lons), 4),
        "w": round(min(lons), 4),
    }
