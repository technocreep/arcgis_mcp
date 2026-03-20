"""Парсер карточек изученности из PDF-вложений ГИС.

Структура карточки соответствует форме УЧЕТНАЯ КАРТОЧКА ИЗУЧЕННОСТИ
(поля 1-28, Росгеолфонд). Пример: АГ-R42-42.pdf.
"""

from __future__ import annotations

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


def parse_investigation_card(pdf_bytes: bytes) -> InvestigationCardData | None:
    """Извлечь структурированные данные из PDF карточки изученности."""
    try:
        import fitz  # PyMuPDF
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
        # Ищем паттерн типа R-42, Q-43 в тексте
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
        card.title = raw[:500]  # ограничиваем длину

    # --- Поле 8: Индекс вида/стадии/метода ---
    m = re.search(r'8\.\s*Индекс.*?метода\s*/\s*\n?\s*([^\n]+)', text, re.IGNORECASE)
    if m:
        card.work_type = m.group(1).strip()[:100]

    # --- Поле 9: Масштаб ---
    m = re.search(r'9\.\s*Масштаб\s*\n?\s*(1:\d[\d\s.]+)', text, re.IGNORECASE)
    if m:
        card.scale = re.sub(r'\s', '', m.group(1)).strip()
    else:
        # Ищем масштаб в тексте заголовка отчёта
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
    # Fallback: два 4-значных года рядом
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
    # Ищем блок таблицы с координатами после "22.Координаты"
    m = re.search(r'22\.\s*Координаты(.*?)(?:23\.|$)', text, re.DOTALL | re.IGNORECASE)
    coord_text = m.group(1) if m else text

    # Ищем строки вида: "68 0 67 0" (градусы минуты для широты и долготы)
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
