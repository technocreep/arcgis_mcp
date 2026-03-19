# Data Cube Viewer — источники данных и блоки визуализации

## Инжекция в dashboard-отчёты

HTML-страницы, возвращаемые через `GET /api/projects/{id}/datacube/files/{path}`, автоматически получают:

- `dashboard-override.css` — переопределения стилей (инжектируется перед `</head>`)
- `lightbox.js` — кликабельное полноэкранное превью для изображений (инжектируется перед `</body>`)

**Lightbox:** изображения, у которых задан атрибут `data-lb`, открываются в оверлее при клике. Навигация: стрелки / клавиши ← →, закрытие — Escape или клик вне изображения. Группировка изображений — по ближайшему контейнеру `.gallery`, `figure` или `section`.

---

## Артефакты

Все файлы хранятся в MinIO: `MINIO_CUBE_BUCKET/{project_id}/`

| Файл | Поля |
|------|------|
| `blocks.csv` | `block_id`, `row`, `col`, `x_m`, `y_m`, `lon`, `lat`, `cell_size_m` |
| `scores.csv` | `block_id`, `score` |
| `eval_report.json` | `metrics.pr_auc`, `capture_efficiency.x_volume_fraction[]`, `capture_efficiency.y_captured_fraction[]`, `capture_efficiency.x_star`, `capture_efficiency.score_threshold_at_x_star` |
| `model_meta.json` | `model_type`, `cv.mean_pr_auc`, `cv.effective_splits` |
| `run_meta.json` | `cube_spec`, `label_spec`, `train_spec`, `layer_mapping`, `derived` |
| `viz/map.html` | Folium-карта (pre-rendered) |
| `interpretability/global_importance_features.csv` | `feature`, `importance`, `group` |
| `interpretability/ale_1d.csv` | `feature`, `bin_center`, `ale` |
| `interpretability/shap_values.csv` | WIDE: `block_id`, `feat1`, `feat2`, … |
| `interpretability/dominant_driver_group.csv` | `block_id`, `dominant_driver_group` |
| `interpretability/shap_geo_unit_summary.csv` | `geo_unit`, `feature`, `mean_shap` |

---

## Блоки визуализации

### V0 — Интерактивная карта
Folium-карта в iframe.

**Источник:** `viz/map.html`

---

### V1 — Score-хороплет (canvas)
Сетка блоков, раскрашенных по score (viridis). Поддерживает зум колесом, перетаскивание и сброс двойным кликом. Клик по блоку открывает V7 с его SHAP-объяснением.

**Источники:** `blocks.csv` (координаты и размер ячеек) · `scores.csv` (цвет)

---

### V2 — Гистограмма скоров
20-бинная гистограмма распределения score по всем блокам (Chart.js). Показывает Q1, медиану, Q3 и n.

**Источник:** `scores.csv`

---

### V3 — Метрики модели
Четыре карточки с ключевыми показателями:
- **PR-AUC (test)** — `eval_report.metrics.pr_auc`
- **CV PR-AUC** — `model_meta.cv.mean_pr_auc`
- **x\* (volume)** — оптимальная доля площади для исследования (`capture_efficiency.x_star`)
- **Score @ x\*** — порог скора в точке x\* (`capture_efficiency.score_threshold_at_x_star`)

Также: тип модели и число CV-сплитов.

**Источники:** `eval_report.json` · `model_meta.json`

---

### V4 — Capture Efficiency Curve
Линейный график (Chart.js): доля захваченных рудных объектов vs доля исследованной площади. Пунктирная линия — случайный baseline. Вертикальная метка — x\*.

**Источник:** `eval_report.json` → `capture_efficiency.x_volume_fraction[]`, `y_captured_fraction[]`, `x_star`

---

### V5 — Feature Importance
Горизонтальный bar chart (Chart.js), топ-20 фич по важности. Цвет баров соответствует группе фичи.

**Источник:** `interpretability/global_importance_features.csv`

---

### V6 — ALE-кривые
Сетка до 8 мини-графиков (Chart.js line), по одному на фичу. Фичи отсортированы по убыванию важности. Показывает, как среднее предсказание меняется при изменении значения фичи.

**Источники:** `interpretability/ale_1d.csv` · `interpretability/global_importance_features.csv` (для сортировки)

---

### V7 — SHAP Waterfall (drilldown)
Горизонтальный bar chart (Chart.js) для выбранного блока: SHAP-вклад каждой фичи, отсортированный по |значению|. Зелёный — положительный вклад, красный — отрицательный. Активируется кликом по блоку в V1.

**Источник:** `interpretability/shap_values.csv` (WIDE-формат, строка по `block_id`)

---

### V8 — Dominant Driver Map
Canvas-хороплет. Каждый блок раскрашен по доминирующей группе факторов (`dominant_driver_group`). Легенда с категориальной палитрой.

**Источники:** `blocks.csv` · `interpretability/dominant_driver_group.csv`

---

### V9 — Run Parameters
Таблица параметров запуска пайплайна: все ключи верхнего уровня из `run_meta.json` с рекурсивным рендерингом вложенных объектов.

**Источник:** `run_meta.json`

---

### V10 — SHAP Heatmap by Geo-Unit
Canvas-тепловая карта: строки — geo_unit, колонки — фичи (топ-30 по суммарному |SHAP|). Цвет ячейки — дивергентная палитра (синий = отрицательный вклад, красный = положительный).

**Источник:** `interpretability/shap_geo_unit_summary.csv`

---

### V11 — Visualization Hub
Галерея HTML-отчётов, сгенерированных пайплайном визуализации (рецепты: `raster+contours`, `topq-vector`, `demo-investor`). Каждый рецепт открывается в iframe или новой вкладке. Изображения внутри отчётов поддерживают lightbox (клик для увеличения).

**Источник:** `viz/hub/index.html` и связанные HTML-страницы в `viz/`
