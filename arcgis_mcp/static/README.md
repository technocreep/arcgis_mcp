# static/ — фронтенд GIS Data Hub

Статические файлы, раздаваемые сервисом `gis-loader` по маршруту `/ui/`.

## Структура

```
static/
├── index.html      # GIS Data Hub — основной портал (SPA)
├── app.js          # Vue 3 логика портала
├── style.css       # Стили портала
└── datacube/       # Data Cube Viewer (отдельная страница)
    ├── index.html
    ├── viewer.js
    ├── viewer.css
    ├── lightbox.js              # Lightbox для изображений в dashboard-отчётах
    ├── dashboard-override.css   # CSS-переопределения для инжектируемых HTML-страниц
    └── README.md   # описание V-блоков и источников данных
```

---

## index.html + app.js — GIS Data Hub Portal

Vue 3 (CDN, Composition API) SPA на Tailwind CSS + Font Awesome 6. Доступен по адресу `http://localhost:10003/ui/`.

### Экраны и функции

**Авторизация**
- Форма входа с Basic Auth → `/api/auth`
- Учётные данные хранятся в `localStorage`, подставляются в заголовки запросов

**Список проектов** (`/api/projects`)
- Карточки всех загруженных проектов со skeleton-анимацией при загрузке
- Цветная полоска левого бордера: emerald — если есть Data Cube артефакты, slate — иначе
- Индикаторы прямо на карточке:
  - спиннер + текущий этап + таймер — пока задача Data Cube запущена
  - кнопка «Cube ready» — после успешного завершения
- Кнопки действий: Observe (JSON), Data Cube, Delete (иконка в шапке карточки)

**Загрузка проекта** (`/api/upload`)
- Поля: `project_id`, `.gdb` (drag-and-drop зона или клик), `.aprx` (опционально)
- Двухфазный прогресс-бар:
  1. Uploading — реальный процент через `XMLHttpRequest.upload.onprogress`
  2. Processing… — неопределённый shimmer-прогресс пока сервер выполняет пайплайн

**Observe** (модальное окно)
- Полный `manifest.json` проекта с интерактивным JSON-деревом (сворачиваемые ноды)

**Data Cube** (модальное окно)
- Запуск ML-пайплайна: `POST /api/datacube/jobs`
- Polling статуса: `GET /api/datacube/jobs/{jobId}` каждые 2 с
- Закрытие модалки **не прерывает** задачу — polling продолжается фоново
- Визуальные этапы: `grid → features → qa → labels → training → evaluation → visualization → upload`
  - emerald = завершён, синий = активен, серый = ожидание, красный = ошибка
- Счётчик прошедшего времени (elapsed timer)
- Кнопки: Close (скрывает модалку), Run again (сброс + новый запуск), View Artifacts

**Delete** (`DELETE /api/projects/{id}`)
- Встроенная модалка подтверждения (вместо `window.confirm()`)

**Toast-уведомления**
- Заменяют все `window.alert()`: success / error / info / warn
- Автоматически скрываются через 4 секунды

**Общее**
- Все модалки закрываются по `Escape` (кроме Data Cube во время активного расчёта)
- `Cache-Control: no-cache` на все `/ui/*` маршруты — достаточно обычного F5 для получения обновлений

---

## app.js — ключевые refs и функции

| Ref / функция | Назначение |
|---|---|
| `toasts` / `addToast(msg, type)` | Toast-система |
| `uploadProgress`, `uploadPhase` | Прогресс загрузки файла |
| `gdbFile`, `gdbDragOver`, `onGdbDrop` | Drag-and-drop зона |
| `deleteTarget`, `confirmDelete`, `doDelete` | Модалка удаления |
| `activeJobProjectId` | Глобальный ID проекта с активной задачей |
| `dataCubeStage`, `dcElapsed`, `dcElapsedLabel` | Прогресс и таймер Data Cube |
| `closeDataCube()` | Скрыть модалку, не останавливая задачу |
| `resetDataCube()` | Полный сброс состояния (для «Run again») |
| `isJobRunning(id)`, `cubeReady(id)` | Состояние задачи для карточек |
| `pollDataCube(projectId, jobId)` | Polling с обновлением стадии |

---

## style.css — ключевые классы

| Класс | Назначение |
|---|---|
| `.toast` + модификаторы `.success/.error/.info/.warn` | Toast-уведомления |
| `.skeleton-card` | Shimmer-карточка при загрузке списка проектов |
| `.skeleton-progress` | Amber shimmer для фазы Processing в upload |
| `.drop-zone` / `.drop-zone--over` / `.drop-zone--filled` | Зона drag-and-drop |
| `.project-card` / `.project-card.has-cube` | Карточка с цветным бордером |
| `.dc-stage-pill` | Пилюли этапов Data Cube прогресса |

---

## datacube/ — Data Cube Viewer

Отдельная страница, открывается в новой вкладке: `/ui/datacube/?project_id={id}`

Загружает ML-артефакты через `/api/projects/{id}/datacube/files/{path}` и отображает 11 визуализационных блоков (V0–V11).

**Lightbox:** все HTML-страницы dashboard-отчётов (подаваемые через `datacube_file_serve`) получают инжекцию `lightbox.js` и `dashboard-override.css`. Клик по любому изображению с атрибутом `data-lb` открывает его в полноэкранном оверлее с навигацией стрелками и Escape.

Подробное описание блоков и источников данных — в [datacube/README.md](datacube/README.md).
