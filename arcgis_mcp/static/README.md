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
    └── README.md   # описание V-блоков и источников данных
```

---

## index.html + app.js — GIS Data Hub Portal

Vue 3 SPA. Доступен по адресу `http://localhost:10003/ui/`.

### Экраны и функции

**Авторизация**
- Форма входа с Basic Auth → `/api/auth`
- Токен хранится в `localStorage`, подставляется в заголовки запросов

**Список проектов** (`/api/projects`)
- Карточки всех загруженных проектов
- Индикатор наличия Data Cube артефактов (иконка, запрос к `/api/projects/{id}/datacube`)
- Кнопки: Observe, Data Cube, Delete

**Загрузка проекта** (`/api/upload`)
- Поля: `project_id`, `name`, `.gdb` файл, `.aprx` файл (опционально)
- Прогресс-бар загрузки

**Observe** (модальное окно)
- Полный `manifest.json` проекта с интерактивным JSON-деревом
- Список слоёв, метаданные, качество

**Data Cube** (модальное окно)
- Запуск ML-пайплайна: `POST /api/datacube/jobs` с параметрами `project_id`, `cube_params`, `label_params`, `train_params`
- Polling статуса задачи: `GET /api/datacube/jobs/{jobId}` каждые 2 с
- Прогресс-бар по стадиям пайплайна: `grid → features → labels → train → interpret → upload`
- После завершения: кнопка открыть Data Cube Viewer в новой вкладке

**Delete** (`DELETE /api/projects/{id}`)
- Подтверждение перед удалением

---

## datacube/ — Data Cube Viewer

Отдельная страница, открывается в новой вкладке: `/ui/datacube/?project_id={id}`

Загружает ML-артефакты из MinIO через `/api/projects/{id}/datacube/files/{path}` и отображает 11 визуализационных блоков (V0–V10).

Подробное описание блоков и источников данных — в [datacube/README.md](datacube/README.md).
