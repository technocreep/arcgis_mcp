from __future__ import annotations

from pathlib import Path

from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import FileResponse, HTMLResponse

from _backend.auth_utils.auth_utils import verify_user
from _backend.projects_utils.load_projects import PROJECTS_DIR


router = APIRouter(prefix="/projects", tags=["datacube"])


_DASHBOARD_OVERRIDE_CSS = r'''
:root {
    --brand: #059669;
    --brand-light: #d1fae5;
    --text: #1e293b;
    --muted: #64748b;
    --border: #e2e8f0;
    --bg: #f8fafc;
    --surface: #ffffff;
    --radius: 10px;
    --shadow: 0 1px 4px rgba(0,0,0,0.07);
}
*, *::before, *::after { box-sizing: border-box; }
body {
    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif !important;
    background: #f1f5f9 !important;
    color: var(--text) !important;
    font-size: 14px !important;
    margin: 0 !important;
}
body > header, body > nav, header, nav {
    background: var(--surface) !important;
    border-bottom: 1px solid var(--border) !important;
    padding: 10px 20px !important;
    font-size: 13px !important;
    box-shadow: var(--shadow) !important;
}
nav a, header a {
    color: var(--muted) !important;
    text-decoration: none !important;
    padding: 3px 8px;
    border-radius: 5px;
    transition: background 0.15s, color 0.15s;
    font-weight: 500 !important;
}
nav a:hover, header a:hover {
    background: var(--bg) !important;
    color: var(--brand) !important;
}
body > main, main {
    max-width: 1280px !important;
    margin: 0 auto !important;
    padding: 20px !important;
}
.card, section, article {
    background: var(--surface) !important;
    border: 1px solid var(--border) !important;
    border-radius: var(--radius) !important;
    box-shadow: var(--shadow) !important;
    padding: 18px !important;
    margin-bottom: 18px !important;
}
section > h2, section > h3, .card > h2, .card > h3 {
    font-size: 13px !important;
    font-weight: 700 !important;
    color: var(--muted) !important;
    text-transform: uppercase !important;
    letter-spacing: 0.06em !important;
    margin: 0 0 12px !important;
    padding-bottom: 8px !important;
    border-bottom: 1px solid var(--border) !important;
}
h1 { font-size: 16px !important; font-weight: 700 !important; color: var(--text) !important; }
h2 { font-size: 14px !important; font-weight: 600 !important; }
h3 { font-size: 13px !important; font-weight: 600 !important; }
table, .table { width: 100% !important; border-collapse: collapse !important; font-size: 12px !important; }
th {
    background: var(--bg) !important;
    color: var(--muted) !important;
    font-weight: 600 !important;
    text-transform: uppercase !important;
    letter-spacing: 0.04em !important;
    font-size: 10px !important;
    padding: 8px 10px !important;
    border-bottom: 2px solid var(--border) !important;
    text-align: left !important;
}
td {
    padding: 6px 10px !important;
    border-bottom: 1px solid var(--border) !important;
    color: var(--text) !important;
    vertical-align: top !important;
}
tr:last-child td { border-bottom: none !important; }
tr:hover td { background: #f8fafc !important; }
.img-card, figure {
    background: var(--bg) !important;
    border: 1px solid var(--border) !important;
    border-radius: 8px !important;
    padding: 10px !important;
    text-align: center !important;
}
figcaption { font-size: 11px !important; color: var(--muted) !important; margin-top: 6px !important; }
img { max-width: 100% !important; height: auto !important; border-radius: 6px !important; }
.gallery { display: grid !important; grid-template-columns: repeat(auto-fill, minmax(260px, 1fr)) !important; gap: 14px !important; }
.map-frame, iframe { width: 100% !important; min-height: 560px !important; border: 1px solid var(--border) !important; border-radius: 8px !important; }
pre, code {
    font-family: 'SF Mono', 'Fira Code', ui-monospace, monospace !important;
    font-size: 12px !important;
    background: var(--bg) !important;
    border: 1px solid var(--border) !important;
    border-radius: 6px !important;
    padding: 10px 14px !important;
    overflow-x: auto !important;
    color: #334155 !important;
}
.warn {
    background: #fefce8 !important;
    border: 1px solid #fde047 !important;
    border-left: 3px solid #eab308 !important;
    border-radius: 6px !important;
    padding: 10px 14px !important;
    font-size: 12px !important;
    color: #713f12 !important;
}
.split { display: grid !important; grid-template-columns: 1fr 1fr !important; gap: 14px !important; }
@media (max-width: 700px) { .split { grid-template-columns: 1fr !important; } }
a { color: var(--brand) !important; text-decoration: none !important; }
a:hover { text-decoration: underline !important; }
select {
    border: 1px solid var(--border) !important;
    border-radius: 6px !important;
    padding: 5px 10px !important;
    font-size: 12px !important;
    background: var(--surface) !important;
    color: var(--text) !important;
    outline: none !important;
}
'''


_LIGHTBOX_JS = r'''
(function () {
    'use strict';
    const css = `
#lb-overlay { display:none; position:fixed; inset:0; z-index:9999; background:rgba(0,0,0,.82); backdrop-filter:blur(4px); align-items:center; justify-content:center; flex-direction:column; gap:12px; animation:lb-fade-in .18s ease; }
#lb-overlay.lb-open { display:flex; }
@keyframes lb-fade-in { from { opacity:0; } to { opacity:1; } }
#lb-img { max-width:min(92vw,1400px); max-height:82vh; object-fit:contain; border-radius:6px; box-shadow:0 8px 40px rgba(0,0,0,.6); cursor:zoom-out; animation:lb-scale-in .18s ease; }
@keyframes lb-scale-in { from { transform:scale(.93); opacity:0; } to { transform:scale(1); opacity:1; } }
#lb-caption { color:rgba(255,255,255,.72); font-size:12px; font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif; max-width:min(92vw,1400px); text-align:center; line-height:1.4; }
#lb-close { position:fixed; top:16px; right:20px; background:rgba(255,255,255,.12); border:none; color:#fff; font-size:22px; line-height:1; width:36px; height:36px; border-radius:50%; cursor:pointer; display:flex; align-items:center; justify-content:center; transition:background .15s; }
#lb-close:hover { background:rgba(255,255,255,.22); }
#lb-prev,#lb-next { position:fixed; top:50%; transform:translateY(-50%); background:rgba(255,255,255,.1); border:none; color:#fff; font-size:26px; width:44px; height:60px; border-radius:6px; cursor:pointer; display:flex; align-items:center; justify-content:center; transition:background .15s; user-select:none; }
#lb-prev { left:12px; } #lb-next { right:12px; }
#lb-prev:hover,#lb-next:hover { background:rgba(255,255,255,.2); }
#lb-prev.lb-hidden,#lb-next.lb-hidden { display:none; }
img[data-lb] { cursor:zoom-in !important; transition:opacity .12s, transform .12s !important; }
img[data-lb]:hover { opacity:.88 !important; transform:scale(1.01) !important; }
`;
    const style = document.createElement('style'); style.textContent = css; document.head.appendChild(style);
    const overlay = document.createElement('div');
    overlay.id = 'lb-overlay';
    overlay.innerHTML = '<button id="lb-close" title="Close (Esc)">✕</button><button id="lb-prev" title="Previous">‹</button><img id="lb-img" alt=""><div id="lb-caption"></div><button id="lb-next" title="Next">›</button>';
    document.body.appendChild(overlay);
    const lbImg = document.getElementById('lb-img');
    const lbCaption = document.getElementById('lb-caption');
    const lbClose = document.getElementById('lb-close');
    const lbPrev = document.getElementById('lb-prev');
    const lbNext = document.getElementById('lb-next');
    let gallery = []; let current = 0;
    function open(imgs, idx) { gallery = imgs; show(idx); overlay.classList.add('lb-open'); document.body.style.overflow = 'hidden'; }
    function close() { overlay.classList.remove('lb-open'); document.body.style.overflow = ''; lbImg.src = ''; }
    function show(idx) {
        current = (idx + gallery.length) % gallery.length;
        const img = gallery[current];
        lbImg.src = img.src; lbImg.alt = img.alt || '';
        const caption = img.alt || img.closest('figure')?.querySelector('figcaption')?.textContent?.trim() || '';
        lbCaption.textContent = caption;
        lbPrev.classList.toggle('lb-hidden', gallery.length <= 1);
        lbNext.classList.toggle('lb-hidden', gallery.length <= 1);
    }
    lbClose.addEventListener('click', close);
    lbPrev.addEventListener('click', function () { show(current - 1); });
    lbNext.addEventListener('click', function () { show(current + 1); });
    overlay.addEventListener('click', function (e) { if (e.target === overlay) close(); });
    document.addEventListener('keydown', function (e) {
        if (!overlay.classList.contains('lb-open')) return;
        if (e.key === 'Escape') close();
        if (e.key === 'ArrowLeft') show(current - 1);
        if (e.key === 'ArrowRight') show(current + 1);
    });
    function attachImages() {
        const all = Array.from(document.querySelectorAll('img')).filter(function (img) {
            const src = img.getAttribute('src') || '';
            if (!src || src.startsWith('data:image/svg')) return false;
            return true;
        });
        all.forEach(function (img) {
            img.setAttribute('data-lb', '1');
            img.addEventListener('click', function () {
                const container = img.closest('.gallery, figure, section, .card, article, main');
                const siblings = container ? Array.from(container.querySelectorAll('img[data-lb]')) : all;
                const idxInSiblings = siblings.indexOf(img);
                open(siblings.length > 0 ? siblings : all, idxInSiblings >= 0 ? idxInSiblings : 0);
            });
        });
    }
    if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', attachImages); else attachImages();
})();
'''


def _find_dc_dir(project_path: Path) -> Path | None:
    candidates = [
        project_path / "datacube",
        project_path / "data" / "datacube",
    ]
    for candidate in candidates:
        if candidate.exists() and candidate.is_dir():
            return candidate
    return None


def _inject_dashboard_enhancements(html: str) -> str:
    style_tag = f"<style>{_DASHBOARD_OVERRIDE_CSS}</style>\n"
    script_tag = f"<script>{_LIGHTBOX_JS}</script>\n"
    if "</head>" in html:
        html = html.replace("</head>", style_tag + "</head>", 1)
    else:
        html = style_tag + html
    if "</body>" in html:
        html = html.replace("</body>", script_tag + "</body>", 1)
    else:
        html += script_tag
    return html


@router.get("/{project_id}/datacube/files")
async def list_datacube_files(project_id: str, user=Depends(verify_user)):
    project_path = Path(PROJECTS_DIR) / project_id
    if not project_path.exists():
        raise HTTPException(status_code=404, detail="Project not found")

    dc_dir = _find_dc_dir(project_path)
    if dc_dir is None:
        return {"exists": False, "files": []}

    files = [item.relative_to(dc_dir).as_posix() for item in dc_dir.rglob("*") if item.is_file()]
    return {"exists": True, "files": sorted(files)}


@router.get("/{project_id}/datacube")
async def list_datacube_files_legacy(project_id: str, user=Depends(verify_user)):
    return await list_datacube_files(project_id, user)


@router.get("/{project_id}/datacube/files/{file_path:path}")
async def serve_datacube_file(project_id: str, file_path: str, user=Depends(verify_user)):
    project_path = Path(PROJECTS_DIR) / project_id
    if not project_path.exists():
        raise HTTPException(status_code=404, detail="Project not found")

    dc_dir = _find_dc_dir(project_path)
    if dc_dir is None:
        raise HTTPException(status_code=404, detail="No Data Cube artifacts found")

    try:
        target = (dc_dir / file_path).resolve()
        if not str(target).startswith(str(dc_dir.resolve())):
            raise HTTPException(status_code=403, detail="Forbidden")
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=400, detail="Invalid path") from exc

    if not target.is_file():
        raise HTTPException(status_code=404, detail="File not found")

    if target.suffix.lower() == ".html":
        html = target.read_text(encoding="utf-8", errors="replace")
        return HTMLResponse(_inject_dashboard_enhancements(html))

    return FileResponse(str(target))
