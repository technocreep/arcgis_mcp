/* ── Dashboard Lightbox — injected into all Data Cube dashboard HTML pages ── */
(function () {
    'use strict';

    /* ── Styles ─────────────────────────────────────────────────────────────── */
    const css = `
#lb-overlay {
    display: none;
    position: fixed;
    inset: 0;
    z-index: 9999;
    background: rgba(0, 0, 0, 0.82);
    backdrop-filter: blur(4px);
    align-items: center;
    justify-content: center;
    flex-direction: column;
    gap: 12px;
    animation: lb-fade-in 0.18s ease;
}
#lb-overlay.lb-open { display: flex; }

@keyframes lb-fade-in {
    from { opacity: 0; }
    to   { opacity: 1; }
}

#lb-img {
    max-width: min(92vw, 1400px);
    max-height: 82vh;
    object-fit: contain;
    border-radius: 6px;
    box-shadow: 0 8px 40px rgba(0,0,0,0.6);
    cursor: zoom-out;
    animation: lb-scale-in 0.18s ease;
}

@keyframes lb-scale-in {
    from { transform: scale(0.93); opacity: 0; }
    to   { transform: scale(1);    opacity: 1; }
}

#lb-caption {
    color: rgba(255,255,255,0.72);
    font-size: 12px;
    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
    max-width: min(92vw, 1400px);
    text-align: center;
    line-height: 1.4;
}

#lb-close {
    position: fixed;
    top: 16px;
    right: 20px;
    background: rgba(255,255,255,0.12);
    border: none;
    color: #fff;
    font-size: 22px;
    line-height: 1;
    width: 36px;
    height: 36px;
    border-radius: 50%;
    cursor: pointer;
    display: flex;
    align-items: center;
    justify-content: center;
    transition: background 0.15s;
}
#lb-close:hover { background: rgba(255,255,255,0.22); }

#lb-prev, #lb-next {
    position: fixed;
    top: 50%;
    transform: translateY(-50%);
    background: rgba(255,255,255,0.1);
    border: none;
    color: #fff;
    font-size: 26px;
    width: 44px;
    height: 60px;
    border-radius: 6px;
    cursor: pointer;
    display: flex;
    align-items: center;
    justify-content: center;
    transition: background 0.15s;
    user-select: none;
}
#lb-prev { left: 12px; }
#lb-next { right: 12px; }
#lb-prev:hover, #lb-next:hover { background: rgba(255,255,255,0.2); }
#lb-prev.lb-hidden, #lb-next.lb-hidden { display: none; }

/* Make images in dashboard look clickable */
img[data-lb] {
    cursor: zoom-in !important;
    transition: opacity 0.12s, transform 0.12s !important;
}
img[data-lb]:hover {
    opacity: 0.88 !important;
    transform: scale(1.01) !important;
}
`;

    /* ── Build DOM ──────────────────────────────────────────────────────────── */
    const style = document.createElement('style');
    style.textContent = css;
    document.head.appendChild(style);

    const overlay = document.createElement('div');
    overlay.id = 'lb-overlay';
    overlay.innerHTML = `
        <button id="lb-close" title="Close (Esc)">✕</button>
        <button id="lb-prev" title="Previous">‹</button>
        <img id="lb-img" alt="">
        <div id="lb-caption"></div>
        <button id="lb-next" title="Next">›</button>
    `;
    document.body.appendChild(overlay);

    const lbImg     = document.getElementById('lb-img');
    const lbCaption = document.getElementById('lb-caption');
    const lbClose   = document.getElementById('lb-close');
    const lbPrev    = document.getElementById('lb-prev');
    const lbNext    = document.getElementById('lb-next');

    /* ── State ──────────────────────────────────────────────────────────────── */
    let gallery = [];   // array of img elements in current context
    let current = 0;

    /* ── Helpers ────────────────────────────────────────────────────────────── */
    function open(imgs, idx) {
        gallery = imgs;
        show(idx);
        overlay.classList.add('lb-open');
        document.body.style.overflow = 'hidden';
    }

    function close() {
        overlay.classList.remove('lb-open');
        document.body.style.overflow = '';
        lbImg.src = '';
    }

    function show(idx) {
        current = (idx + gallery.length) % gallery.length;
        const img = gallery[current];
        lbImg.src = img.src;
        lbImg.alt = img.alt || '';
        const caption = img.alt || img.closest('figure')?.querySelector('figcaption')?.textContent?.trim() || '';
        lbCaption.textContent = caption;
        lbPrev.classList.toggle('lb-hidden', gallery.length <= 1);
        lbNext.classList.toggle('lb-hidden', gallery.length <= 1);
    }

    /* ── Events ─────────────────────────────────────────────────────────────── */
    lbClose.addEventListener('click', close);
    lbPrev.addEventListener('click', () => show(current - 1));
    lbNext.addEventListener('click', () => show(current + 1));

    overlay.addEventListener('click', (e) => {
        if (e.target === overlay) close();
    });

    document.addEventListener('keydown', (e) => {
        if (!overlay.classList.contains('lb-open')) return;
        if (e.key === 'Escape') close();
        if (e.key === 'ArrowLeft')  show(current - 1);
        if (e.key === 'ArrowRight') show(current + 1);
    });

    /* ── Attach to images ───────────────────────────────────────────────────── */
    function attachImages() {
        // Collect all content images (skip tiny icons < 48px natural width)
        const all = Array.from(document.querySelectorAll('img')).filter(img => {
            const src = img.getAttribute('src') || '';
            // Skip data URIs that are tiny (icons) and external SVG icons
            if (!src || src.startsWith('data:image/svg')) return false;
            return true;
        });

        all.forEach((img, i) => {
            img.setAttribute('data-lb', '1');
            img.addEventListener('click', () => {
                // Build gallery from sibling images in the nearest gallery/figure container,
                // or fall back to all page images
                const container = img.closest('.gallery, figure, section, .card, article, main');
                const siblings = container
                    ? Array.from(container.querySelectorAll('img[data-lb]'))
                    : all;
                const idxInSiblings = siblings.indexOf(img);
                open(siblings.length > 0 ? siblings : all, idxInSiblings >= 0 ? idxInSiblings : 0);
            });
        });
    }

    // Run after DOM is ready
    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', attachImages);
    } else {
        attachImages();
    }
})();
