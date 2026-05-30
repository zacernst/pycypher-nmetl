// FastOpenData — Tweaks panel (vanilla)
// State is persisted via parent postMessage protocol on the LANDING page,
// and via localStorage so other pages stay in sync.

(function () {
  const DEFAULTS = /*EDITMODE-BEGIN*/{
    "fontMode": "sans",
    "accent": "#0E7C3A",
    "paperWarmth": "warm"
  }/*EDITMODE-END*/;

  const STORAGE_KEY = "fod-tweaks";

  function readStored() {
    try { return JSON.parse(localStorage.getItem(STORAGE_KEY) || "{}"); } catch { return {}; }
  }
  function writeStored(state) {
    try { localStorage.setItem(STORAGE_KEY, JSON.stringify(state)); } catch {}
  }

  let state = Object.assign({}, DEFAULTS, readStored());

  function apply() {
    document.body.classList.toggle("mono-body", state.fontMode === "mono");
    document.documentElement.style.setProperty("--accent", state.accent);
    // tweak: deeper hover
    const dark = state.accent === "#0E7C3A" ? "#0B6230"
      : state.accent === "#B25F00" ? "#8E4900"
      : state.accent === "#1E40AF" ? "#172E78"
      : state.accent === "#7F1D1D" ? "#5F1414"
      : state.accent;
    document.documentElement.style.setProperty("--accent-2", dark);

    if (state.paperWarmth === "cool") {
      document.documentElement.style.setProperty("--paper", "#F7F8FA");
      document.documentElement.style.setProperty("--paper-2", "#EFF1F4");
      document.documentElement.style.setProperty("--rule", "#E3E6EB");
      document.documentElement.style.setProperty("--rule-strong", "#C4C9D1");
    } else if (state.paperWarmth === "paper") {
      document.documentElement.style.setProperty("--paper", "#F5F1E8");
      document.documentElement.style.setProperty("--paper-2", "#EEE9DB");
      document.documentElement.style.setProperty("--rule", "#DDD5BF");
      document.documentElement.style.setProperty("--rule-strong", "#BBB39C");
    } else {
      document.documentElement.style.setProperty("--paper", "#F9F8F4");
      document.documentElement.style.setProperty("--paper-2", "#F2F0E9");
      document.documentElement.style.setProperty("--rule", "#E5E2D8");
      document.documentElement.style.setProperty("--rule-strong", "#C9C5B6");
    }
  }

  function persistAndApply(patch) {
    state = Object.assign({}, state, patch);
    writeStored(state);
    apply();
    if (window.parent !== window) {
      try {
        window.parent.postMessage({ type: "__edit_mode_set_keys", edits: patch }, "*");
      } catch {}
    }
  }

  apply();

  // ---- Panel ----
  let panel = null;
  function buildPanel() {
    if (panel) return panel;
    panel = document.createElement("div");
    panel.id = "fod-tweaks";
    panel.innerHTML = `
      <style>
        #fod-tweaks {
          position: fixed;
          right: 20px; bottom: 20px;
          z-index: 999;
          width: 280px;
          background: #FFFFFF;
          border: 1px solid var(--rule, #E5E2D8);
          border-radius: 12px;
          box-shadow: 0 12px 36px rgba(0,0,0,0.12), 0 2px 6px rgba(0,0,0,0.06);
          font-family: var(--font-sans, sans-serif);
          color: var(--ink, #0A0A0A);
          font-size: 13px;
          overflow: hidden;
        }
        #fod-tweaks header {
          display: flex; align-items: center; justify-content: space-between;
          padding: 11px 14px;
          border-bottom: 1px solid var(--rule, #E5E2D8);
          font-family: var(--font-mono, monospace);
          font-size: 11px;
          text-transform: uppercase;
          letter-spacing: 0.1em;
          color: var(--ink-3, #8A8A82);
          cursor: grab;
        }
        #fod-tweaks header button {
          background: transparent; border: 0; cursor: pointer;
          color: var(--ink-3, #8A8A82); font-size: 18px; line-height: 1;
          padding: 0 4px;
        }
        #fod-tweaks header button:hover { color: var(--ink, #0A0A0A); }
        #fod-tweaks .body { padding: 14px; display: flex; flex-direction: column; gap: 16px; }
        #fod-tweaks .group { display: flex; flex-direction: column; gap: 7px; }
        #fod-tweaks label.row {
          display: block;
          font-size: 11px;
          font-family: var(--font-mono, monospace);
          color: var(--ink-3, #8A8A82);
          text-transform: uppercase;
          letter-spacing: 0.08em;
        }
        #fod-tweaks .seg {
          display: flex;
          background: var(--paper-2, #F2F0E9);
          border-radius: 6px;
          padding: 3px;
          gap: 2px;
        }
        #fod-tweaks .seg button {
          flex: 1;
          padding: 7px 8px;
          background: transparent;
          border: 0;
          border-radius: 4px;
          font-family: var(--font-sans, sans-serif);
          font-size: 12px;
          font-weight: 500;
          color: var(--ink-2, #4A4A48);
          cursor: pointer;
          transition: background 120ms;
        }
        #fod-tweaks .seg button.on {
          background: var(--surface, #fff);
          color: var(--ink, #0A0A0A);
          box-shadow: 0 1px 2px rgba(0,0,0,0.08);
        }
        #fod-tweaks .swatches { display: flex; gap: 8px; }
        #fod-tweaks .sw {
          width: 32px; height: 32px;
          border-radius: 6px;
          border: 1px solid var(--rule, #E5E2D8);
          cursor: pointer;
          position: relative;
          transition: transform 120ms;
        }
        #fod-tweaks .sw.on::after {
          content: ''; position: absolute; inset: -3px;
          border-radius: 9px;
          border: 1.5px solid var(--ink, #0A0A0A);
        }
      </style>
      <header data-drag>
        <span>Tweaks</span>
        <button data-close aria-label="Close">×</button>
      </header>
      <div class="body">
        <div class="group">
          <label class="row">Body font</label>
          <div class="seg" data-key="fontMode">
            <button data-val="sans">Sans (Montserrat)</button>
            <button data-val="mono">Mono (JetBrains)</button>
          </div>
        </div>
        <div class="group">
          <label class="row">Accent color</label>
          <div class="swatches" data-key="accent">
            <div class="sw" style="background:#0E7C3A" data-val="#0E7C3A" title="Forest"></div>
            <div class="sw" style="background:#B25F00" data-val="#B25F00" title="Amber"></div>
            <div class="sw" style="background:#1E40AF" data-val="#1E40AF" title="Ink blue"></div>
            <div class="sw" style="background:#7F1D1D" data-val="#7F1D1D" title="Brick"></div>
          </div>
        </div>
        <div class="group">
          <label class="row">Paper</label>
          <div class="seg" data-key="paperWarmth">
            <button data-val="warm">Warm</button>
            <button data-val="paper">Paper</button>
            <button data-val="cool">Cool</button>
          </div>
        </div>
      </div>
    `;
    document.body.appendChild(panel);

    // Bind
    panel.querySelector("[data-close]").addEventListener("click", () => {
      panel.style.display = "none";
      try { window.parent.postMessage({ type: "__edit_mode_dismissed" }, "*"); } catch {}
    });

    panel.querySelectorAll(".seg").forEach(seg => {
      const key = seg.dataset.key;
      seg.querySelectorAll("button").forEach(b => {
        b.addEventListener("click", () => {
          persistAndApply({ [key]: b.dataset.val });
          syncUI();
        });
      });
    });
    panel.querySelectorAll(".swatches").forEach(sw => {
      const key = sw.dataset.key;
      sw.querySelectorAll(".sw").forEach(s => {
        s.addEventListener("click", () => {
          persistAndApply({ [key]: s.dataset.val });
          syncUI();
        });
      });
    });

    // Drag
    let drag = null;
    const handle = panel.querySelector("[data-drag]");
    handle.addEventListener("mousedown", (e) => {
      const r = panel.getBoundingClientRect();
      drag = { dx: e.clientX - r.left, dy: e.clientY - r.top };
      panel.style.right = "auto"; panel.style.bottom = "auto";
      panel.style.left = r.left + "px";
      panel.style.top = r.top + "px";
    });
    window.addEventListener("mousemove", (e) => {
      if (!drag) return;
      panel.style.left = (e.clientX - drag.dx) + "px";
      panel.style.top = (e.clientY - drag.dy) + "px";
    });
    window.addEventListener("mouseup", () => drag = null);

    syncUI();
    return panel;
  }

  function syncUI() {
    if (!panel) return;
    panel.querySelectorAll(".seg").forEach(seg => {
      const k = seg.dataset.key;
      seg.querySelectorAll("button").forEach(b => {
        b.classList.toggle("on", b.dataset.val === state[k]);
      });
    });
    panel.querySelectorAll(".swatches").forEach(sw => {
      const k = sw.dataset.key;
      sw.querySelectorAll(".sw").forEach(s => {
        s.classList.toggle("on", s.dataset.val === state[k]);
      });
    });
  }

  // ---- Edit-mode protocol ----
  window.addEventListener("message", (e) => {
    if (!e.data || typeof e.data !== "object") return;
    if (e.data.type === "__activate_edit_mode") {
      buildPanel();
      panel.style.display = "";
    } else if (e.data.type === "__deactivate_edit_mode") {
      if (panel) panel.style.display = "none";
    }
  });

  // Announce
  try { window.parent.postMessage({ type: "__edit_mode_available" }, "*"); } catch {}
})();
