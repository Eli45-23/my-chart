/* TradingView-style drawing tools for Eli's chart. Browser-local and read-only. */
(() => {
  const STORAGE_PREFIX = "myChartDrawings";
  const DEFAULT_COLOR = "#d7e2ee";
  const TOOLS = [
    ["select", "Select"], ["trend", "Trend"], ["ray", "Ray"], ["extended", "Extend"],
    ["horizontal", "H-Line"], ["vertical", "V-Line"], ["rectangle", "Box"], ["brush", "Brush"],
    ["arrow", "Arrow"], ["text", "Text"], ["priceRange", "Range"], ["riskReward", "R:R"],
    ["fib", "Fib"], ["delete", "Delete"],
  ];
  const TWO_POINT = new Set(["trend", "ray", "extended", "rectangle", "arrow", "priceRange", "riskReward", "fib"]);
  const ONE_POINT = new Set(["horizontal", "vertical", "text"]);
  let activeTool = "select";
  let drawings = [];
  let selectedId = null;
  let draft = null;
  let brushDraft = null;
  let locked = false;
  let lastScope = "";

  const $ = (id) => document.getElementById(id);
  const svgNs = "http://www.w3.org/2000/svg";
  const chartObj = () => (typeof chart !== "undefined" ? chart : null);
  const series = () => (typeof candleSeries !== "undefined" ? candleSeries : null);
  const chartDiv = () => $("chart");
  const scope = () => `${typeof activeSymbol !== "undefined" ? activeSymbol : "AAPL"}:${typeof activeTimeframe !== "undefined" ? activeTimeframe : "1Min"}`;
  const key = () => `${STORAGE_PREFIX}:${scope()}`;
  const id = (prefix) => `${prefix}-${Date.now()}-${Math.random().toString(16).slice(2)}`;
  const money = (n) => Number.isFinite(Number(n)) ? `$${Number(n).toFixed(2)}` : "n/a";

  function esc(value) {
    return String(value ?? "").replace(/[&<>\"']/g, ch => ({ "&":"&amp;", "<":"&lt;", ">":"&gt;", '"':"&quot;", "'":"&#039;" }[ch]));
  }

  function save() {
    try { localStorage.setItem(key(), JSON.stringify(drawings.slice(-300))); } catch (_) {}
  }

  function load() {
    try {
      const parsed = JSON.parse(localStorage.getItem(key()) || "[]");
      drawings = Array.isArray(parsed) ? parsed : [];
    } catch (_) { drawings = []; }
    selectedId = null;
    render();
  }

  function pointFromEvent(event) {
    const el = chartDiv();
    if (!el || !series() || !chartObj()) return null;
    const rect = el.getBoundingClientRect();
    const x = event.clientX - rect.left;
    const y = event.clientY - rect.top;
    return { x, y, price: series().coordinateToPrice(y), time: chartObj().timeScale().coordinateToTime(x) };
  }

  function toScreen(point) {
    if (!point || !series() || !chartObj()) return null;
    const y = Number.isFinite(Number(point.price)) ? series().priceToCoordinate(Number(point.price)) : point.y;
    const x = point.time !== null && point.time !== undefined ? chartObj().timeScale().timeToCoordinate(point.time) : point.x;
    if (!Number.isFinite(x) || !Number.isFinite(y)) return null;
    return { x, y };
  }

  function el(name, attrs = {}) {
    const node = document.createElementNS(svgNs, name);
    Object.entries(attrs).forEach(([k, v]) => v !== null && v !== undefined && node.setAttribute(k, String(v)));
    return node;
  }

  function addStyleAndToolbar() {
    if ($("drawingSvgOverlay")) return;
    const style = document.createElement("style");
    style.textContent = `
      .drawing-toolbar{display:flex;align-items:center;flex-wrap:wrap;gap:4px;padding:6px 14px;border-bottom:1px solid var(--border,#202c3b);background:#081018}
      .drawing-toolbar-label{color:#78879a;font-size:9px;font-weight:800;letter-spacing:.08em;text-transform:uppercase;margin-right:4px}
      .drawing-tool-btn,.drawing-action-btn{background:#121a25;color:#aeb9c9;border:1px solid #263448;border-radius:6px;padding:4px 7px;cursor:pointer;font-size:10px;font-weight:700}
      .drawing-tool-btn.active,.drawing-action-btn.active{background:#172638;border-color:#6f8eaf;color:#e1edf8}.drawing-action-btn.danger{border-color:rgba(215,121,121,.42);color:#dda0a0;background:rgba(82,38,42,.22)}
      .drawing-settings{display:flex;gap:5px;align-items:center;margin-left:6px}.drawing-settings input[type=color]{width:25px;height:23px;padding:0;border:1px solid #263448;border-radius:5px;background:#101823}.drawing-settings select{height:24px;border:1px solid #263448;border-radius:5px;background:#101823;color:#cbd9e8;font-size:10px}
      .drawing-help{color:#728198;font-size:10px;margin-left:auto}.drawing-svg-overlay{position:absolute;inset:0;z-index:4;width:100%;height:100%;pointer-events:auto;overflow:hidden}.drawing-svg-overlay.locked{pointer-events:none}.drawing-hit{stroke:transparent;stroke-width:14;fill:transparent;cursor:pointer}.drawing-shape{vector-effect:non-scaling-stroke}.drawing-selected{filter:drop-shadow(0 0 4px rgba(215,226,238,.75))}.drawing-label{font-size:11px;font-weight:800;fill:#dbe7f2;paint-order:stroke;stroke:rgba(7,11,17,.88);stroke-width:3px}
    `;
    document.head.appendChild(style);

    const toolbar = document.createElement("div");
    toolbar.className = "drawing-toolbar";
    toolbar.innerHTML = `<span class="drawing-toolbar-label">Drawing Tools</span>${TOOLS.map(([tool,label]) => `<button class="drawing-tool-btn ${tool === "select" ? "active" : ""}" data-tool="${tool}" title="${label}" type="button">${label}</button>`).join("")}<div class="drawing-settings"><input id="drawingColor" type="color" value="${DEFAULT_COLOR}" title="Drawing color"><select id="drawingWidth" title="Line width"><option value="1">1px</option><option value="2" selected>2px</option><option value="3">3px</option><option value="4">4px</option></select></div><button class="drawing-action-btn" id="drawingUndo" type="button">Undo</button><button class="drawing-action-btn" id="drawingLock" type="button">Lock</button><button class="drawing-action-btn danger" id="drawingClear" type="button">Clear All</button><span class="drawing-help" id="drawingHelp">Select a tool, then click/drag on the chart.</span>`;
    const toggleRow = document.querySelector(".toggle-row");
    const shell = document.querySelector(".chart-shell");
    if (toggleRow) toggleRow.insertAdjacentElement("afterend", toolbar);
    else shell?.insertAdjacentElement("beforebegin", toolbar);

    const svg = el("svg", { id: "drawingSvgOverlay", class: "drawing-svg-overlay", "aria-label": "Manual chart drawing tools" });
    shell?.appendChild(svg);

    toolbar.addEventListener("click", toolbarClick);
    $("drawingUndo")?.addEventListener("click", () => { drawings.pop(); selectedId = null; save(); render(); });
    $("drawingClear")?.addEventListener("click", () => { if (drawings.length && confirm(`Clear all drawings for ${scope()}?`)) { drawings = []; selectedId = null; save(); render(); } });
    $("drawingLock")?.addEventListener("click", () => { locked = !locked; $("drawingSvgOverlay")?.classList.toggle("locked", locked); $("drawingLock").classList.toggle("active", locked); $("drawingLock").textContent = locked ? "Locked" : "Lock"; });
    svg.addEventListener("pointerdown", pointerDown);
    svg.addEventListener("pointermove", pointerMove);
    svg.addEventListener("pointerup", finish);
    svg.addEventListener("pointercancel", finish);
    svg.addEventListener("dblclick", doubleClick);
    window.addEventListener("keydown", keydown);
  }

  function toolbarClick(event) {
    const btn = event.target.closest("[data-tool]");
    if (!btn) return;
    activeTool = btn.dataset.tool;
    document.querySelectorAll("[data-tool]").forEach(b => b.classList.toggle("active", b === btn));
    $("drawingHelp").textContent = activeTool === "select" ? "Click a drawing to select it. Press Delete to remove it." : activeTool === "delete" ? "Click a drawing to delete it." : "Click/drag on the chart to place the selected drawing.";
  }

  function options() { return { color: $("drawingColor")?.value || DEFAULT_COLOR, width: Number($("drawingWidth")?.value || 2) }; }

  function pointerDown(event) {
    if (locked) return;
    const target = event.target.closest?.("[data-drawing-id]");
    if (target) {
      const did = target.dataset.drawingId;
      if (activeTool === "delete") { drawings = drawings.filter(d => d.id !== did); selectedId = null; save(); render(); return; }
      selectedId = did; render(); return;
    }
    const point = pointFromEvent(event);
    if (!point || activeTool === "select" || activeTool === "delete") return;
    $("drawingSvgOverlay")?.setPointerCapture?.(event.pointerId);
    if (activeTool === "brush") { brushDraft = { id: id("brush"), type: "brush", points: [point], ...options() }; render(); return; }
    if (ONE_POINT.has(activeTool)) { const d = { id: id(activeTool), type: activeTool, points: [point], text: activeTool === "text" ? "Text" : "", ...options() }; drawings.push(d); selectedId = d.id; save(); render(); return; }
    if (TWO_POINT.has(activeTool)) { draft = { id: id(activeTool), type: activeTool, points: [point, point], ...options() }; render(); }
  }

  function pointerMove(event) {
    const point = pointFromEvent(event);
    if (!point) return;
    if (draft) { draft.points[1] = point; render(); }
    if (brushDraft) { brushDraft.points.push(point); render(); }
  }

  function finish() {
    if (draft) { drawings.push(draft); selectedId = draft.id; draft = null; save(); }
    if (brushDraft) { drawings.push(brushDraft); selectedId = brushDraft.id; brushDraft = null; save(); }
    render();
  }

  function doubleClick(event) {
    const target = event.target.closest?.("[data-drawing-id]");
    const d = drawings.find(x => x.id === target?.dataset.drawingId);
    if (!d || d.type !== "text") return;
    const value = prompt("Edit chart text", d.text || "Text");
    if (value !== null) { d.text = value.trim() || "Text"; save(); render(); }
  }

  function keydown(event) {
    if ((event.key === "Delete" || event.key === "Backspace") && selectedId) { drawings = drawings.filter(d => d.id !== selectedId); selectedId = null; save(); render(); }
    if ((event.metaKey || event.ctrlKey) && event.key.toLowerCase() === "z") { drawings.pop(); selectedId = null; save(); render(); }
  }

  function render() {
    const svg = $("drawingSvgOverlay"), c = chartDiv();
    if (!svg || !c) return;
    const w = c.clientWidth || 1, h = c.clientHeight || 1;
    svg.setAttribute("viewBox", `0 0 ${w} ${h}`);
    svg.innerHTML = `<defs><marker id="drawingArrowHead" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="6" markerHeight="6" orient="auto-start-reverse"><path d="M 0 0 L 10 5 L 0 10 z" fill="context-stroke"></path></marker></defs>`;
    [...drawings, draft, brushDraft].filter(Boolean).forEach(d => { const g = renderDrawing(d, w, h); if (g) svg.appendChild(g); });
  }

  function renderDrawing(d, w, h) {
    const points = (d.points || []).map(toScreen).filter(Boolean);
    if (!points.length) return null;
    const color = d.color || DEFAULT_COLOR, sw = d.width || 2;
    const g = el("g", { "data-drawing-id": d.id, class: d.id === selectedId ? "drawing-selected" : "" });
    const hit = (attrs) => g.appendChild(el("line", { ...attrs, class: "drawing-hit", "data-drawing-id": d.id }));
    const line = (a,b,extra={}) => { g.appendChild(el("line", { x1:a.x, y1:a.y, x2:b.x, y2:b.y, stroke:color, "stroke-width":sw, fill:"none", class:"drawing-shape", ...extra })); hit({ x1:a.x, y1:a.y, x2:b.x, y2:b.y }); };
    const label = (x,y,text,anchor="end") => { const t = el("text", { x, y, class:"drawing-label", "text-anchor":anchor, "data-drawing-id":d.id }); t.textContent = text; g.appendChild(t); };
    if (d.type === "horizontal") { line({x:0,y:points[0].y}, {x:w,y:points[0].y}, {"stroke-dasharray":"6 4"}); label(w-76, points[0].y-7, money(d.points[0].price)); }
    else if (d.type === "vertical") line({x:points[0].x,y:0}, {x:points[0].x,y:h}, {"stroke-dasharray":"6 4"});
    else if (["trend","ray","extended","arrow"].includes(d.type) && points[1]) { let [a,b] = points; if (d.type === "ray" || d.type === "extended") { ({a,b} = extend(a,b,w,h,d.type === "extended")); } line(a,b,d.type === "arrow" ? {"marker-end":"url(#drawingArrowHead)"} : {}); }
    else if (d.type === "rectangle" && points[1]) { const r = rect(points[0], points[1]); g.appendChild(el("rect", {...r, fill:color, opacity:.12, stroke:color, "stroke-width":sw})); g.appendChild(el("rect", {...r, class:"drawing-hit", "data-drawing-id":d.id})); }
    else if (d.type === "brush" && points.length > 1) { const path = points.map((p,i) => `${i ? "L" : "M"}${p.x},${p.y}`).join(" "); g.appendChild(el("path", { d:path, stroke:color, "stroke-width":sw, fill:"none", "stroke-linecap":"round", "stroke-linejoin":"round" })); g.appendChild(el("path", { d:path, class:"drawing-hit", "data-drawing-id":d.id })); }
    else if (d.type === "text") label(points[0].x, points[0].y, d.text || "Text", "start");
    else if (d.type === "priceRange" && points[1]) renderRange(g,d,points,color,sw,w,label);
    else if (d.type === "riskReward" && points[1]) renderRR(g,d,points,sw,label);
    else if (d.type === "fib" && points[1]) renderFib(g,points,color,sw,w,label);
    return g;
  }

  function rect(a,b) { return { x:Math.min(a.x,b.x), y:Math.min(a.y,b.y), width:Math.abs(b.x-a.x), height:Math.abs(b.y-a.y) }; }
  function extend(a,b,w,h,both) { const dx=b.x-a.x, dy=b.y-a.y, len=Math.sqrt(dx*dx+dy*dy)||1, scale=Math.max(w,h)*3, ux=dx/len, uy=dy/len; return { a: both ? {x:a.x-ux*scale,y:a.y-uy*scale} : a, b:{x:b.x+ux*scale,y:b.y+uy*scale} }; }
  function renderRange(g,d,p,color,sw,w,label) { const r = rect(p[0],p[1]); const a=Number(d.points[0].price), b=Number(d.points[1].price), diff=Math.abs(a-b), pct=Math.min(a,b)>0?diff/Math.min(a,b)*100:0; g.appendChild(el("rect", {...r, fill:color, opacity:.10, stroke:color, "stroke-width":sw})); label(r.x+r.width+8, r.y+r.height/2, `${money(diff)} · ${pct.toFixed(2)}%`, "start"); }
  function renderRR(g,d,p,sw,label) { const entry=Number(d.points[0].price), target=Number(d.points[1].price), risk=Math.abs(target-entry)/2 || .01, stop=target>entry ? entry-risk : entry+risk, stopY=series().priceToCoordinate(stop), left=Math.min(p[0].x,p[1].x), right=Math.max(p[0].x,p[1].x)||left+80; g.appendChild(el("rect", {...rect({x:left,y:p[0].y},{x:right,y:p[1].y}), fill:"#65b9a6", opacity:.13, stroke:"#65b9a6", "stroke-width":sw})); g.appendChild(el("rect", {...rect({x:left,y:p[0].y},{x:right,y:stopY}), fill:"#d77979", opacity:.13, stroke:"#d77979", "stroke-width":sw})); label(right+8,p[0].y-6,`Entry ${money(entry)} · R:R 2.00`,"start"); label(right+8,p[1].y-6,`TP ${money(target)}`,"start"); label(right+8,stopY-6,`SL ${money(stop)}`,"start"); }
  function renderFib(g,p,color,sw,w,label) { [0,.236,.382,.5,.618,.786,1].forEach(level => { const y=p[0].y+(p[1].y-p[0].y)*level; g.appendChild(el("line", {x1:Math.min(p[0].x,p[1].x), y1:y, x2:w, y2:y, stroke:color, "stroke-width":level===.5?sw:1, "stroke-dasharray":level===0||level===1?"":"4 4"})); label(w-8,y-4,`${(level*100).toFixed(level?1:0)}%`); }); }

  function boot() {
    addStyleAndToolbar();
    lastScope = scope();
    load();
    window.addEventListener("resize", () => requestAnimationFrame(render));
    try { chartObj()?.timeScale()?.subscribeVisibleLogicalRangeChange(() => requestAnimationFrame(render)); } catch (_) {}
    setInterval(() => { if (scope() !== lastScope) { lastScope = scope(); load(); } else render(); }, 700);
  }

  if (document.readyState === "loading") document.addEventListener("DOMContentLoaded", boot); else boot();
})();
