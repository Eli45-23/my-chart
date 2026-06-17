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
  const LINE_TYPES = new Set(["trend", "ray", "extended", "arrow"]);
  const svgNs = "http://www.w3.org/2000/svg";

  let activeTool = "select";
  let drawings = [];
  let selectedId = null;
  let draft = null;
  let brushDraft = null;
  let locked = true;
  let lastScope = "";
  let drag = null;

  const $ = (id) => document.getElementById(id);
  const runtime = () => window.ChartRuntime || {};
  const chartObj = () => runtime().chart || null;
  const series = () => runtime().candleSeries || null;
  const chartDiv = () => runtime().chartEl || $("chart");
  const scope = () => `${runtime().activeSymbol || "AAPL"}:${runtime().activeTimeframe || "1Min"}`;
  const key = () => `${STORAGE_PREFIX}:${scope()}`;
  const uid = (prefix) => `${prefix}-${Date.now()}-${Math.random().toString(16).slice(2)}`;
  const money = (n) => Number.isFinite(Number(n)) ? `$${Number(n).toFixed(2)}` : "n/a";

  function esc(value) {
    return String(value ?? "").replace(/[&<>\"']/g, ch => ({ "&":"&amp;", "<":"&lt;", ">":"&gt;", '"':"&quot;", "'":"&#039;" }[ch]));
  }

  function isTypingTarget(target) {
    const tag = target?.tagName?.toLowerCase();
    return tag === "input" || tag === "textarea" || tag === "select" || target?.isContentEditable;
  }

  function save() {
    try { localStorage.setItem(key(), JSON.stringify(drawings.slice(-300))); } catch (_) {}
  }

  function load() {
    try {
      const parsed = JSON.parse(localStorage.getItem(key()) || "[]");
      drawings = Array.isArray(parsed) ? parsed : [];
    } catch (_) {
      drawings = [];
    }
    selectedId = null;
    draft = null;
    brushDraft = null;
    drag = null;
    render();
  }

  function pointFromEvent(event) {
    const el = chartDiv();
    if (!el || !series() || !chartObj()) return null;
    const rect = el.getBoundingClientRect();
    const x = event.clientX - rect.left;
    const y = event.clientY - rect.top;
    return screenToPoint(x, y);
  }

  function screenToPoint(x, y, fallbackTime = null) {
    const s = series();
    const c = chartObj();
    if (!s || !c) return null;
    return {
      x,
      y,
      price: s.coordinateToPrice(y),
      time: c.timeScale().coordinateToTime(x) ?? fallbackTime,
    };
  }

  function toScreen(point) {
    if (!point || !series() || !chartObj()) return null;
    const y = Number.isFinite(Number(point.price)) ? series().priceToCoordinate(Number(point.price)) : point.y;
    const x = point.time !== null && point.time !== undefined ? chartObj().timeScale().timeToCoordinate(point.time) : point.x;
    if (!Number.isFinite(x) || !Number.isFinite(y)) return null;
    return { x, y };
  }

  function clonePoint(point) {
    return { ...point };
  }

  function cloneDrawing(drawing) {
    return { ...drawing, points: (drawing.points || []).map(clonePoint) };
  }

  function el(name, attrs = {}) {
    const node = document.createElementNS(svgNs, name);
    Object.entries(attrs).forEach(([k, v]) => v !== null && v !== undefined && node.setAttribute(k, String(v)));
    return node;
  }

  function setLocked(value) {
    locked = Boolean(value);
    $("drawingSvgOverlay")?.classList.toggle("locked", locked);
    $("drawingLock")?.classList.toggle("active", locked);
    if ($("drawingLock")) $("drawingLock").textContent = locked ? "Locked" : "Lock";
    if ($("drawingHelp")) {
      $("drawingHelp").textContent = locked
        ? "Drawings locked. Chart pan/scroll is normal."
        : activeTool === "select"
          ? "Click a drawing to select it. Drag body or handles to edit."
          : activeTool === "delete"
            ? "Click a drawing to delete it."
            : "Click/drag on the chart to place the selected drawing.";
    }
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
      .drawing-help{color:#728198;font-size:10px;margin-left:auto}.drawing-svg-overlay{position:absolute;inset:0;z-index:4;width:100%;height:100%;pointer-events:auto;overflow:hidden;touch-action:none}.drawing-svg-overlay.locked{pointer-events:none}.drawing-hit{stroke:transparent;stroke-width:14;fill:transparent;cursor:grab}.drawing-handle{fill:#081018;stroke:#dbe7f2;stroke-width:1.5;cursor:grab}.drawing-handle:hover{fill:#dbe7f2;stroke:#081018}.drawing-shape{vector-effect:non-scaling-stroke}.drawing-selected{filter:drop-shadow(0 0 4px rgba(215,226,238,.75))}.drawing-label{font-size:11px;font-weight:800;fill:#dbe7f2;paint-order:stroke;stroke:rgba(7,11,17,.88);stroke-width:3px;cursor:grab}
    `;
    document.head.appendChild(style);

    const toolbar = document.createElement("div");
    toolbar.className = "drawing-toolbar";
    toolbar.innerHTML = `<span class="drawing-toolbar-label">Drawing Tools</span>${TOOLS.map(([tool,label]) => `<button class="drawing-tool-btn ${tool === "select" ? "active" : ""}" data-tool="${tool}" title="${label}" type="button">${label}</button>`).join("")}<div class="drawing-settings"><input id="drawingColor" type="color" value="${DEFAULT_COLOR}" title="Drawing color"><select id="drawingWidth" title="Line width"><option value="1">1px</option><option value="2" selected>2px</option><option value="3">3px</option><option value="4">4px</option></select></div><button class="drawing-action-btn" id="drawingUndo" type="button">Undo</button><button class="drawing-action-btn active" id="drawingLock" type="button">Locked</button><button class="drawing-action-btn danger" id="drawingClear" type="button">Clear All</button><span class="drawing-help" id="drawingHelp">Drawings locked. Chart pan/scroll is normal.</span>`;
    const toggleRow = document.querySelector(".toggle-row");
    const shell = document.querySelector(".chart-shell");
    if (toggleRow) toggleRow.insertAdjacentElement("afterend", toolbar);
    else shell?.insertAdjacentElement("beforebegin", toolbar);

    const svg = el("svg", { id: "drawingSvgOverlay", class: "drawing-svg-overlay locked", "aria-label": "Manual chart drawing tools" });
    shell?.appendChild(svg);

    toolbar.addEventListener("click", toolbarClick);
    $("drawingUndo")?.addEventListener("click", () => undoLast());
    $("drawingClear")?.addEventListener("click", () => {
      if (drawings.length && confirm(`Clear all drawings for ${scope()}?`)) {
        drawings = [];
        selectedId = null;
        save();
        render();
      }
    });
    $("drawingLock")?.addEventListener("click", () => setLocked(!locked));
    $("drawingColor")?.addEventListener("input", updateSelectedStyle);
    $("drawingWidth")?.addEventListener("change", updateSelectedStyle);
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
    if (activeTool !== "select") selectedId = null;
    setLocked(false);
    render();
  }

  function options() {
    return { color: $("drawingColor")?.value || DEFAULT_COLOR, width: Number($("drawingWidth")?.value || 2) };
  }

  function updateSelectedStyle() {
    const d = drawings.find(x => x.id === selectedId);
    if (!d) return;
    Object.assign(d, options(), { updatedAt: new Date().toISOString() });
    save();
    render();
  }

  function setActiveTool(tool) {
    activeTool = tool;
    document.querySelectorAll("[data-tool]").forEach(b => b.classList.toggle("active", b.dataset.tool === tool));
    setLocked(false);
    render();
  }

  function pointerDown(event) {
    if (locked) return;
    const svg = $("drawingSvgOverlay");
    const target = event.target.closest?.("[data-drawing-id]");
    const handle = event.target.closest?.("[data-handle]");
    const point = pointFromEvent(event);
    if (!point) return;

    if (target) {
      event.preventDefault();
      event.stopPropagation();
      const did = target.dataset.drawingId;
      if (activeTool === "delete") {
        drawings = drawings.filter(d => d.id !== did);
        selectedId = null;
        save();
        render();
        return;
      }
      selectedId = did;
      activeTool = activeTool === "delete" ? "select" : activeTool;
      document.querySelectorAll("[data-tool]").forEach(b => b.classList.toggle("active", b.dataset.tool === activeTool));
      const drawing = drawings.find(d => d.id === did);
      drag = {
        id: did,
        handle: handle?.dataset.handle || "body",
        start: point,
        original: drawing ? cloneDrawing(drawing) : null,
      };
      svg?.setPointerCapture?.(event.pointerId);
      render();
      return;
    }

    if (activeTool === "select" || activeTool === "delete") {
      selectedId = null;
      render();
      return;
    }

    event.preventDefault();
    event.stopPropagation();
    svg?.setPointerCapture?.(event.pointerId);
    if (activeTool === "brush") {
      brushDraft = { id: uid("brush"), type: "brush", points: [point], createdAt: new Date().toISOString(), ...options() };
      render();
      return;
    }
    if (ONE_POINT.has(activeTool)) {
      const d = { id: uid(activeTool), type: activeTool, points: [point], text: activeTool === "text" ? "Text" : "", createdAt: new Date().toISOString(), ...options() };
      drawings.push(d);
      selectedId = d.id;
      save();
      render();
      return;
    }
    if (TWO_POINT.has(activeTool)) {
      draft = { id: uid(activeTool), type: activeTool, points: [point, point], createdAt: new Date().toISOString(), ...options() };
      render();
    }
  }

  function pointerMove(event) {
    if (locked) return;
    const point = pointFromEvent(event);
    if (!point) return;
    if (drag) {
      event.preventDefault();
      applyDrag(point);
      render();
      return;
    }
    if (draft) {
      event.preventDefault();
      draft.points[1] = point;
      render();
    }
    if (brushDraft) {
      event.preventDefault();
      brushDraft.points.push(point);
      render();
    }
  }

  function finish(event) {
    if (draft) {
      drawings.push(draft);
      selectedId = draft.id;
      draft = null;
      save();
    }
    if (brushDraft) {
      drawings.push(brushDraft);
      selectedId = brushDraft.id;
      brushDraft = null;
      save();
    }
    if (drag) {
      const d = drawings.find(x => x.id === drag.id);
      if (d) d.updatedAt = new Date().toISOString();
      drag = null;
      save();
    }
    try { $("drawingSvgOverlay")?.releasePointerCapture?.(event?.pointerId); } catch (_) {}
    render();
  }

  function applyDrag(point) {
    const d = drawings.find(x => x.id === drag.id);
    if (!d || !drag.original) return;
    const originalPoints = drag.original.points || [];
    if (drag.handle && drag.handle !== "body") {
      if (drag.handle.startsWith("corner:")) {
        const [, xIndexValue, yIndexValue] = drag.handle.split(":");
        const xIndex = Number(xIndexValue);
        const yIndex = Number(yIndexValue);
        d.points = originalPoints.map(clonePoint);
        if (Number.isInteger(xIndex) && d.points[xIndex]) {
          d.points[xIndex].time = point.time;
          d.points[xIndex].x = point.x;
        }
        if (Number.isInteger(yIndex) && d.points[yIndex]) {
          d.points[yIndex].price = point.price;
          d.points[yIndex].y = point.y;
        }
        return;
      }
      const index = Number(drag.handle.replace("p", ""));
      if (Number.isInteger(index) && originalPoints[index]) d.points[index] = point;
      return;
    }
    d.points = originalPoints.map(originalPoint => {
      const startScreen = toScreen(drag.start);
      const originalScreen = toScreen(originalPoint);
      if (!startScreen || !originalScreen) return clonePoint(originalPoint);
      const movedX = originalScreen.x + (point.x - startScreen.x);
      const movedY = originalScreen.y + (point.y - startScreen.y);
      return screenToPoint(movedX, movedY, originalPoint.time) || clonePoint(originalPoint);
    });
  }

  function doubleClick(event) {
    const target = event.target.closest?.("[data-drawing-id]");
    const d = drawings.find(x => x.id === target?.dataset.drawingId);
    if (!d || d.type !== "text") return;
    const value = prompt("Edit chart text", d.text || "Text");
    if (value !== null) {
      d.text = value.trim() || "Text";
      d.updatedAt = new Date().toISOString();
      save();
      render();
    }
  }

  function keydown(event) {
    if (isTypingTarget(event.target)) return;
    if (event.key === "Escape") {
      draft = null;
      brushDraft = null;
      drag = null;
      selectedId = null;
      setActiveTool("select");
      return;
    }
    if (event.key.toLowerCase() === "v" && !event.metaKey && !event.ctrlKey && !event.altKey) {
      setActiveTool("select");
      return;
    }
    if ((event.key === "Delete" || event.key === "Backspace") && selectedId) {
      event.preventDefault();
      drawings = drawings.filter(d => d.id !== selectedId);
      selectedId = null;
      save();
      render();
      return;
    }
    if ((event.metaKey || event.ctrlKey) && event.key.toLowerCase() === "z") {
      event.preventDefault();
      undoLast();
    }
  }

  function undoLast() {
    if (selectedId) {
      drawings = drawings.filter(d => d.id !== selectedId);
      selectedId = null;
    } else {
      drawings.pop();
    }
    save();
    render();
  }

  function render() {
    const svg = $("drawingSvgOverlay"), c = chartDiv();
    if (!svg || !c) return;
    const w = c.clientWidth || 1, h = c.clientHeight || 1;
    svg.setAttribute("viewBox", `0 0 ${w} ${h}`);
    svg.innerHTML = `<defs><marker id="drawingArrowHead" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="6" markerHeight="6" orient="auto-start-reverse"><path d="M 0 0 L 10 5 L 0 10 z" fill="context-stroke"></path></marker></defs>`;
    const ordered = drawings.filter(d => d.id !== selectedId);
    const selected = drawings.find(d => d.id === selectedId);
    [...ordered, selected, draft, brushDraft].filter(Boolean).forEach(d => {
      const g = renderDrawing(d, w, h);
      if (g) svg.appendChild(g);
    });
  }

  function renderDrawing(d, w, h) {
    const rawPoints = d.points || [];
    const points = rawPoints.map(toScreen);
    if (!points.some(Boolean)) return null;
    const color = d.color || DEFAULT_COLOR, sw = d.width || 2;
    const selected = d.id === selectedId;
    const g = el("g", { "data-drawing-id": d.id, class: selected ? "drawing-selected" : "" });
    const hit = (attrs) => g.appendChild(el("line", { ...attrs, class: "drawing-hit", "data-drawing-id": d.id }));
    const line = (a,b,extra={}) => {
      g.appendChild(el("line", { x1:a.x, y1:a.y, x2:b.x, y2:b.y, stroke:color, "stroke-width":sw, fill:"none", class:"drawing-shape", ...extra }));
      hit({ x1:a.x, y1:a.y, x2:b.x, y2:b.y });
    };
    const label = (x,y,text,anchor="end") => {
      const t = el("text", { x, y, class:"drawing-label", "text-anchor":anchor, "data-drawing-id":d.id });
      t.textContent = text;
      g.appendChild(t);
    };

    if (d.type === "horizontal" && points[0]) {
      line({x:0,y:points[0].y}, {x:w,y:points[0].y}, {"stroke-dasharray":"6 4"});
      label(w-76, points[0].y-7, money(rawPoints[0].price));
      if (selected) addHandle(g, w - 18, points[0].y, "p0", d.id);
    } else if (d.type === "vertical" && points[0]) {
      line({x:points[0].x,y:0}, {x:points[0].x,y:h}, {"stroke-dasharray":"6 4"});
      if (selected) addHandle(g, points[0].x, 22, "p0", d.id);
    } else if (LINE_TYPES.has(d.type) && points[0] && points[1]) {
      let a = points[0], b = points[1];
      if (d.type === "ray" || d.type === "extended") ({a,b} = extend(a,b,w,h,d.type === "extended"));
      line(a,b,d.type === "arrow" ? {"marker-end":"url(#drawingArrowHead)"} : {});
      if (selected) addPointHandles(g, points, d.id, 2);
    } else if (d.type === "rectangle" && points[0] && points[1]) {
      const r = rect(points[0], points[1]);
      g.appendChild(el("rect", {...r, fill:color, opacity:.12, stroke:color, "stroke-width":sw}));
      g.appendChild(el("rect", {...r, class:"drawing-hit", "data-drawing-id":d.id}));
      if (selected) addBoxHandles(g, points, d.id);
    } else if (d.type === "brush" && points.filter(Boolean).length > 1) {
      const valid = points.filter(Boolean);
      const path = valid.map((p,i) => `${i ? "L" : "M"}${p.x},${p.y}`).join(" ");
      g.appendChild(el("path", { d:path, stroke:color, "stroke-width":sw, fill:"none", "stroke-linecap":"round", "stroke-linejoin":"round" }));
      g.appendChild(el("path", { d:path, class:"drawing-hit", "data-drawing-id":d.id }));
    } else if (d.type === "text" && points[0]) {
      label(points[0].x, points[0].y, d.text || "Text", "start");
      if (selected) addHandle(g, points[0].x, points[0].y, "p0", d.id);
    } else if (d.type === "priceRange" && points[0] && points[1]) {
      renderRange(g,d,points,color,sw,w,label);
      if (selected) addBoxHandles(g, points, d.id);
    } else if (d.type === "riskReward" && points[0] && points[1]) {
      renderRR(g,d,points,sw,label);
      if (selected) addBoxHandles(g, points, d.id);
    } else if (d.type === "fib" && points[0] && points[1]) {
      renderFib(g,points,color,sw,w,label);
      if (selected) addPointHandles(g, points, d.id, 2);
    }
    return g;
  }

  function addHandle(group, x, y, handle, drawingId) {
    group.appendChild(el("circle", {
      cx: x, cy: y, r: 5, class: "drawing-handle", "data-drawing-id": drawingId, "data-handle": handle,
    }));
  }

  function addPointHandles(group, points, drawingId, count) {
    points.slice(0, count).forEach((point, index) => {
      if (point) addHandle(group, point.x, point.y, `p${index}`, drawingId);
    });
  }

  function addBoxHandles(group, points, drawingId) {
    const [a, b] = points;
    if (!a || !b) return;
    const leftIndex = a.x <= b.x ? 0 : 1;
    const rightIndex = leftIndex === 0 ? 1 : 0;
    const topIndex = a.y <= b.y ? 0 : 1;
    const bottomIndex = topIndex === 0 ? 1 : 0;
    const left = Math.min(a.x, b.x);
    const right = Math.max(a.x, b.x);
    const top = Math.min(a.y, b.y);
    const bottom = Math.max(a.y, b.y);
    addHandle(group, left, top, `corner:${leftIndex}:${topIndex}`, drawingId);
    addHandle(group, right, top, `corner:${rightIndex}:${topIndex}`, drawingId);
    addHandle(group, left, bottom, `corner:${leftIndex}:${bottomIndex}`, drawingId);
    addHandle(group, right, bottom, `corner:${rightIndex}:${bottomIndex}`, drawingId);
  }

  function rect(a,b) {
    return { x:Math.min(a.x,b.x), y:Math.min(a.y,b.y), width:Math.abs(b.x-a.x), height:Math.abs(b.y-a.y) };
  }

  function extend(a,b,w,h,both) {
    const dx=b.x-a.x, dy=b.y-a.y, len=Math.sqrt(dx*dx+dy*dy)||1, scale=Math.max(w,h)*3, ux=dx/len, uy=dy/len;
    return { a: both ? {x:a.x-ux*scale,y:a.y-uy*scale} : a, b:{x:b.x+ux*scale,y:b.y+uy*scale} };
  }

  function renderRange(g,d,p,color,sw,w,label) {
    const r = rect(p[0],p[1]);
    const a=Number(d.points[0].price), b=Number(d.points[1].price), diff=Math.abs(a-b), pct=Math.min(a,b)>0?diff/Math.min(a,b)*100:0;
    g.appendChild(el("rect", {...r, fill:color, opacity:.10, stroke:color, "stroke-width":sw}));
    g.appendChild(el("rect", {...r, class:"drawing-hit", "data-drawing-id":d.id}));
    label(r.x+r.width+8, r.y+r.height/2, `${money(diff)} · ${pct.toFixed(2)}%`, "start");
  }

  function renderRR(g,d,p,sw,label) {
    const entry=Number(d.points[0].price), target=Number(d.points[1].price), risk=Math.abs(target-entry)/2 || .01;
    const stop=target>entry ? entry-risk : entry+risk;
    const stopY=series().priceToCoordinate(stop);
    const left=Math.min(p[0].x,p[1].x), right=Math.max(p[0].x,p[1].x)||left+80;
    const rewardRect = rect({x:left,y:p[0].y},{x:right,y:p[1].y});
    const riskRect = rect({x:left,y:p[0].y},{x:right,y:stopY});
    g.appendChild(el("rect", {...rewardRect, fill:"#65b9a6", opacity:.13, stroke:"#65b9a6", "stroke-width":sw}));
    g.appendChild(el("rect", {...riskRect, fill:"#d77979", opacity:.13, stroke:"#d77979", "stroke-width":sw}));
    g.appendChild(el("rect", {...rewardRect, class:"drawing-hit", "data-drawing-id":d.id}));
    g.appendChild(el("rect", {...riskRect, class:"drawing-hit", "data-drawing-id":d.id}));
    label(right+8,p[0].y-6,`Entry ${money(entry)} · R:R 2.00`,"start");
    label(right+8,p[1].y-6,`TP ${money(target)}`,"start");
    label(right+8,stopY-6,`SL ${money(stop)}`,"start");
  }

  function renderFib(g,p,color,sw,w,label) {
    [0,.236,.382,.5,.618,.786,1].forEach(level => {
      const y=p[0].y+(p[1].y-p[0].y)*level;
      g.appendChild(el("line", {x1:Math.min(p[0].x,p[1].x), y1:y, x2:w, y2:y, stroke:color, "stroke-width":level===.5?sw:1, "stroke-dasharray":level===0||level===1?"":"4 4"}));
      hitFibLine(g, Math.min(p[0].x,p[1].x), y, w, p[0].x <= p[1].x ? p[0].x : p[1].x);
      label(w-8,y-4,`${(level*100).toFixed(level?1:0)}%`);
    });
  }

  function hitFibLine(group, x1, y, x2) {
    group.appendChild(el("line", { x1, y1:y, x2, y2:y, class:"drawing-hit", "data-drawing-id":group.dataset.drawingId }));
  }

  function boot() {
    addStyleAndToolbar();
    lastScope = scope();
    load();
    window.addEventListener("resize", () => requestAnimationFrame(render));
    window.addEventListener("chart-runtime-redraw", () => requestAnimationFrame(render));
    try { chartObj()?.timeScale()?.subscribeVisibleLogicalRangeChange(() => requestAnimationFrame(render)); } catch (_) {}
    setInterval(() => {
      if (scope() !== lastScope) {
        lastScope = scope();
        load();
      } else {
        render();
      }
    }, 700);
  }

  if (document.readyState === "loading") document.addEventListener("DOMContentLoaded", boot); else boot();
})();
