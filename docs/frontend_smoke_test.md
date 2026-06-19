# Frontend Smoke Test Checklist

Run this checklist against the local app at `http://127.0.0.1:8900/` after frontend or chart changes.

## Chart

- [ ] `/` loads without a visible error.
- [ ] AAPL loads as the default symbol.
- [ ] The symbol input loads another valid stock or ETF symbol.
- [ ] `1m`, `5m`, and `15m` buttons load their respective views.
- [ ] The chart pans and zooms while drawings are locked.
- [ ] Clean Mode toggles without breaking chart layers.
- [ ] Line Audit opens and closes.
- [ ] Candle Compare opens and closes.
- [ ] Paper Trade Planner opens and closes.

## Drawing Tools

- [ ] Drawing toolbar appears.
- [ ] Lock/unlock works.
- [ ] A trend line can be drawn.
- [ ] A selected drawing shows handles.
- [ ] A selected drawing can be moved.
- [ ] Box corners can be resized.
- [ ] Text can be edited by double-clicking it.
- [ ] Delete or Backspace removes the selected drawing.
- [ ] `V` selects the pointer/select tool.
- [ ] `Esc` cancels or deselects.
- [ ] Cmd+Z or Ctrl+Z undoes the latest drawing change.

## Dashboard And Browser

- [ ] `/performance` loads.
- [ ] Browser console has no errors.

## Notes

This is a manual UI check. It does not authorize orders, broker connections, or automated actions.
