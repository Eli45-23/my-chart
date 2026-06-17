# Drawing Tools

This adds TradingView-style manual drawing tools to the chart frontend in a safe, read-only way.

## Safety

- Browser-local only
- Saves drawings in `localStorage` by `symbol:timeframe`
- Does not place trades
- Does not call broker APIs
- Does not change backend setup gates

## Included tools

- Select
- Delete
- Trend line
- Ray
- Extended line
- Horizontal line
- Vertical line
- Rectangle / zone box
- Brush / freehand
- Arrow
- Text label
- Price range
- Risk/reward box
- Fibonacci retracement
- Undo
- Lock drawings
- Clear all drawings

## Use

Start the Flask server normally, then open:

```text
http://127.0.0.1:8900/index_stream_draw.html
```

The original chart page is left unchanged. The drawing page loads the existing chart frontend plus `drawing_tools.js`.

## Notes

Drawings are anchored to chart price/time when possible, so lines stay aligned when you scroll or zoom. Brush/freehand drawings are best used for quick markup.
