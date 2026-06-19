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
http://127.0.0.1:8900/
```

The normal chart page loads the existing chart frontend plus `drawing_tools.js`.
`docs/reference/index_stream_draw.html` remains as a backup/reference page and is not served by the app.

## Editing

- Drawings start locked so normal chart pan/scroll behavior is preserved.
- Pick a drawing tool to unlock drawing interaction.
- Pick **Select** or press `V` to select and edit an existing drawing.
- Drag the drawing body to move it.
- Drag visible handles to edit line endpoints, box corners, text anchors, or range anchors.
- Double-click text labels to edit their text.
- Press `Delete`/`Backspace` to remove the selected drawing.
- Press `Esc` to cancel a draft or deselect.
- Press `Cmd+Z`/`Ctrl+Z` to undo the latest drawing action.

## Notes

Drawings are anchored to chart price/time when possible, so lines stay aligned when you scroll or zoom. Brush/freehand drawings are best used for quick markup.
