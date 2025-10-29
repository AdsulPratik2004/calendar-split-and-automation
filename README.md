# Calendar splitter service

This small FastAPI service accepts a POST from the frontend and splits a calendar row's `content_items` array into separate rows (one row per post) inside the `calendar_data` table in Supabase.

Files added
- `main.py` — FastAPI service with `/split-calendar` POST endpoint.
- `requirements.txt` — Python dependencies.

Environment

Create a `.env` or set environment variables in your system:

- `SUPABASE_URL` — your Supabase URL
- `SUPABASE_KEY` — your Supabase anon/service key

Install and run

1. Create a venv and install dependencies:

```powershell
python -m venv .venv; .\.venv\Scripts\Activate; pip install -r requirements.txt
```

2. Run the server:

```powershell
uvicorn main:app --reload --host 0.0.0.0 --port 8000
```

Frontend example (fetch POST)

```js
// payload can include id OR user_id/month/year/platform filters
const payload = { id: '7577a560-6d0a-4f00-87cf-d943306f3d45' };

fetch('/split-calendar', {
  method: 'POST',
  headers: { 'Content-Type': 'application/json' },
  body: JSON.stringify(payload),
})
  .then(r => r.json())
  .then(console.log)
  .catch(console.error);
```

Notes

- This implementation inserts the per-post rows back into `calendar_data` using a new `id` for each post and places the single item under the `calendar_data` column as `{ metadata: ..., content_item: ... }`.
- If you prefer a dedicated `calendar_posts` table, let me know and I can update the script and provide the SQL to create it.
