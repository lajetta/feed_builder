Feed Builder — minimal web UI

This is a tiny Flask-based UI to run the existing `feed_builder_generator.py` interactively.

Setup
1. Create a virtualenv: python -m venv .venv
2. Activate it: .venv\Scripts\Activate.ps1
3. Install deps: pip install -r requirements.txt

Run
python feed_builder_ui.py
Open http://127.0.0.1:5000 in your browser.

Notes
- The UI writes uploaded files to `uploads/` and uses `DEFAULTS` from `feed_builder_generator.py` for default values.
- This is a development helper only; do not expose to untrusted networks without adding auth.
