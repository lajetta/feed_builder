from pathlib import Path
import os
import uuid
from flask import Flask, request, render_template, redirect, url_for, send_from_directory, flash
import feed_builder_generator as fbg

app = Flask(__name__)
app.secret_key = os.environ.get("FLASK_SECRET", str(uuid.uuid4()))
BASE = Path(__file__).resolve().parent
UPLOAD_DIR = BASE / "uploads"
UPLOAD_DIR.mkdir(exist_ok=True)

@app.route("/", methods=["GET", "POST"])
def index():
    defaults = fbg.DEFAULTS.copy()
    if request.method == "POST":
        # Collect form values (fall back to defaults)
        template = request.form.get("template") or defaults.get("template")
        feed_id = request.form.get("feed_id") or defaults.get("feed_id")
        feed_name = request.form.get("feed_name") or defaults.get("feed_name")
        provider_id = request.form.get("provider_id") or defaults.get("provider_id")
        import_frequency_id = request.form.get("import_frequency_id") or defaults.get("import_frequency_id")
        schema = request.form.get("schema") or defaults.get("schema")
        out_dir = request.form.get("out_dir") or defaults.get("out_dir")
        mssql_conn_str = request.form.get("mssql_conn_str") or defaults.get("mssql_conn_str")

        # Handle file uploads (optional)
        vendor_file_path = None
        mapping_path = None

        vendor_f = request.files.get("vendor_file")
        if vendor_f and vendor_f.filename:
            dest = UPLOAD_DIR / f"vendor_{uuid.uuid4().hex}_{Path(vendor_f.filename).name}"
            vendor_f.save(dest)
            vendor_file_path = str(dest)
        else:
            vendor_file_path = request.form.get("vendor_file_path") or defaults.get("vendor_file")

        mapping_f = request.files.get("mapping")
        if mapping_f and mapping_f.filename:
            dest = UPLOAD_DIR / f"mapping_{uuid.uuid4().hex}_{Path(mapping_f.filename).name}"
            mapping_f.save(dest)
            mapping_path = str(dest)
        else:
            mapping_path = request.form.get("mapping_path") or defaults.get("mapping")

        # Call build function
        try:
            out_path = fbg.build_feed_json_paths(
                template_path=template,
                vendor_file=vendor_file_path,
                feed_id=feed_id,
                feed_name=feed_name,
                provider_id=provider_id,
                import_frequency_id=import_frequency_id,
                mapping_path=(mapping_path if mapping_path and str(mapping_path).strip() else None),
                out_dir=out_dir,
                schema=schema,
                custom_table_name=request.form.get("custom_table_name") or "CustomTable",
                mssql_conn_str=mssql_conn_str,
            )
            flash(f"Feed built: {out_path}", "success")
            return redirect(url_for("result", filename=Path(out_path).name))
        except Exception as e:
            flash(f"Error: {e}", "danger")

    return render_template("index.html", defaults=defaults)


@app.route("/result/<path:filename>")
def result(filename):
    # show link to download file
    return render_template("result.html", filename=filename)


@app.route("/download/<path:filename>")
def download(filename):
    # Use defaults from the imported feed_builder_generator module (fbg)
    out_dir = Path(fbg.DEFAULTS.get("out_dir") or ".").resolve()
    return send_from_directory(directory=str(out_dir), filename=filename, as_attachment=True)


if __name__ == "__main__":
    # Start dev server
    app.run(host="127.0.0.1", port=5000, debug=True)
