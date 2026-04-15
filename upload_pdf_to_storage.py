import json
import os
import datetime
from supabase_client import get_supabase

PDF_PATH = "report.pdf"
PUBLIC_URL_OUTPUT = "public_pdf_url.json"


def main():
    if not os.path.exists(PDF_PATH):
        print(f"[ERROR] PDF file not found: {PDF_PATH}")
        raise SystemExit(1)

    bucket = os.environ.get("SUPABASE_PUBLIC_STORAGE_BUCKET", "reports-public")
    today = datetime.date.today().strftime("%Y-%m-%d")
    timestamp = datetime.datetime.utcnow().strftime("%H%M%S")
    storage_path = f"daily-reports/{today}/report-{timestamp}.pdf"

    sb = get_supabase()

    print(f"[INFO] Uploading PDF to public Supabase bucket={bucket} path={storage_path}")

    try:
        with open(PDF_PATH, "rb") as f:
            sb.storage.from_(bucket).upload(
                path=storage_path,
                file=f,
                file_options={
                    "content-type": "application/pdf",
                    "cache-control": "3600",
                    "upsert": "false",
                },
            )
        print("[INFO] PDF uploaded successfully")
    except Exception as e:
        print(f"[ERROR] Upload failed: {e}")
        raise SystemExit(1)

    supabase_url = os.environ["SUPABASE_URL"].rstrip("/")
    public_url = f"{supabase_url}/storage/v1/object/public/{bucket}/{storage_path}"

    with open(PUBLIC_URL_OUTPUT, "w", encoding="utf-8") as f:
        json.dump({"public_url": public_url}, f, indent=2)

    print(f"[INFO] Public URL written to {PUBLIC_URL_OUTPUT}")
    print(f"[INFO] Public URL: {public_url}")


if __name__ == "__main__":
    main()
