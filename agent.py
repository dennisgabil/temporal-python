import asyncio
import time
import smtplib
from email.mime.text import MIMEText
from temporalio.client import Client
 
# ----------------------------
# Config
# ----------------------------
TEMPORAL_ADDRESS = "localhost:7233"
NAMESPACE = "default"
CHECK_INTERVAL_SECONDS = 30   # ✅ Reduced for real-time
 
ALERT_TO = "rinkisweta@gmail.com"
 
SMTP_HOST = "smtp.gmail.com"
SMTP_PORT = 587
SMTP_SENDER = "rinkisweta@gmail.com"
SMTP_PASSWORD = "ummygaxdjtrfxlzy"
 
# ✅ Track already-alerted failures
alerted_failures = set()
 
# ----------------------------
# Collect workflows
# ----------------------------
async def collect_status():
    client = await Client.connect(TEMPORAL_ADDRESS, namespace=NAMESPACE)
 
    rows = []
    async for we in client.list_workflows(page_size=200):
        rows.append({
            "workflow_id": we.id,
            "run_id": we.run_id,
            "workflow_type": getattr(we.workflow_type, "name", None),
            "status": we.status.name if we.status else None,
            "start_time": str(we.start_time),
        })
 
    return rows
 
 
# ----------------------------
# Failure filter
# ----------------------------
def get_failures(rows):
    return [
        r for r in rows
        if r["status"] and (
            "FAIL" in r["status"].upper() or
            "TERMIN" in r["status"].upper()
        )
    ]
 
 
# ----------------------------
# Build failure email ONLY
# ----------------------------
def build_failure_email(failures):
    subject = f"[ALERT] {len(failures)} Workflow(s) FAILED"
 
    lines = []
    for w in failures:
        lines.append(
            f"- ID={w['workflow_id']} | Status={w['status']} | Start={w['start_time']}"
        )
 
    body = "\n".join(lines)
    return subject, body
 
 
# ----------------------------
# Email sender
# ----------------------------
def send_email_smtp(subject, body):
    msg = MIMEText(body, "plain", "utf-8")
    msg["Subject"] = subject
    msg["From"] = SMTP_SENDER
    msg["To"] = ALERT_TO
 
    with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
        server.starttls()
        server.login(SMTP_SENDER, SMTP_PASSWORD)
        server.sendmail(SMTP_SENDER, [ALERT_TO], msg.as_string())
 
 
# ----------------------------
# Main loop
# ----------------------------
def main():
    print("Agent starting...")
    print("Temporal:", TEMPORAL_ADDRESS, "namespace:", NAMESPACE)
 
    while True:
        try:
            rows = asyncio.run(collect_status())
 
            # ✅ Print ALL workflows (for debugging / UI sync)
            print("\n=== WORKFLOW STATUS ===")
            for r in rows:
                print(r["workflow_id"], "→", r["status"])
 
            failures = get_failures(rows)
 
            # ✅ Only send email for NEW failures
            new_failures = []
            for f in failures:
                if f["workflow_id"] not in alerted_failures:
                    new_failures.append(f)
                    alerted_failures.add(f["workflow_id"])
 
            if new_failures:
                subject, body = build_failure_email(new_failures)
 
                print("\n🚨 NEW FAILURES DETECTED")
                print(body)
 
                send_email_smtp(subject, body)
                print("✅ Email sent:", subject)
 
            else:
                print("✔ No new failures")
 
        except Exception as e:
            print("❌ Error:", repr(e))
 
        time.sleep(CHECK_INTERVAL_SECONDS)
 
 
# ----------------------------
if __name__ == "__main__":
    main()