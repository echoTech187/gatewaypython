import pika
import json
import requests
import datetime
import pytz
import os
import threading
import time
from dotenv import load_dotenv

# ──────────────────────────────────────────────────────────────────────────────
# consumer_dlq_monitor.py
#
# Memantau semua Dead-Letter Queue (DLQ) dari consumer RabbitMQ.
# Saat ada pesan gagal masuk ke DLQ, consumer ini akan:
#   1. Membaca payload asli dan detail error (dari header x-error-detail)
#   2. Membuat notifikasi yang informatif
#   3. Mengirimkan notifikasi ke digi-ci3 via HTTP POST (endpoint internal)
#   4. Melakukan basic_ack agar pesan tidak diproses ulang
#
# Pesan di DLQ TIDAK di-retry — hanya dicatat sebagai notifikasi.
# ──────────────────────────────────────────────────────────────────────────────

load_dotenv()

JakartaTz = pytz.timezone("Asia/Jakarta")

# ── Semua DLQ yang dipantau ───────────────────────────────────────────────────
DLQ_NAMES = [
    "queue_notification_ewallet_dlq",
    "queue_notification_qris_mpm_dlq",
    "queue_notification_va_dlq",
    "queue_notification_transfer_dlq",
    "queue_waiting_success_dynamic_ewallet_dlq",
    "queue_waiting_success_dynamic_qris_mpm_dlq",
    "queue_waiting_success_dynamic_va_dlq",
    "queue_waiting_success_transfer_bifast_dlq",
    "queue_pending_dynamic_ewallet_dlq",
    "queue_pending_dynamic_qris_mpm_dlq",
    "queue_pending_dynamic_va_dlq",
    "queue_pending_recurring_va_dlq",
    "queue_pending_recurring_qris_mpm_dlq",
]

# ── Label ramah manusia untuk tiap queue ─────────────────────────────────────
QUEUE_LABELS = {
    "queue_notification_ewallet_dlq":                   "Notifikasi E-Wallet",
    "queue_notification_qris_mpm_dlq":                  "Notifikasi QRIS MPM",
    "queue_notification_va_dlq":                        "Notifikasi Virtual Account",
    "queue_notification_transfer_dlq":                  "Notifikasi Transfer",
    "queue_waiting_success_dynamic_ewallet_dlq":        "Waiting Success Dynamic E-Wallet",
    "queue_waiting_success_dynamic_qris_mpm_dlq":       "Waiting Success Dynamic QRIS",
    "queue_waiting_success_dynamic_va_dlq":             "Waiting Success Dynamic VA",
    "queue_waiting_success_transfer_bifast_dlq":        "Waiting Success Transfer BI-Fast",
    "queue_pending_dynamic_ewallet_dlq":                "Pending Dynamic E-Wallet",
    "queue_pending_dynamic_qris_mpm_dlq":               "Pending Dynamic QRIS",
    "queue_pending_dynamic_va_dlq":                     "Pending Dynamic VA",
    "queue_pending_recurring_va_dlq":                   "Pending Recurring VA",
    "queue_pending_recurring_qris_mpm_dlq":             "Pending Recurring QRIS",
}


class DLQMonitorThread(threading.Thread):
    """
    Satu thread per DLQ.
    Mengkonsumsi pesan gagal dan mengirim notifikasi ke digi-ci3.
    """

    def __init__(self, thread_id, queue_name, host_name, host_port,
                 pika_user, pika_pass, digi_ci3_url):
        threading.Thread.__init__(self)
        self.daemon = True   # mati otomatis saat main thread mati
        self.thread_id    = thread_id
        self.queue_name   = queue_name
        self.host_name    = host_name
        self.host_port    = int(host_port)
        self.pika_user    = pika_user
        self.pika_pass    = pika_pass
        self.digi_ci3_url = digi_ci3_url  # URL endpoint notifikasi digi-ci3
        self.queue_label  = QUEUE_LABELS.get(queue_name, queue_name)

    def run(self):
        while True:
            try:
                self._connect_and_consume()
            except Exception as e:
                ts = datetime.datetime.now(JakartaTz).strftime("%Y-%m-%d %H:%M:%S")
                print(f"[{ts}] [DLQ-{self.queue_name}] Connection lost: {e}. Reconnecting in 10s...", flush=True)
                time.sleep(10)

    def _connect_and_consume(self):
        ts = datetime.datetime.now(JakartaTz).strftime("%Y-%m-%d %H:%M:%S")
        credentials  = pika.PlainCredentials(self.pika_user, self.pika_pass)
        connection   = pika.BlockingConnection(
            pika.ConnectionParameters(
                host=self.host_name,
                port=self.host_port,
                virtual_host='/',
                credentials=credentials,
                heartbeat=600,
                blocked_connection_timeout=300,
            )
        )
        channel = connection.channel()

        # Pastikan DLQ sudah dideklarasikan (durable, tanpa DLX lagi)
        channel.queue_declare(queue=self.queue_name, durable=True, passive=True)

        print(f"[{ts}] [DLQ-Monitor] Listening on: {self.queue_name}", flush=True)

        def callback(ch, method, properties, body):
            now = datetime.datetime.now(JakartaTz).strftime("%Y-%m-%d %H:%M:%S")

            # ── Baca payload asli ──────────────────────────────────────────
            try:
                data = json.loads(body)
            except Exception:
                data = {}

            msg_type = data.get("msgType", "unknown")
            msg_info = data.get("msgInfo", {})

            # ── Baca error detail dari header ──────────────────────────────
            error_detail = "Tidak ada detail error."
            if properties and properties.headers:
                error_detail = properties.headers.get("x-error-detail", error_detail)
                if isinstance(error_detail, bytes):
                    error_detail = error_detail.decode("utf-8", errors="replace")

            # ── Bangun pesan notifikasi ────────────────────────────────────
            title = f"Transaksi Gagal: {self.queue_label}"
            message = (
                f"Queue: {self.queue_name} | "
                f"MsgType: {msg_type} | "
                f"Error: {str(error_detail)[:300]}"
            )

            ref_data = {
                "queue_name":   self.queue_name,
                "queue_label":  self.queue_label,
                "msg_type":     msg_type,
                "error_detail": str(error_detail)[:500],
                "failed_at":    now,
            }

            # Sertakan info transaksi jika ada
            if isinstance(msg_info, dict):
                for key in ["transactionId", "merchantId", "ref_merchantId", "amount"]:
                    if key in msg_info:
                        ref_data[key] = msg_info[key]

            print(f"[{now}] [DLQ-Monitor] Failed msg on {self.queue_name}: {error_detail[:100]}", flush=True)

            # ── Kirim notifikasi ke digi-ci3 ───────────────────────────────
            sent = self._send_notification(title, message, ref_data)
            if sent:
                ch.basic_ack(delivery_tag=method.delivery_tag)
                print(f"[{now}] [DLQ-Monitor] Notification sent & ACK'd for {self.queue_name}", flush=True)
            else:
                # Jika gagal kirim notifikasi, NACK tanpa requeue
                # (agar tidak loop, tapi pesan tetap di DLQ)
                ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
                print(f"[{now}] [DLQ-Monitor] Failed to send notification for {self.queue_name} — NACK (no requeue)", flush=True)

        channel.basic_qos(prefetch_count=1)
        channel.basic_consume(queue=self.queue_name, on_message_callback=callback)
        channel.start_consuming()

    def _send_notification(self, title, message, ref_data, retries=3):
        """
        POST notifikasi ke endpoint digi-ci3.
        Retry sampai 3x dengan backoff 2 detik.
        """
        payload = json.dumps({
            "type":     "dlq_failed",
            "title":    title,
            "message":  message,
            "ref_data": ref_data,
        })
        headers = {
            "Content-Type":    "application/json",
            "X-DLQ-Monitor":   "1",          # marker agar mudah diidentifikasi di log
        }

        for attempt in range(1, retries + 1):
            try:
                response = requests.post(
                    self.digi_ci3_url,
                    headers=headers,
                    data=payload,
                    timeout=10,
                )
                if response.status_code == 200:
                    try:
                        resp_json = response.json()
                        # gatewayinternal menggunakan format {"responseCode": "SUCCESS"}
                        if resp_json.get("responseCode") == "SUCCESS":
                            return True
                    except Exception:
                        pass
                ts = datetime.datetime.now(JakartaTz).strftime("%Y-%m-%d %H:%M:%S")
                print(f"[{ts}] [DLQ-Monitor] Attempt {attempt}: HTTP {response.status_code} — {response.text[:100]}", flush=True)
            except Exception as e:
                ts = datetime.datetime.now(JakartaTz).strftime("%Y-%m-%d %H:%M:%S")
                print(f"[{ts}] [DLQ-Monitor] Attempt {attempt} exception: {e}", flush=True)

            if attempt < retries:
                time.sleep(2 ** attempt)  # backoff: 2, 4 detik

        return False


# ── Main ─────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    host_name        = os.environ.get("host_name", "localhost")
    host_port        = os.environ.get("host_port", "5672")
    pika_user        = os.environ.get("pika_user", "guest")
    pika_pass        = os.environ.get("pika_pass", "guest")
    internal_url_hit = os.environ.get("internal_url_hit", "http://127.0.0.1/gatewayinternal/")

    # URL endpoint gatewayinternal untuk menerima notifikasi dari DLQ monitor
    # Format response: {"responseCode": "SUCCESS", ...} — konsisten dengan internal lainnya
    digi_ci3_url = internal_url_hit.rstrip('/') + "/Notification/PushDlqAlert"

    ts = datetime.datetime.now(JakartaTz).strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] [DLQ-Monitor] Starting — monitoring {len(DLQ_NAMES)} DLQs", flush=True)
    print(f"[{ts}] [DLQ-Monitor] Notify URL: {digi_ci3_url}", flush=True)

    threads = []
    for i, q_name in enumerate(DLQ_NAMES):
        t = DLQMonitorThread(
            thread_id   = i + 1,
            queue_name  = q_name,
            host_name   = host_name,
            host_port   = host_port,
            pika_user   = pika_user,
            pika_pass   = pika_pass,
            digi_ci3_url= digi_ci3_url,
        )
        t.start()
        threads.append(t)
        time.sleep(0.2)  # jeda kecil antar koneksi

    # Keep main thread hidup
    try:
        while True:
            time.sleep(60)
    except KeyboardInterrupt:
        ts = datetime.datetime.now(JakartaTz).strftime("%Y-%m-%d %H:%M:%S")
        print(f"[{ts}] [DLQ-Monitor] Shutting down...", flush=True)
