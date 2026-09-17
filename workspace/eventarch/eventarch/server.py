"""Server entrypoint: python -m eventarch.server"""

from __future__ import annotations

import logging
import signal
import threading
from http.server import ThreadingHTTPServer

from .api import Handler
from .config import Config
from .store import ArchiveStore

log = logging.getLogger("eventarch.server")


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    cfg = Config.from_env()
    store = ArchiveStore(cfg)
    store.open()
    store.start_janitor()

    host, _, port = cfg.addr.rpartition(":")
    httpd = ThreadingHTTPServer((host or "0.0.0.0", int(port)), Handler)
    httpd.store = store  # type: ignore[attr-defined]
    httpd.daemon_threads = True

    def _shutdown(signum, _frame):
        log.info("signal %d received, shutting down", signum)
        threading.Thread(target=httpd.shutdown, daemon=True).start()

    signal.signal(signal.SIGTERM, _shutdown)
    signal.signal(signal.SIGINT, _shutdown)

    log.info("eventarch listening on %s (data_dir=%s, fsync=%s)",
             cfg.addr, cfg.data_dir, cfg.fsync)
    try:
        httpd.serve_forever(poll_interval=0.5)
    finally:
        httpd.server_close()
        store.close()
        log.info("shutdown complete; all acknowledged data is durable")


if __name__ == "__main__":
    main()
