import logging
import os

from db_engine import test_connection
from login_app import app


logger = logging.getLogger("runner")


if __name__ == "__main__":
    # Run DB connectivity check once before starting the web app.
    db_ok = test_connection()
    if not db_ok:
        logger.warning("DB check failed. Starting login app anyway.")

    port = int(os.getenv("PORT", "5000"))
    debug = os.getenv("FLASK_DEBUG", "0").strip().lower() in ("1", "true", "yes", "on")
    app.run(host="0.0.0.0", port=port, debug=debug)
