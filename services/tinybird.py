"""
Non-blocking Tinybird event streaming for Fogata.
Sends scored leads to the scored_leads datasource.
If Tinybird is unreachable, Fogata keeps working — errors are logged only.
"""

import json
import os
import threading
from datetime import datetime, timezone

import requests

from utils import log_dev

_DEFAULT_HOST = "https://api.eu-central-1.aws.tinybird.co"


def stream_scored_lead(lead) -> None:
    token = os.getenv("TINYBIRD_APPEND_TOKEN")
    if not token:
        return

    host = os.getenv("TINYBIRD_API_HOST", _DEFAULT_HOST)

    payload = {
        "place_id": lead.place_id or "",
        "name": lead.name or "",
        "category": lead.category or "",
        "city": lead.city or "",
        "country": lead.country or "",
        "domain": lead.domain or "",
        "website": lead.website or "",
        "rating": lead.rating or 0.0,
        "reviews_count": lead.reviews_count or 0,
        "fit_score": lead.fit_score or 0,
        "fit_reasons": json.dumps(lead.fit_reasons or []),
        "source": lead.source or "",
        "source_query": lead.source_query or "",
        "scored_at": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
    }

    def _send():
        try:
            resp = requests.post(
                f"{host}/v0/events?name=scored_leads",
                headers={
                    "Authorization": f"Bearer {token}",
                    "Content-Type": "application/json",
                },
                data=json.dumps(payload),
                timeout=5,
            )
            if resp.status_code != 202:
                log_dev("TINYBIRD", f"Unexpected {resp.status_code}: {resp.text[:200]}", "warning")
        except Exception as exc:
            log_dev("TINYBIRD", f"Stream failed: {exc}", "error")

    threading.Thread(target=_send, daemon=True).start()
