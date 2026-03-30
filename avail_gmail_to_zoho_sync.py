#!/usr/bin/env python3
"""
Sync recent Avail lead emails from Gmail into a Zoho Campaigns mailing list.

What it does
------------
1. Authenticates to Gmail with OAuth (desktop app flow).
2. Searches INBOX for the last N hours of emails with a matching subject.
3. Parses each email body for lead name, email, and phone.
4. Adds the lead to an existing Zoho Campaigns mailing list.
5. Avoids duplicates by storing processed Gmail message IDs locally.

Before first run
----------------
1. Create a Google Cloud project and enable the Gmail API.
2. Create OAuth client credentials for a Desktop app and save the JSON file as:
   credentials.json
3. In Zoho Campaigns, create or choose the mailing list you want to use and get its list key.
4. Create a Zoho OAuth client, then obtain a refresh token with Campaigns scopes.
5. Copy .env.example values into a real .env file.

Install
-------
pip install google-api-python-client google-auth-httplib2 google-auth-oauthlib requests python-dotenv beautifulsoup4

Run
---
python avail_gmail_to_zoho_sync.py
"""

from __future__ import annotations

import base64
import json
import logging
import os
import re
import sqlite3
import sys
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Iterable, Optional

import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# Gmail readonly is enough because this script only reads and parses messages.
GMAIL_SCOPES = ["https://www.googleapis.com/auth/gmail.readonly"]
GMAIL_TOKEN_FILE = "gmail_token.json"
GMAIL_CREDENTIALS_FILE = "credentials.json"
DB_FILE = "processed_avail_leads.db"


@dataclass
class Lead:
    gmail_message_id: str
    gmail_internal_date_ms: int
    subject: str
    property_name: str
    lead_name: str
    lead_email: Optional[str]
    lead_phone: Optional[str]
    lead_message: Optional[str]


class ConfigError(Exception):
    pass


def load_config() -> dict:
    load_dotenv()

    config = {
        "gmail_subject_phrase": os.getenv(
            "GMAIL_SUBJECT_PHRASE",
            "New Renter Lead for 10045 South Saint Andrews Place",
        ),
        "gmail_from_filter": os.getenv("GMAIL_FROM_FILTER", "reply.avail.co"),
        "lookback_hours": int(os.getenv("LOOKBACK_HOURS", "6")),
        "zoho_client_id": os.getenv("ZOHO_CLIENT_ID", "").strip(),
        "zoho_client_secret": os.getenv("ZOHO_CLIENT_SECRET", "").strip(),
        "zoho_refresh_token": os.getenv("ZOHO_REFRESH_TOKEN", "").strip(),
        "zoho_accounts_base": os.getenv("ZOHO_ACCOUNTS_BASE", "https://accounts.zoho.com").strip(),
        "zoho_campaigns_base": os.getenv("ZOHO_CAMPAIGNS_BASE", "https://campaigns.zoho.com").strip(),
        "zoho_list_key": os.getenv("ZOHO_LIST_KEY", "").strip(),
        "dry_run": os.getenv("DRY_RUN", "false").strip().lower() == "true",
        "log_level": os.getenv("LOG_LEVEL", "INFO").strip().upper(),
    }

    missing = [
        key
        for key in [
            "zoho_client_id",
            "zoho_client_secret",
            "zoho_refresh_token",
            "zoho_list_key",
        ]
        if not config[key]
    ]
    if missing:
        raise ConfigError(f"Missing required environment variables: {', '.join(missing)}")

    return config


def setup_logging(level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level, logging.INFO),
        format="%(asctime)s %(levelname)s %(message)s",
    )


def init_db(db_path: str = DB_FILE) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS processed_messages (
            gmail_message_id TEXT PRIMARY KEY,
            lead_email TEXT,
            processed_at TEXT NOT NULL,
            zoho_status TEXT,
            zoho_response TEXT
        )
        """
    )
    conn.commit()
    return conn


def is_processed(conn: sqlite3.Connection, gmail_message_id: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM processed_messages WHERE gmail_message_id = ?",
        (gmail_message_id,),
    ).fetchone()
    return row is not None


def mark_processed(
    conn: sqlite3.Connection,
    gmail_message_id: str,
    lead_email: Optional[str],
    zoho_status: str,
    zoho_response: str,
) -> None:
    conn.execute(
        """
        INSERT OR REPLACE INTO processed_messages (
            gmail_message_id,
            lead_email,
            processed_at,
            zoho_status,
            zoho_response
        ) VALUES (?, ?, ?, ?, ?)
        """,
        (
            gmail_message_id,
            lead_email,
            datetime.now(timezone.utc).isoformat(),
            zoho_status,
            zoho_response,
        ),
    )
    conn.commit()


def gmail_service() -> object:
    creds = None
    if Path(GMAIL_TOKEN_FILE).exists():
        creds = Credentials.from_authorized_user_file(GMAIL_TOKEN_FILE, GMAIL_SCOPES)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            if not Path(GMAIL_CREDENTIALS_FILE).exists():
                raise FileNotFoundError(
                    f"Missing {GMAIL_CREDENTIALS_FILE}. Download your Google OAuth desktop-app credentials and save them with this filename."
                )
            flow = InstalledAppFlow.from_client_secrets_file(
                GMAIL_CREDENTIALS_FILE, GMAIL_SCOPES
            )
            creds = flow.run_local_server(port=0)

        with open(GMAIL_TOKEN_FILE, "w", encoding="utf-8") as token_file:
            token_file.write(creds.to_json())

    return build("gmail", "v1", credentials=creds)


def build_gmail_query(subject_phrase: str, from_filter: str, lookback_hours: int) -> str:
    # newer_than supports integer hours like 6h in Gmail search syntax.
    return f'in:inbox subject:"{subject_phrase}" from:{from_filter} newer_than:{lookback_hours}h'


def list_matching_message_ids(service: object, query: str) -> list[str]:
    ids: list[str] = []
    page_token: Optional[str] = None

    while True:
        response = (
            service.users()
            .messages()
            .list(userId="me", q=query, labelIds=["INBOX"], pageToken=page_token)
            .execute()
        )
        ids.extend(msg["id"] for msg in response.get("messages", []))
        page_token = response.get("nextPageToken")
        if not page_token:
            break

    return ids


def get_message(service: object, message_id: str) -> dict:
    return (
        service.users()
        .messages()
        .get(userId="me", id=message_id, format="full")
        .execute()
    )


def decode_b64url(data: str) -> str:
    padded = data + "=" * (-len(data) % 4)
    return base64.urlsafe_b64decode(padded.encode("utf-8")).decode("utf-8", errors="replace")


def extract_headers(message: dict) -> dict[str, str]:
    headers = {}
    for item in message.get("payload", {}).get("headers", []):
        name = item.get("name", "")
        value = item.get("value", "")
        headers[name.lower()] = value
    return headers


def extract_body_from_payload(payload: dict) -> str:
    mime_type = payload.get("mimeType", "")
    body_data = payload.get("body", {}).get("data")
    parts = payload.get("parts", [])

    if body_data and mime_type in {"text/plain", "text/html"}:
        return decode_b64url(body_data)

    for part in parts:
        text = extract_body_from_payload(part)
        if text:
            return text

    return ""


def html_to_text(html: str) -> str:
    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text("\n")
    lines = [line.strip() for line in text.splitlines()]
    return "\n".join(line for line in lines if line)


def clean_whitespace(value: str) -> str:
    return re.sub(r"\s+", " ", value).strip()


def parse_lead_from_text(message_id: str, subject: str, body_text: str, internal_date_ms: int) -> Optional[Lead]:
    property_match = re.search(r"New Renter Lead for\s+(.+)$", subject, flags=re.IGNORECASE)
    property_name = property_match.group(1).strip() if property_match else ""

    normalized_text = body_text.replace("\r", "")

    def extract_field(label: str) -> Optional[str]:
        patterns = [
            rf"{label}:\s*([^\n]+)",
            rf"{label}:\s*\n\s*([^\n]+)",
            rf"{label}\s*:\s*\*?\s*([^\n]+)",
        ]
        for pattern in patterns:
            match = re.search(pattern, normalized_text, flags=re.IGNORECASE)
            if match:
                value = clean_whitespace(match.group(1)).lstrip("*•- ").strip()
                if value:
                    return value
        return None

    lead_name = extract_field("Name") or ""
    lead_email = extract_field("Email")
    lead_phone = extract_field("Phone")

    if lead_email:
        email_match = re.search(
            r"[A-Z0-9._%+\-]+@[A-Z0-9.\-]+\.[A-Z]{2,}",
            lead_email,
            flags=re.IGNORECASE,
        )
        lead_email = email_match.group(0) if email_match else None

    if lead_phone:
        phone_match = re.search(
            r"\+?1?[\s\-.]?(?:\(?\d{3}\)?[\s\-.]?)\d{3}[\s\-.]?\d{4}",
            lead_phone,
            flags=re.IGNORECASE,
        )
        lead_phone = clean_whitespace(phone_match.group(0)) if phone_match else None

    lead_message = None
    msg_match = re.search(
        r"Message From Lead:\s*(.+?)\s*Log into your Avail account",
        normalized_text,
        flags=re.IGNORECASE | re.DOTALL,
    )
    if msg_match:
        lead_message = clean_whitespace(msg_match.group(1)).strip('"')

    if not lead_name and not lead_email:
        return None

    return Lead(
        gmail_message_id=message_id,
        gmail_internal_date_ms=internal_date_ms,
        subject=subject,
        property_name=property_name,
        lead_name=lead_name,
        lead_email=lead_email,
        lead_phone=lead_phone,
        lead_message=lead_message,
    )


def parse_gmail_message(message: dict) -> Optional[Lead]:
    headers = extract_headers(message)
    subject = headers.get("subject", "")
    internal_date_ms = int(message.get("internalDate", "0"))

    raw_body = extract_body_from_payload(message.get("payload", {}))
    if "<html" in raw_body.lower():
        body_text = html_to_text(raw_body)
    else:
        body_text = raw_body

    return parse_lead_from_text(message["id"], subject, body_text, internal_date_ms)


def get_zoho_access_token(config: dict) -> str:
    token_url = f"{config['zoho_accounts_base'].rstrip('/')}/oauth/v2/token"
    response = requests.post(
        token_url,
        data={
            "refresh_token": config["zoho_refresh_token"],
            "client_id": config["zoho_client_id"],
            "client_secret": config["zoho_client_secret"],
            "grant_type": "refresh_token",
        },
        timeout=30,
    )
    response.raise_for_status()
    payload = response.json()
    access_token = payload.get("access_token")
    if not access_token:
        raise RuntimeError(f"Unable to obtain Zoho access token: {payload}")
    return access_token


def subscribe_contact_to_zoho(config: dict, access_token: str, lead: Lead) -> dict:
    if not lead.lead_email:
        raise ValueError(f"Lead '{lead.lead_name}' has no email; cannot add to Zoho Campaigns.")

    # listsubscribe supports contact field updates for existing contacts.
    endpoint = f"{config['zoho_campaigns_base'].rstrip('/')}/api/v1.1/json/listsubscribe"

    first_name = lead.lead_name.split()[0] if lead.lead_name else ""
    last_name = " ".join(lead.lead_name.split()[1:]) if len(lead.lead_name.split()) > 1 else ""

    contactinfo = {
        "First Name": first_name,
        "Last Name": last_name,
        "Contact Email": lead.lead_email,
    }

    if lead.lead_phone:
        contactinfo["Phone"] = lead.lead_phone

    response = requests.post(
        endpoint,
        headers={
            "Authorization": f"Zoho-oauthtoken {access_token}",
            "Content-Type": "application/x-www-form-urlencoded",
        },
        data={
            "resfmt": "JSON",
            "listkey": config["zoho_list_key"],
            "contactinfo": json.dumps(contactinfo, separators=(",", ":")),
            "source": "Avail Gmail Sync",
        },
        timeout=30,
    )
    response.raise_for_status()
    return response.json()


def iter_recent_avail_leads(service: object, query: str) -> Iterable[Lead]:
    for message_id in list_matching_message_ids(service, query):
        message = get_message(service, message_id)
        lead = parse_gmail_message(message)
        if lead:
            yield lead
        else:
            logging.warning("Could not parse lead details from Gmail message %s", message_id)


def sync() -> int:
    config = load_config()
    setup_logging(config["log_level"])
    conn = init_db()

    query = build_gmail_query(
        subject_phrase=config["gmail_subject_phrase"],
        from_filter=config["gmail_from_filter"],
        lookback_hours=config["lookback_hours"],
    )
    logging.info("Using Gmail query: %s", query)

    try:
        service = gmail_service()
        access_token = None if config["dry_run"] else get_zoho_access_token(config)

        processed_count = 0
        skipped_count = 0
        error_count = 0

        for lead in iter_recent_avail_leads(service, query):
            if is_processed(conn, lead.gmail_message_id):
                logging.info("Skipping already processed Gmail message %s", lead.gmail_message_id)
                skipped_count += 1
                continue

            logging.info(
                "Parsed lead: name=%s email=%s phone=%s property=%s",
                lead.lead_name,
                lead.lead_email,
                lead.lead_phone,
                lead.property_name,
            )

            if not lead.lead_email:
                logging.warning("Skipping lead with no email: %s", lead.lead_name)
                mark_processed(
                    conn,
                    lead.gmail_message_id,
                    None,
                    "skipped_no_email",
                    "Lead had no email address in parsed message body.",
                )
                skipped_count += 1
                continue

            if config["dry_run"]:
                logging.info("DRY_RUN=true, not sending lead to Zoho: %s", lead.lead_email)
                mark_processed(
                    conn,
                    lead.gmail_message_id,
                    lead.lead_email,
                    "dry_run",
                    json.dumps(lead.__dict__, ensure_ascii=False),
                )
                processed_count += 1
                continue

            try:
                zoho_response = subscribe_contact_to_zoho(config, access_token, lead)
                mark_processed(
                    conn,
                    lead.gmail_message_id,
                    lead.lead_email,
                    "success",
                    json.dumps(zoho_response, ensure_ascii=False),
                )
                processed_count += 1
                logging.info("Added/updated Zoho contact for %s", lead.lead_email)
            except Exception as exc:
                error_count += 1
                logging.exception("Zoho sync failed for Gmail message %s", lead.gmail_message_id)
                # Do not mark as processed on failure so it can be retried.

        logging.info(
            "Done. processed=%s skipped=%s errors=%s",
            processed_count,
            skipped_count,
            error_count,
        )
        return 0 if error_count == 0 else 1

    except HttpError:
        logging.exception("Gmail API error")
        return 1
    except Exception:
        logging.exception("Unhandled error during sync")
        return 1
    finally:
        conn.close()


if __name__ == "__main__":
    sys.exit(sync())
