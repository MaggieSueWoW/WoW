import re
import json
import hashlib
import logging
from typing import Dict, Any
import requests

WCL_OAUTH_URL = "https://www.warcraftlogs.com/oauth/token"
WCL_API_URL = "https://www.warcraftlogs.com/api/v2/client"

LOGGER = logging.getLogger("wcl")

REPORT_GQL = """
query ReportFightsAndActors($code: String!, $translate: Boolean = true) {
  reportData {
    report(code: $code) {
      code
      startTime
      endTime
      region { id name compactName }
      guild { id name server { name region { id name compactName } } }
      fights {
        id
        encounterID
        name
        difficulty
        startTime
        endTime
        friendlyPlayers
        kill
      }
      masterData(translate: $translate) {
        actors(type: "Player") {
          id
          name
          server
          subType
          type
        }
      }
    }
  }
}
"""

def extract_report_code(url_or_code: str) -> str:
    if not url_or_code:
        return ""
    m = re.search(r"/reports/([A-Za-z0-9]+)", url_or_code)
    return m.group(1) if m else url_or_code.strip()

def get_token(client_id: str, client_secret: str) -> str:
    LOGGER.info("Requesting Warcraft Logs OAuth token…")
    resp = requests.post(
        WCL_OAUTH_URL,
        data={"grant_type": "client_credentials"},
        auth=(client_id, client_secret),
        timeout=30
    )
    resp.raise_for_status()
    tok = resp.json()["access_token"]
    LOGGER.info("Got OAuth token.")
    return tok

def gql(token: str, query: str, variables: Dict[str, Any]) -> Dict[str, Any]:
    LOGGER.info("GraphQL request → %s vars=%s", WCL_API_URL, variables)
    resp = requests.post(
        WCL_API_URL,
        json={"query": query, "variables": variables},
        headers={"Authorization": f"Bearer {token}", "Accept": "application/json"},
        timeout=60
    )
    resp.raise_for_status()
    data = resp.json()
    if "errors" in data:
        LOGGER.error("GraphQL errors: %s", data["errors"])
        raise RuntimeError(f"GraphQL errors: {data['errors']}")
    LOGGER.info("GraphQL request ok.")
    return data["data"]

def fetch_report(token: str, code: str) -> Dict[str, Any]:
    return gql(token, REPORT_GQL, {"code": code, "translate": True})["reportData"]["report"]

def stable_digest(obj: Any) -> str:
    raw = json.dumps(obj, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()
