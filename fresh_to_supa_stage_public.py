# fresh_to_supa_stage_public.py
import os
import sys
import json
import csv
import argparse
import re
from typing import Any, Dict, List, Optional, Tuple
from html import unescape
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse
import hashlib

import requests
from requests.auth import HTTPBasicAuth
from mysql.connector import pooling, Error as MySQLError

# ========== .env loader (sem dependências) ==========

def load_dotenv(path: str = ".env") -> None:
    if not os.path.isfile(path):
        return
    try:
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                key, val = line.split("=", 1)
                key = key.strip()
                val = val.strip().strip('"').strip("'")
                if key and (key not in os.environ):
                    os.environ[key] = val
    except Exception as e:
        print(f"[warn] Falha ao ler .env: {e}", file=sys.stderr)

# ========== Utils & Logging ==========

INLINE_RE = re.compile(r'https?://[^\s\'"]+\.(?:png|jpe?g|gif|webp|bmp)(?:\?[^\s\'"]*)?', re.IGNORECASE)
TAG_RE = re.compile(r"<[^>]+>")
SIGNATURE_NAME_RE = re.compile(r"(logo|assinatura|signature|rodape|footer|image0+\d|facebook|instagram|linkedin|twitter|whatsapp|tracking|pixel)", re.I)

DEFAULT_INLINE_BLOCKLIST = [
    "italac.com.br/assinatura-italac",
    "cdn.omie.com.br/publish/email",
    "portaldecomprasscala.com/configuracao/scala/logo_interno.jpg",
    "static1.squarespace.com",
]

ERRORS: List[Dict[str, Any]] = []

def log_error(kind: str, ticket_id: Optional[int] = None, **extra):
    ERRORS.append({
        "ts_utc": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
        "type": kind,
        "ticket_id": ticket_id,
        **extra
    })

def write_error_csv(path: str):
    if not ERRORS:
        return
    cols = sorted({k for row in ERRORS for k in row.keys()})
    try:
        with open(path, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=cols)
            w.writeheader()
            w.writerows(ERRORS)
        print(f"[ok] Log de erros salvo em: {path}")
    except Exception as e:
        print(f"[warn] Falhou ao salvar log CSV: {e}", file=sys.stderr)

def html_to_text(html: Optional[str]) -> str:
    if not html:
        return ""
    return unescape(TAG_RE.sub("", html))

def env_or(*keys: str, default: Optional[str] = None) -> Optional[str]:
    for k in keys:
        v = os.getenv(k)
        if v is not None and v != "":
            return v
    return default

def env_bool(name: str, default: bool = False) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    return v.strip().lower() in ("1", "true", "yes", "y", "on")

def parse_dt(s: Optional[str]) -> Optional[str]:
    if not s:
        return None
    try:
        if s.endswith("Z"):
            dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        else:
            dt = datetime.fromisoformat(s)
        return dt.strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return s.replace("T", " ").split(".")[0]

def parse_dt_obj(s: Optional[str]) -> Optional[datetime]:
    if not s:
        return None
    try:
        if s.endswith("Z"):
            return datetime.fromisoformat(s.replace("Z", "+00:00")).astimezone(timezone.utc)
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        else:
            dt = dt.astimezone(timezone.utc)
        return dt
    except Exception:
        try:
            return datetime.strptime(s.replace("T", " ").split(".")[0], "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
        except Exception:
            return None

def parse_date_ymd(s: Optional[str]) -> Optional[datetime]:
    if not s:
        return None
    try:
        return datetime.strptime(s, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    except Exception:
        return None

def safe_filename(name: str) -> str:
    return re.sub(r'[<>:"/\\|?*\x00-\x1F]', "_", name)[:180]

def ensure_dir(p: Path):
    p.mkdir(parents=True, exist_ok=True)

def save_bytes(path: Path, content: bytes) -> None:
    ensure_dir(path.parent)
    with open(path, "wb") as f:
        f.write(content)

def sha256_bytes(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()

def hostname(url: str) -> str:
    try:
        return urlparse(url).netloc.lower()
    except Exception:
        return ""

INT32_MIN = -2147483648
INT32_MAX =  2147483647

def int32_or_none(v) -> Optional[int]:
    try:
        n = int(v)
        if INT32_MIN <= n <= INT32_MAX:
            return n
    except Exception:
        pass
    return None

def to_int_or_none(v) -> Optional[int]:
    try:
        return int(v)
    except Exception:
        return None

def cf_pick(cf: Dict[str, Any], *aliases: str):
    for k in aliases:
        if k in cf:
            return cf.get(k)
    for k in aliases:
        kk = f"cf_{k}"
        if kk in cf:
            return cf.get(kk)
    alt_map = {
        "codigo": ["cdigo", "codigo"],
        "numero": ["nmero", "numero"],
        "endereco": ["endereco", "endereo"],
        "cidade": ["cidade"],
        "estado": ["estado"],
        "email_padrao": ["email_padrao", "email_padro"],
        "tipo_de_cliente": ["tipo_de_cliente"],
        "grupo_de_cliente": ["grupo_de_cliente"],
    }
    for k in aliases:
        if k in alt_map:
            for alt in alt_map[k]:
                if alt in cf:
                    return cf.get(alt)
                cfk = f"cf_{alt}"
                if cfk in cf:
                    return cf.get(cfk)
    return None

# ========== Freshdesk API ==========

def fd_headers() -> Dict[str, str]:
    return {"Content-Type": "application/json", "Accept": "application/json"}

def fd_base(domain: str) -> str:
    domain = (domain or "").strip().rstrip("/")
    if domain and "." not in domain:
        domain = f"{domain}.freshdesk.com"
    if not domain.startswith("http"):
        domain = "https://" + domain
    return domain

def fd_get(domain: str, api_key: str, path: str, query: str = "") -> requests.Response:
    url = f"{fd_base(domain)}/api/v2{path}{query}"
    auth = HTTPBasicAuth(api_key, "X")
    r = requests.get(url, headers=fd_headers(), auth=auth, timeout=120)
    r.raise_for_status()
    return r

def fd_paginate_tickets(domain: str, api_key: str, per_page: int = 100, page_start: int = 1, updated_since: Optional[str] = None):
    page = page_start
    while True:
        q = f"?per_page={per_page}&page={page}"
        if updated_since:
            q += f"&updated_since={updated_since}"
        r = fd_get(domain, api_key, "/tickets", q)
        data = r.json()
        if not data:
            break
        for item in data:
            yield item
        if len(data) < per_page:
            break
        page += 1

def fd_get_ticket(domain: str, api_key: str, ticket_id: int) -> Dict[str, Any]:
    r = fd_get(domain, api_key, f"/tickets/{ticket_id}", "?include=conversations,stats")
    return r.json()

def fd_get_agent(domain: str, api_key: str, agent_id: int) -> Optional[Dict[str, Any]]:
    try:
        r = fd_get(domain, api_key, f"/agents/{agent_id}")
        return r.json()
    except Exception as e:
        print(f"[warn] agente {agent_id} não carregado: {e}", file=sys.stderr)
        return None

def fd_get_group(domain: str, api_key: str, group_id: int) -> Optional[Dict[str, Any]]:
    try:
        r = fd_get(domain, api_key, f"/groups/{group_id}")
        return r.json()
    except Exception as e:
        print(f"[warn] grupo {group_id} não carregado: {e}", file=sys.stderr)
        return None

def fd_get_contact(domain: str, api_key: str, contact_id: int) -> Optional[Dict[str, Any]]:
    try:
        r = fd_get(domain, api_key, f"/contacts/{contact_id}")
        return r.json()
    except requests.HTTPError as he:
        if he.response is not None and he.response.status_code == 404:
            return None
        raise
    except Exception:
        return None

def fd_get_company(domain: str, api_key: str, company_id: int) -> Optional[Dict[str, Any]]:
    try:
        r = fd_get(domain, api_key, f"/companies/{company_id}")
        return r.json()
    except Exception as e:
        print(f"[warn] company {company_id} não carregada: {e}", file=sys.stderr)
        return None

# ========== Octadesk API (lookup) ==========

def octa_headers(api_key: str, agent_email: Optional[str]) -> Dict[str, str]:
    h = {"x-api-key": api_key, "Accept": "application/json"}
    if agent_email:
        h["octa-agent-email"] = agent_email
    return h

def octa_base(url: str) -> str:
    url = (url or "").strip().rstrip("/")
    if not url.startswith("http"):
        url = "https://" + url
    return url

def _fmt_err_body(text: str, maxlen: int = 240) -> str:
    if not text:
        return ""
    t = text.strip().replace("\n", " ")
    return (t[:maxlen] + "…") if len(t) > maxlen else t

def octa_get(base_url: str, api_key: str, agent_email: Optional[str], path: str, params: Optional[Dict[str, Any]] = None, timeout: int = 60) -> Dict[str, Any]:
    url = f"{octa_base(base_url)}{path}"
    r = requests.get(url, headers=octa_headers(api_key, agent_email), params=params or {}, timeout=timeout)
    if not r.ok:
        print(f"[warn] Octa GET {path} status={r.status_code} params={params}", file=sys.stderr)
    r.raise_for_status()
    try:
        return r.json()
    except Exception:
        return {}

def _first_item(payload: Any) -> Optional[Dict[str, Any]]:
    if isinstance(payload, dict):
        for key in ("items", "data", "results"):
            v = payload.get(key)
            if isinstance(v, list) and v:
                return v[0]
    if isinstance(payload, list) and payload:
        return payload[0]
    return None

_OCTA_CONTACT_BY_EMAIL: Dict[str, Dict[str, Any]] = {}
_OCTA_CONTACT_BY_CF: Dict[Tuple[str, str], Dict[str, Any]] = {}
_OCTA_ORG_BY_NAME: Dict[str, Dict[str, Any]] = {}
_OCTA_ORG_BY_CF: Dict[Tuple[str, str], Dict[str, Any]] = {}

def octa_find_contact(api_url: str, api_key: str, agent_email: Optional[str],
                      email: Optional[str], fresh_contact_id: Optional[int],
                      cf_key: Optional[str], timeout: int = 60) -> Optional[Dict[str, Any]]:
    if email:
        if email in _OCTA_CONTACT_BY_EMAIL:
            return _OCTA_CONTACT_BY_EMAIL[email]
        params = {
            "limit": 1,
            "filters[0][property]": "email",
            "filters[0][operator]": "eq",
            "filters[0][value]": email
        }
        try:
            data = octa_get(api_url, api_key, agent_email, "/contacts", params, timeout=timeout)
            item = _first_item(data)
            if item:
                _OCTA_CONTACT_BY_EMAIL[email] = item
                return item
        except Exception as e:
            print(f"[warn] Octa contato GET por email falhou: {e}", file=sys.stderr)

    if cf_key and fresh_contact_id is not None:
        value = str(fresh_contact_id)
        prop = f"customFields.{cf_key}"
        cache_k = (prop, value)
        if cache_k in _OCTA_CONTACT_BY_CF:
            return _OCTA_CONTACT_BY_CF[cache_k]
        params = {
            "limit": 1,
            "filters[0][property]": prop,
            "filters[0][operator]": "eq",
            "filters[0][value]": value
        }
        try:
            data = octa_get(api_url, api_key, agent_email, "/contacts", params, timeout=timeout)
            item = _first_item(data)
            if item:
                _OCTA_CONTACT_BY_CF[cache_k] = item
                return item
        except requests.HTTPError as he:
            body_prev = _fmt_err_body(getattr(he.response, "text", "") if getattr(he, "response", None) else "")
            print(f"[warn] Octa contato GET por CF falhou: {he} body={body_prev}", file=sys.stderr)
        except Exception as e:
            print(f"[warn] Octa contato GET por CF falhou: {e}", file=sys.stderr)

    return None

def octa_find_organization(api_url: str, api_key: str, agent_email: Optional[str],
                           name: Optional[str], fresh_company_id: Optional[int],
                           cf_key: Optional[str], timeout: int = 60) -> Optional[Dict[str, Any]]:
    # muitos ambientes do Octa não aceitam 'filters' em /organizations (retorna INVALID_PROPERTY).
    # nesse caso, não insistimos — preferimos organization vindo do contato.
    try_filters = True
    # 1) por CF (se aceito)
    if try_filters and cf_key and fresh_company_id is not None:
        value = str(fresh_company_id)
        prop = f"customFields.{cf_key}"
        cache_k = (prop, value)
        if cache_k in _OCTA_ORG_BY_CF:
            return _OCTA_ORG_BY_CF[cache_k]
        params = {
            "limit": 1,
            "filters[0][property]": prop,
            "filters[0][operator]": "eq",
            "filters[0][value]": value
        }
        try:
            data = octa_get(api_url, api_key, agent_email, "/organizations", params, timeout=timeout)
            item = _first_item(data)
            if item:
                _OCTA_ORG_BY_CF[cache_k] = item
                return item
        except requests.HTTPError as he:
            txt = _fmt_err_body(getattr(he.response, "text", "") if getattr(he, "response", None) else "")
            if "INVALID_PROPERTY" in txt:
                try_filters = False
            print(f"[warn] Octa org GET por CF falhou: {he} body={txt}", file=sys.stderr)
        except Exception as e:
            print(f"[warn] Octa org GET por CF falhou: {e}", file=sys.stderr)

    # 2) por nome (muitos ambientes também rejeitam)
    if try_filters and name:
        if name in _OCTA_ORG_BY_NAME:
            return _OCTA_ORG_BY_NAME[name]
        params = {
            "limit": 1,
            "filters[0][property]": "name",
            "filters[0][operator]": "eq",
            "filters[0][value]": name
        }
        try:
            data = octa_get(api_url, api_key, agent_email, "/organizations", params, timeout=timeout)
            item = _first_item(data)
            if item:
                _OCTA_ORG_BY_NAME[name] = item
                return item
        except requests.HTTPError as he:
            txt = _fmt_err_body(getattr(he.response, "text", "") if getattr(he, "response", None) else "")
            print(f"[warn] Octa org GET por nome falhou: {he} body={txt}", file=sys.stderr)
        except Exception as e:
            print(f"[warn] Octa org GET por nome falhou: {e}", file=sys.stderr)

    return None

# ========== MySQL ==========

class MySQL:
    def __init__(self, host: str, user: str, password: str, database: str, pool_size: int = 5):
        try:
            self.pool = pooling.MySQLConnectionPool(
                pool_name="db_pool",
                pool_size=pool_size,
                host=host,
                user=user,
                password=password,
                database=database,
                charset="utf8mb4",
                use_unicode=True,
            )
        except MySQLError as e:
            print(f"[fatal] erro ao criar pool MySQL: {e}", file=sys.stderr)
            raise

    def exec_many(self, sql: str, rows: List[Dict[str, Any]]):
        if not rows:
            return
        conn = self.pool.get_connection()
        try:
            cur = conn.cursor()
            cur.executemany(sql, rows)
            conn.commit()
            cur.close()
        except MySQLError as e:
            conn.rollback()
            print(f"[error] exec_many falhou: {e}\nSQL: {sql[:200]}...", file=sys.stderr)
            raise
        finally:
            conn.close()

    def exec_one_returning_id(self, sql: str, row: Dict[str, Any]) -> int:
        conn = self.pool.get_connection()
        try:
            cur = conn.cursor()
            cur.execute(sql, row)
            conn.commit()
            rid = cur.lastrowid
            cur.close()
            return int(rid) if rid else 0
        except MySQLError as e:
            conn.rollback()
            print(f"[error] exec_one_returning_id falhou: {e}\nSQL: {sql[:200]}...", file=sys.stderr)
            raise
        finally:
            conn.close()

# ========== SQL (ajustado ao seu schema) ==========

TICKET_UPSERT_SQL = """
INSERT INTO `tickets` (
    `freshdesk_ticket_id`,
    `subject`,
    `description_html`,
    `status`,
    `priority`,
    `type`,
    `group_id`,
    `requester_id`,
    `responder_id`,
    `source`,
    `created_at_fd`,
    `updated_at_fd`,
    `raw_json`,
    `octa_ticket_id`,
    `tags`,
    `cc_emails`,
    `fwd_emails`,
    `reply_cc_emails`,
    `email_config_id`,
    `is_escalated`,
    `due_by`,
    `fr_due_by`
) VALUES (
    %(freshdesk_ticket_id)s,
    %(subject)s,
    %(description_html)s,
    %(status)s,
    %(priority)s,
    %(type)s,
    %(group_id)s,
    %(requester_id)s,
    %(responder_id)s,
    %(source)s,
    %(created_at_fd)s,
    %(updated_at_fd)s,
    %(raw_json)s,
    %(octa_ticket_id)s,
    %(tags)s,
    %(cc_emails)s,
    %(fwd_emails)s,
    %(reply_cc_emails)s,
    %(email_config_id)s,
    %(is_escalated)s,
    %(due_by)s,
    %(fr_due_by)s
)
ON DUPLICATE KEY UPDATE
    `subject`          = VALUES(`subject`),
    `description_html` = VALUES(`description_html`),
    `status`           = VALUES(`status`),
    `priority`         = VALUES(`priority`),
    `type`             = VALUES(`type`),
    `group_id`         = VALUES(`group_id`),
    `requester_id`     = VALUES(`requester_id`),
    `responder_id`     = VALUES(`responder_id`),
    `source`           = VALUES(`source`),
    `created_at_fd`    = VALUES(`created_at_fd`),
    `updated_at_fd`    = VALUES(`updated_at_fd`),
    `raw_json`         = VALUES(`raw_json`),
    `tags`             = VALUES(`tags`),
    `cc_emails`        = VALUES(`cc_emails`),
    `fwd_emails`       = VALUES(`fwd_emails`),
    `reply_cc_emails`  = VALUES(`reply_cc_emails`),
    `email_config_id`  = VALUES(`email_config_id`),
    `is_escalated`     = VALUES(`is_escalated`),
    `due_by`           = VALUES(`due_by`),
    `fr_due_by`        = VALUES(`fr_due_by`)
"""

ATTACH_UPSERT_SQL = """
INSERT INTO `attachments` (
    `freshdesk_ticket_id`,
    `message_id`,
    `name`,
    `content_type`,
    `size_bytes`,
    `fresh_url`,
    `fresh_url_expires_at`,
    `stored_url`,
    `stored_at`,
    `sha256`
) VALUES (
    %(freshdesk_ticket_id)s,
    %(message_id)s,
    %(name)s,
    %(content_type)s,
    %(size_bytes)s,
    %(fresh_url)s,
    %(fresh_url_expires_at)s,
    %(stored_url)s,
    %(stored_at)s,
    %(sha256)s
)
ON DUPLICATE KEY UPDATE
    `content_type` = VALUES(`content_type`),
    `size_bytes`   = VALUES(`size_bytes`),
    `fresh_url`    = VALUES(`fresh_url`),
    `fresh_url_expires_at` = VALUES(`fresh_url_expires_at`),
    `stored_url`   = VALUES(`stored_url`),
    `stored_at`    = VALUES(`stored_at`),
    `sha256`       = VALUES(`sha256`)
"""

AGENT_UPSERT_SQL = """
INSERT INTO `agents` (
    `freshdesk_id`,
    `email`,
    `name`,
    `raw_json`,
    `octa_agent_id`
) VALUES (
    %(freshdesk_id)s,
    %(email)s,
    %(name)s,
    %(raw_json)s,
    %(octa_agent_id)s
)
ON DUPLICATE KEY UPDATE
    `email`    = VALUES(`email`),
    `name`     = VALUES(`name`),
    `raw_json` = VALUES(`raw_json`),
    `octa_agent_id` = VALUES(`octa_agent_id`)
"""

BGROUP_UPSERT_SQL = """
INSERT INTO `b_groups` (
    `freshdesk_group_id`,
    `name`,
    `raw_json`,
    `octa_group_id`
) VALUES (
    %(freshdesk_group_id)s,
    %(name)s,
    %(raw_json)s,
    %(octa_group_id)s
)
ON DUPLICATE KEY UPDATE
    `name`     = VALUES(`name`),
    `raw_json` = VALUES(`raw_json`),
    `octa_group_id` = VALUES(`octa_group_id`)
"""

COMPANY_UPSERT_SQL = """
INSERT INTO `companies` (
    `id`,
    `fresh_company_id`,
    `name`,
    `code`,
    `type`,
    `raw_json`,
    `fresh_created_at`,
    `cf_endereco`,
    `numero`,
    `cf_cidade`,
    `cf_estado`,
    `cf_grupo_de_cliente`,
    `cf_tipo_de_cliente`,
    `cf_email_padro`,
    `octa_company_id`
) VALUES (
    %(id)s,
    %(fresh_company_id)s,
    %(name)s,
    %(code)s,
    %(type)s,
    %(raw_json)s,
    %(fresh_created_at)s,
    %(cf_endereco)s,
    %(numero)s,
    %(cf_cidade)s,
    %(cf_estado)s,
    %(cf_grupo_de_cliente)s,
    %(cf_tipo_de_cliente)s,
    %(cf_email_padro)s,
    %(octa_company_id)s
)
ON DUPLICATE KEY UPDATE
    `fresh_company_id`   = VALUES(`fresh_company_id`),
    `name`               = VALUES(`name`),
    `code`               = VALUES(`code`),
    `type`               = VALUES(`type`),
    `raw_json`           = VALUES(`raw_json`),
    `fresh_created_at`   = VALUES(`fresh_created_at`),
    `cf_endereco`        = VALUES(`cf_endereco`),
    `numero`             = VALUES(`numero`),
    `cf_cidade`          = VALUES(`cf_cidade`),
    `cf_estado`          = VALUES(`cf_estado`),
    `cf_grupo_de_cliente`= VALUES(`cf_grupo_de_cliente`),
    `cf_tipo_de_cliente` = VALUES(`cf_tipo_de_cliente`),
    `cf_email_padro`     = VALUES(`cf_email_padro`),
    `octa_company_id`    = VALUES(`octa_company_id`)
"""

CONTACT_UPSERT_SQL = """
INSERT INTO `contacts` (
    `freshdesk_id`,
    `email`,
    `name`,
    `company_id`,
    `raw_json`,
    `octa_contact_id`
) VALUES (
    %(freshdesk_id)s,
    %(email)s,
    %(name)s,
    %(company_id)s,
    %(raw_json)s,
    %(octa_contact_id)s
)
ON DUPLICATE KEY UPDATE
    `email`    = VALUES(`email`),
    `name`     = VALUES(`name`),
    `company_id` = VALUES(`company_id`),
    `raw_json` = VALUES(`raw_json`),
    `octa_contact_id` = VALUES(`octa_contact_id`)
"""

MESSAGE_UPSERT_SQL = """
INSERT INTO `messages` (
    `freshdesk_ticket_id`,
    `created_at_fd`,
    `author_email`,
    `author_name`,
    `is_private`,
    `body_html`,
    `freshdesk_conv_id`
) VALUES (
    %(freshdesk_ticket_id)s,
    %(created_at_fd)s,
    %(author_email)s,
    %(author_name)s,
    %(is_private)s,
    %(body_html)s,
    %(freshdesk_conv_id)s
)
ON DUPLICATE KEY UPDATE
    `created_at_fd` = VALUES(`created_at_fd`),
    `author_email`  = VALUES(`author_email`),
    `author_name`   = VALUES(`author_name`),
    `is_private`    = VALUES(`is_private`),
    `body_html`     = VALUES(`body_html`),
    `freshdesk_conv_id` = VALUES(`freshdesk_conv_id`),
    `id` = LAST_INSERT_ID(`id`)
"""

# ========== Mapeamento de dados ==========

def build_ticket_row(t: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "freshdesk_ticket_id": t.get("id"),
        "subject": t.get("subject"),
        "description_html": t.get("description"),
        "status": t.get("status"),
        "priority": t.get("priority"),
        "type": t.get("type"),
        "group_id": t.get("group_id"),
        "requester_id": t.get("requester_id"),
        "responder_id": t.get("responder_id"),
        "source": t.get("source"),
        "created_at_fd": parse_dt(t.get("created_at")),
        "updated_at_fd": parse_dt(t.get("updated_at")),
        "raw_json": json.dumps(t, ensure_ascii=False),
        "octa_ticket_id": None,
        "tags": json.dumps(t.get("tags") or [], ensure_ascii=False),
        "cc_emails": json.dumps(t.get("cc_emails") or [], ensure_ascii=False),
        "fwd_emails": json.dumps(t.get("fwd_emails") or [], ensure_ascii=False),
        "reply_cc_emails": json.dumps(t.get("reply_cc_emails") or [], ensure_ascii=False),
        "email_config_id": t.get("email_config_id"),
        "is_escalated": 1 if t.get("is_escalated") else 0 if t.get("is_escalated") is not None else None,
        "due_by": parse_dt(t.get("due_by")),
        "fr_due_by": parse_dt(t.get("fr_due_by")),
    }

def build_agent_row(a: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "freshdesk_id": a.get("id"),
        "email": a.get("email") or "",
        "name": a.get("contact", {}).get("name") or a.get("name") or None,
        "raw_json": json.dumps(a, ensure_ascii=False),
        "octa_agent_id": None,
    }

def build_group_row(g: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "freshdesk_group_id": g.get("id"),
        "name": g.get("name"),
        "raw_json": json.dumps(g, ensure_ascii=False),
        "octa_group_id": None,
    }

def build_company_row(c: Dict[str, Any]) -> Dict[str, Any]:
    cf = (c.get("custom_fields") or {}) if isinstance(c.get("custom_fields"), dict) else {}
    cid = c.get("id")
    fresh_company_id = int32_or_none(cid)
    code_val = cf_pick(cf, "codigo")
    if not code_val:
        try:
            domains = c.get("domains") or []
            if isinstance(domains, list) and domains:
                code_val = domains[0]
        except Exception:
            code_val = None
    company_type = cf_pick(cf, "tipo_de_cliente")
    numero_val = to_int_or_none(cf_pick(cf, "numero"))
    created_date = None
    try:
        dt = parse_dt(c.get("created_at"))
        if dt:
            created_date = dt.split(" ")[0]
    except Exception:
        created_date = None
    return {
        "id": cid,
        "fresh_company_id": fresh_company_id,
        "name": c.get("name"),
        "code": code_val,
        "type": company_type,
        "raw_json": json.dumps(c, ensure_ascii=False),
        "fresh_created_at": created_date,
        "cf_endereco": cf_pick(cf, "endereco"),
        "numero": numero_val,
        "cf_cidade": cf_pick(cf, "cidade"),
        "cf_estado": cf_pick(cf, "estado"),
        "cf_grupo_de_cliente": cf_pick(cf, "grupo_de_cliente"),
        "cf_tipo_de_cliente": cf_pick(cf, "tipo_de_cliente"),
        "cf_email_padro": cf_pick(cf, "email_padrao"),
        "octa_company_id": None,
    }

def build_contact_row(c: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "freshdesk_id": c.get("id"),
        "email": c.get("email") or "",
        "name": c.get("name"),
        "company_id": c.get("company_id"),
        "raw_json": json.dumps(c, ensure_ascii=False),
        "octa_contact_id": None,
    }

def build_message_row(conv: Dict[str, Any], ticket_id: int) -> Dict[str, Any]:
    return {
        "freshdesk_ticket_id": ticket_id,
        "created_at_fd": parse_dt(conv.get("created_at")),
        "author_email": conv.get("from_email") or conv.get("email") or "",
        "author_name": conv.get("from_name") or conv.get("user_name") or None,
        "is_private": 1 if conv.get("private") else 0,
        "body_html": conv.get("body") or "",
        "freshdesk_conv_id": conv.get("id"),
    }

# ========== Filtro por período ==========

def ticket_in_period(t: Dict[str, Any],
                     created_from: Optional[datetime],
                     created_to:   Optional[datetime],
                     updated_from: Optional[datetime],
                     updated_to:   Optional[datetime]) -> bool:
    cdt = parse_dt_obj(t.get("created_at"))
    udt = parse_dt_obj(t.get("updated_at"))
    if created_from and (not cdt or cdt < created_from):
        return False
    if created_to and (not cdt or cdt > created_to):
        return False
    if updated_from and (not udt or udt < updated_from):
        return False
    if updated_to and (not udt or udt > updated_to):
        return False
    return True

# ========== Regras para anexos/inline ==========

ALLOWED_DOC_CT = {
    "application/pdf",
    "application/msword",
    "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
    "application/vnd.ms-excel",
    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    "application/vnd.ms-powerpoint",
    "application/vnd.openxmlformats-officedocument.presentationml.presentation",
    "application/zip",
    "application/x-zip-compressed",
    "application/vnd.rar",
    "application/x-7z-compressed",
    "text/plain",
}

def is_signature_like(name: str, url: str) -> bool:
    n = (name or "").lower()
    u = (url or "").lower()
    return bool(SIGNATURE_NAME_RE.search(n) or SIGNATURE_NAME_RE.search(u))

def content_type_allowed(ct: Optional[str]) -> bool:
    if not ct:
        return True
    ct = ct.lower()
    if ct.startswith("image/") or ct.startswith("audio/") or ct.startswith("video/"):
        return True
    if ct in ALLOWED_DOC_CT:
        return True
    return False

# ========== Coleta/Persistência de anexos ==========

def collect_conversation_attachments(ticket: Dict[str, Any], max_mb: int, download_dir: Optional[str],
                                     min_kb: int, attach_signature_block: bool = True) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    convs = ticket.get("conversations") or []
    max_bytes = max_mb * 1024 * 1024
    min_bytes = max(0, min_kb) * 1024
    tid = ticket.get("id")

    base_dir = Path(download_dir) if download_dir else None
    if base_dir:
        base_dir = base_dir / str(tid)

    for c in convs:
        conv_id = c.get("id")
        atts = c.get("attachments") or []
        for a in atts:
            url = a.get("attachment_url") or a.get("url") or a.get("content_url")
            if not url:
                continue

            name = safe_filename(a.get("name") or url.split("/")[-1].split("?")[0] or "attachment")
            ct = a.get("content_type") or "application/octet-stream"
            size_guess = a.get("size") or a.get("bytes")
            size_guess = int(size_guess) if size_guess else None

            # **NOVO**: bloquear por assinatura só se habilitado
            if attach_signature_block and is_signature_like(name, url):
                log_error("attachment_skipped_signature_like", tid, conv_id=conv_id, name=name, url=url)
                continue

            if not content_type_allowed(ct):
                log_error("attachment_skipped_content_type", tid, conv_id=conv_id, name=name, url=url, content_type=ct)
                continue

            content = None
            size = size_guess
            try:
                if base_dir:
                    rr = requests.get(url, timeout=120)
                    rr.raise_for_status()
                    content = rr.content
                    size = len(content)
                    if size and size > max_bytes:
                        log_error("attachment_skipped_too_large", tid, conv_id=conv_id, name=name, url=url, size=size)
                        print(f"[warn] pulo anexo > {max_mb}MB: {url}", file=sys.stderr)
                        continue
                    ct = rr.headers.get("Content-Type", ct)
                else:
                    rr = requests.get(url, timeout=30, stream=True)
                    rr.raise_for_status()
                    if not size:
                        try:
                            size = int(rr.headers.get("Content-Length") or 0) or None
                        except Exception:
                            size = None
                    ct = rr.headers.get("Content-Type", ct)
            except requests.HTTPError as he:
                code = he.response.status_code if he.response is not None else None
                log_error("conv_attachment_download_failed", tid, conv_id=conv_id, name=name, url=url, http_status=code)
                print(f"[warn] conv attachment download falhou: {he}", file=sys.stderr)
                continue
            except Exception as e:
                log_error("conv_attachment_download_failed", tid, conv_id=conv_id, name=name, url=url, err=str(e))
                print(f"[warn] conv attachment download falhou: {e}", file=sys.stderr)
                continue

            if size is not None and size < min_bytes:
                log_error("attachment_skipped_too_small", tid, conv_id=conv_id, name=name, url=url, size=size)
                continue

            stored_url = None
            stored_at = None
            digest = None
            if base_dir and content is not None:
                dest = base_dir / name
                ensure_dir(dest.parent)
                save_bytes(dest, content)
                stored_url = str(dest)
                stored_at = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
                digest = sha256_bytes(content)

            out.append({
                "name": name,
                "content_type": ct,
                "size_bytes": size,
                "fresh_url": url,
                "fresh_url_expires_at": None,
                "stored_url": stored_url,
                "stored_at": stored_at,
                "sha256": digest,
                "conv_id": conv_id,
            })
    return out

def collect_inline_from_description(html: Optional[str], ticket_id: Optional[int], download_dir: Optional[str],
                                    min_kb: int, block_hosts: List[str]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    if not html:
        return out

    base_dir = Path(download_dir) / str(ticket_id) if (download_dir and ticket_id) else None
    min_bytes = max(0, min_kb) * 1024

    for idx, url in enumerate(INLINE_RE.findall(html), 1):
        name = url.split("/")[-1].split("?")[0] or f"inline_{idx}"
        name = f"inline_{idx}_{safe_filename(name)}"
        host = hostname(url)

        url_l = url.lower()
        blocked = any(b in url_l for b in block_hosts) or is_signature_like(name, url)
        if blocked:
            log_error("inline_blocked_by_pattern", ticket_id, url=url, name=name, host=host)
            continue

        stored_url = None
        stored_at = None
        digest = None
        size = None
        ctype = None

        if base_dir:
            try:
                rr = requests.get(url, timeout=120)
                rr.raise_for_status()
                content = rr.content
                size = len(content)
                if size is not None and size < min_bytes:
                    log_error("inline_skipped_too_small", ticket_id, url=url, name=name, size=size)
                    continue
                ctype = rr.headers.get("Content-Type")
                if not content_type_allowed(ctype):
                    log_error("inline_skipped_content_type", ticket_id, url=url, name=name, content_type=ctype)
                    continue
                dest = base_dir / name
                ensure_dir(dest.parent)
                save_bytes(dest, content)
                stored_url = str(dest)
                stored_at = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
                digest = sha256_bytes(content)
            except requests.HTTPError as he:
                code = he.response.status_code if he.response is not None else None
                log_error("inline_download_failed", ticket_id, url=url, name=name, http_status=code)
                print(f"[warn] falha ao baixar inline: {he}", file=sys.stderr)
                continue
            except Exception as e:
                log_error("inline_download_failed", ticket_id, url=url, name=name, err=str(e))
                print(f"[warn] falha ao baixar inline: {e}", file=sys.stderr)
                continue

        out.append({
            "name": name,
            "content_type": ctype,
            "size_bytes": size,
            "fresh_url": url,
            "fresh_url_expires_at": None,
            "stored_url": stored_url,
            "stored_at": stored_at,
            "sha256": digest,
            "conv_id": None,
        })
    return out

# ========== Persistência por tabela ==========

def persist_tickets(db: MySQL, rows: List[Dict[str, Any]]):
    if not rows:
        return
    db.exec_many(TICKET_UPSERT_SQL, rows)

def persist_attachments(db: MySQL, ticket_id: int, atts: List[Dict[str, Any]], conv_to_msg: Dict[int, int]):
    if not atts:
        return
    rows = []
    for a in atts:
        conv_id = a.get("conv_id")
        msg_id = conv_to_msg.get(conv_id) if conv_id else None
        rows.append({
            "freshdesk_ticket_id": ticket_id,
            "message_id": msg_id,
            "name": a.get("name"),
            "content_type": a.get("content_type"),
            "size_bytes": a.get("size_bytes"),
            "fresh_url": a.get("fresh_url"),
            "fresh_url_expires_at": a.get("fresh_url_expires_at"),
            "stored_url": a.get("stored_url"),
            "stored_at": a.get("stored_at"),
            "sha256": a.get("sha256"),
        })
    db.exec_many(ATTACH_UPSERT_SQL, rows)

def persist_agents(db: MySQL, rows: List[Dict[str, Any]]):
    if not rows:
        return
    db.exec_many(AGENT_UPSERT_SQL, rows)

def persist_groups(db: MySQL, rows: List[Dict[str, Any]]):
    if not rows:
        return
    db.exec_many(BGROUP_UPSERT_SQL, rows)

def persist_companies(db: MySQL, rows: List[Dict[str, Any]]):
    if not rows:
        return
    db.exec_many(COMPANY_UPSERT_SQL, rows)

def persist_contacts(db: MySQL, rows: List[Dict[str, Any]]):
    if not rows:
        return
    db.exec_many(CONTACT_UPSERT_SQL, rows)

def persist_messages_return_map(db: MySQL, rows: List[Dict[str, Any]]) -> Dict[int, int]:
    conv_to_msg: Dict[int, int] = {}
    if not rows:
        return conv_to_msg
    for r in rows:
        msg_id = db.exec_one_returning_id(MESSAGE_UPSERT_SQL, r)
        conv_id = r.get("freshdesk_conv_id")
        if conv_id is not None and msg_id:
            conv_to_msg[int(conv_id)] = int(msg_id)
    return conv_to_msg

# ========== Orquestração ==========

def sync_tickets(
    domain: str,
    api_key: str,
    db: MySQL,
    updated_since: Optional[str],
    include_inline: bool,
    inline_scrape: bool,
    max_mb: int,
    page_size: int = 100,
    ticket_ids: Optional[List[int]] = None,
    download_dir: Optional[str] = None,
    created_from: Optional[datetime] = None,
    created_to: Optional[datetime] = None,
    updated_from: Optional[datetime] = None,
    updated_to: Optional[datetime] = None,
    min_attach_kb: int = 5,
    inline_block_hosts: Optional[List[str]] = None,
    # Octa
    octa_lookup: bool = False,
    octa_url: Optional[str] = None,
    octa_key: Optional[str] = None,
    octa_agent_email_hdr: Optional[str] = None,
    octa_contact_cf_key: Optional[str] = None,
    octa_org_cf_key: Optional[str] = None,
    octa_timeout: int = 60,
    # anexos
    attach_signature_block: bool = True,
):
    # 1) IDs alvo
    if ticket_ids:
        found_ids = list(dict.fromkeys(int(t) for t in ticket_ids))
        print(f"[info] Tickets (IDs diretos): {len(found_ids)}")
    else:
        found_ids: List[int] = []
        api_updated_since = None
        if updated_from:
            api_updated_since = updated_from.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        elif updated_since:
            api_updated_since = updated_since
        for t in fd_paginate_tickets(domain, api_key, per_page=page_size, page_start=1, updated_since=api_updated_since):
            if ticket_in_period(t, created_from, created_to, updated_from, updated_to):
                if "id" in t:
                    found_ids.append(int(t["id"]))
        if not found_ids:
            print("[info] Nenhum ticket encontrado no período/filtro informado.")
            return
        print(f"[info] Tickets listados: {len(found_ids)}")

    if inline_block_hosts is None:
        inline_block_hosts = list(DEFAULT_INLINE_BLOCKLIST)

    # 2) Processa
    batch_rows: List[Dict[str, Any]] = []
    for idx, tid in enumerate(found_ids, 1):
        try:
            full = fd_get_ticket(domain, api_key, tid)
        except Exception as e:
            log_error("ticket_fetch_failed", tid, err=str(e))
            print(f"[warn] erro ao buscar ticket {tid}: {e}", file=sys.stderr)
            continue

        # ---- Ticket
        row = build_ticket_row(full)
        batch_rows.append(row)

        # persist ticket antes de messages/attachments (FKs)
        if include_inline or inline_scrape:
            if batch_rows:
                persist_tickets(db, batch_rows)
                batch_rows = []
        else:
            if len(batch_rows) >= 200:
                persist_tickets(db, batch_rows)
                batch_rows = []

        # ---- b_groups
        g_id = full.get("group_id")
        if g_id:
            g = fd_get_group(domain, api_key, int(g_id))
            if g:
                persist_groups(db, [build_group_row(g)])

        # ---- agents
        a_id = full.get("responder_id")
        if a_id:
            a = fd_get_agent(domain, api_key, int(a_id))
            if a:
                persist_agents(db, [build_agent_row(a)])

        # ---- contacts + companies (+ Octa lookup)
        r_id = full.get("requester_id")
        octa_contact_id: Optional[str] = None
        octa_company_id: Optional[str] = None

        if r_id:
            ct = None
            try:
                ct = fd_get_contact(domain, api_key, int(r_id))
            except Exception:
                ct = None
            if not ct:
                log_error("contact_not_found", tid, contact_id=r_id)
            else:
                comp_id = ct.get("company_id")
                comp_json = None
                if comp_id:
                    comp_json = fd_get_company(domain, api_key, int(comp_id))

                if octa_lookup and octa_url and octa_key:
                    try:
                        octa_ct = octa_find_contact(
                            api_url=octa_url,
                            api_key=octa_key,
                            agent_email=octa_agent_email_hdr,
                            email=ct.get("email"),
                            fresh_contact_id=ct.get("id"),
                            cf_key=octa_contact_cf_key,
                            timeout=octa_timeout,
                        )
                        if octa_ct:
                            octa_contact_id = str(octa_ct.get("id")) if octa_ct.get("id") is not None else None
                            org_in_ct = octa_ct.get("organization") or {}
                            if isinstance(org_in_ct, dict) and org_in_ct.get("id") is not None:
                                octa_company_id = str(org_in_ct.get("id"))
                        else:
                            log_error("octa_contact_not_found", tid, contact_fresh_id=ct.get("id"), email=ct.get("email"))

                        # tentar org só se ainda não veio do contato e se o ambiente aceitar filtros
                        if not octa_company_id and comp_json:
                            octa_org = octa_find_organization(
                                api_url=octa_url,
                                api_key=octa_key,
                                agent_email=octa_agent_email_hdr,
                                name=(comp_json.get("name") if isinstance(comp_json, dict) else None),
                                fresh_company_id=(comp_json.get("id") if isinstance(comp_json, dict) else None),
                                cf_key=octa_org_cf_key,
                                timeout=octa_timeout,
                            )
                            if octa_org and octa_org.get("id") is not None:
                                octa_company_id = str(octa_org.get("id"))
                            elif comp_json:
                                log_error("octa_org_not_found", tid, company_fresh_id=comp_json.get("id"), company_name=comp_json.get("name"))
                    except Exception as e:
                        log_error("octa_lookup_failed", tid, err=str(e))

                if comp_json and isinstance(comp_json, dict):
                    comp_row = build_company_row(comp_json)
                    if octa_company_id:
                        comp_row["octa_company_id"] = octa_company_id
                    persist_companies(db, [comp_row])

                ct_row = build_contact_row(ct)
                if octa_contact_id:
                    ct_row["octa_contact_id"] = octa_contact_id
                persist_contacts(db, [ct_row])

        # ---- messages
        convs = full.get("conversations") or []
        msg_rows = [build_message_row(c, ticket_id=tid) for c in convs if c.get("id")]
        conv_to_msg: Dict[int, int] = {}
        if msg_rows:
            conv_to_msg = persist_messages_return_map(db, msg_rows)

        # ---- attachments
        if include_inline or inline_scrape:
            atts = collect_conversation_attachments(
                full,
                max_mb=max_mb,
                download_dir=download_dir,
                min_kb=min_attach_kb,
                attach_signature_block=attach_signature_block
            )
            if inline_scrape:
                atts += collect_inline_from_description(
                    full.get("description"),
                    ticket_id=tid,
                    download_dir=download_dir,
                    min_kb=min_attach_kb,
                    block_hosts=inline_block_hosts
                )
            if atts:
                persist_attachments(db, tid, atts, conv_to_msg)

        if idx % 50 == 0:
            print(f"[info] Processados {idx}/{len(found_ids)} tickets...")

    if batch_rows:
        persist_tickets(db, batch_rows)

    print("[ok] Sincronização concluída.")

# ========== CLI ==========

def parse_args():
    p = argparse.ArgumentParser(description="Sync Freshdesk → MySQL (tickets, entities, messages, anexos; lookup Octa opcional).")
    p.add_argument("--env-file", dest="env_file", default=".env")
    p.add_argument("--fd-domain", dest="fd_domain")
    p.add_argument("--fd-key", dest="fd_key")
    p.add_argument("--mysql-host", dest="mysql_host")
    p.add_argument("--mysql-db", dest="mysql_db")
    p.add_argument("--mysql-user", dest="mysql_user")
    p.add_argument("--mysql-pass", dest="mysql_pass")
    p.add_argument("--since", dest="updated_since")
    p.add_argument("--include-inline", action="store_true")
    p.add_argument("--inline-scrape", action="store_true")
    p.add_argument("--max-attach-mb", type=int, default=15)
    p.add_argument("--page-size", type=int, default=100)
    p.add_argument("--ticket-id", type=int, action="append")
    p.add_argument("--ticket-ids", type=str)
    p.add_argument("--download-dir", type=str)
    p.add_argument("--created-from", dest="created_from")
    p.add_argument("--created-to",   dest="created_to")
    p.add_argument("--updated-from", dest="updated_from")
    p.add_argument("--updated-to",   dest="updated_to")

    # Log & filtros de anexos
    p.add_argument("--error-log", dest="error_log")
    p.add_argument("--min-attach-kb", dest="min_attach_kb", type=int, default=5)
    p.add_argument("--inline-block-host", dest="inline_block_hosts", action="append")
    p.add_argument("--no-inline-default-blocklist", dest="no_inline_default_blocklist", action="store_true")

    # Octa lookup
    p.add_argument("--no-octa-lookup", dest="no_octa_lookup", action="store_true")
    p.add_argument("--octa-timeout", dest="octa_timeout", type=int, default=60)

    # **NOVO**: não bloquear anexos por assinatura/logo
    p.add_argument("--no-attach-signature-block", dest="no_attach_signature_block", action="store_true")

    return p.parse_args()

def main():
    args = parse_args()
    load_dotenv(args.env_file)

    fd_domain = args.fd_domain or env_or("FRESHDESK_DOMAIN")
    fd_key = args.fd_key or env_or("FRESHDESK_API_KEY")

    mysql_host = args.mysql_host or env_or("MYSQL_HOST", default="127.0.0.1")
    mysql_db   = (args.mysql_db or env_or("MYSQL_DB", "MYSQL_DATABASE"))
    mysql_user = args.mysql_user or env_or("MYSQL_USER", default="root")
    mysql_pass = args.mysql_pass or env_or("MYSQL_PASS", "MYSQL_PASSWORD", default="")

    include_inline_default = env_bool("INCLUDE_INLINE_ATTACHMENTS", default=False)
    inline_scrape_default  = env_bool("INCLUDE_HTML_INLINE_SCRAPE", default=False)
    include_inline = args.include_inline or include_inline_default
    inline_scrape  = args.inline_scrape  or inline_scrape_default
    download_dir = args.download_dir or env_or("DOWNLOAD_DIR")

    created_from_dt = parse_date_ymd(args.created_from) if args.created_from else None
    created_to_dt   = parse_date_ymd(args.created_to)   if args.created_to   else None
    updated_from_dt = parse_date_ymd(args.updated_from) if args.updated_from else None
    updated_to_dt   = parse_date_ymd(args.updated_to)   if args.updated_to   else None
    if created_to_dt:
        created_to_dt = created_to_dt.replace(hour=23, minute=59, second=59)
    if updated_to_dt:
        updated_to_dt = updated_to_dt.replace(hour=23, minute=59, second=59)

    missing = []
    if not fd_domain: missing.append("FRESHDESK_DOMAIN/--fd-domain")
    if not fd_key: missing.append("FRESHDESK_API_KEY/--fd-key")
    if not mysql_db: missing.append("MYSQL_DATABASE/--mysql-db")
    if missing:
        print(f"[fatal] Variáveis obrigatórias faltando: {', '.join(missing)}", file=sys.stderr)
        sys.exit(2)

    ids: List[int] = []
    if args.ticket_id:
        ids.extend(args.ticket_id)
    if args.ticket_ids:
        for x in args.ticket_ids.split(","):
            x = x.strip()
            if x.isdigit():
                ids.append(int(x))
    ids = list(dict.fromkeys(ids))

    page_size = max(1, min(100, args.page_size))

    inline_block_hosts = [] if args.no_inline_default_blocklist else list(DEFAULT_INLINE_BLOCKLIST)
    if args.inline_block_hosts:
        inline_block_hosts.extend(args.inline_block_hosts)

    db = MySQL(
        host=mysql_host,
        user=mysql_user,
        password=mysql_pass,
        database=mysql_db,
        pool_size=5,
    )

    # Octa setup
    octa_url = env_or("OCTADESK_BASE_URL")
    octa_key = env_or("OCTADESK_API_KEY")
    octa_agent_email_hdr = env_or("OCTADESK_AGENT_EMAIL")
    octa_contact_cf_key = env_or("OCTA_CONTACT_FRESH_ID_KEY")
    octa_org_cf_key = env_or("OCTA_ORG_FRESH_ID_KEY")
    octa_lookup_enabled = (not args.no_octa_lookup) and (octa_url and octa_key and octa_agent_email_hdr)

    if (not args.no_octa_lookup) and not octa_lookup_enabled:
        print("[info] Lookup no Octa desativado (faltam OCTADESK_BASE_URL, OCTADESK_API_KEY ou OCTADESK_AGENT_EMAIL).")

    # Run
    sync_tickets(
        domain=fd_domain,
        api_key=fd_key,
        db=db,
        updated_since=args.updated_since,
        include_inline=include_inline,
        inline_scrape=inline_scrape,
        max_mb=args.max_attach_mb,
        page_size=page_size,
        ticket_ids=ids if ids else None,
        download_dir=download_dir,
        created_from=created_from_dt,
        created_to=created_to_dt,
        updated_from=updated_from_dt,
        updated_to=updated_to_dt,
        min_attach_kb=args.min_attach_kb,
        inline_block_hosts=inline_block_hosts,
        # Octa
        octa_lookup=octa_lookup_enabled,
        octa_url=octa_url,
        octa_key=octa_key,
        octa_agent_email_hdr=octa_agent_email_hdr,
        octa_contact_cf_key=octa_contact_cf_key,
        octa_org_cf_key=octa_org_cf_key,
        octa_timeout=max(5, int(args.octa_timeout)),
        # anexos
        attach_signature_block=not args.no_attach_signature_block,
    )

    log_path = args.error_log or f"./errors_freshdesk_sync_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.csv"
    write_error_csv(log_path)

if __name__ == "__main__":
    main()
