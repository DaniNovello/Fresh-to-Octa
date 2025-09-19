import os
import sys
import json
import argparse
import re
from typing import Any, Dict, List, Optional, Tuple
from html import unescape
from datetime import datetime, timezone
from pathlib import Path
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

# ========== Utils ==========

INLINE_RE = re.compile(r'https?://[^\s\'"]+\.(?:png|jpe?g|gif|webp|bmp)(?:\?[^\s\'"]*)?', re.IGNORECASE)
TAG_RE = re.compile(r"<[^>]+>")

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
    """Retorna string 'YYYY-MM-DD HH:MM:SS' (MySQL) ou None."""
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
    """Retorna datetime com tz UTC, ou None."""
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
    """Aceita 'YYYY-MM-DD' (sem hora) e devolve datetime UTC no início do dia."""
    if not s:
        return None
    try:
        return datetime.strptime(s, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    except Exception:
        return None

def safe_filename(name: str) -> str:
    # limpa caracteres inválidos no Windows e limita tamanho
    return re.sub(r'[<>:"/\\|?*\x00-\x1F]', "_", name)[:180]

def ensure_dir(p: Path):
    p.mkdir(parents=True, exist_ok=True)

def save_bytes(path: Path, content: bytes) -> None:
    ensure_dir(path.parent)
    with open(path, "wb") as f:
        f.write(content)

def sha256_bytes(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()

# limites INT32 para colunas INT (evitar out-of-range)
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
    """
    Retorna o primeiro valor encontrado no dict cf para qualquer um dos aliases,
    testando também com/sem prefixo 'cf_' e variações comuns com/sem acento.
    """
    # tenta direto
    for k in aliases:
        if k in cf:
            return cf.get(k)

    # tenta com prefixo cf_
    for k in aliases:
        kk = f"cf_{k}"
        if kk in cf:
            return cf.get(kk)

    # mapa de variações comuns
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
    # se não houver ponto, assume Freshdesk padrão
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
    except Exception as e:
        print(f"[warn] contact {contact_id} não carregado: {e}", file=sys.stderr)
        return None

def fd_get_company(domain: str, api_key: str, company_id: int) -> Optional[Dict[str, Any]]:
    try:
        r = fd_get(domain, api_key, f"/companies/{company_id}")
        return r.json()
    except Exception as e:
        print(f"[warn] company {company_id} não carregada: {e}", file=sys.stderr)
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
        """
        Executa um INSERT ... ON DUPLICATE KEY UPDATE id=LAST_INSERT_ID(id)
        e retorna o lastrowid (o id existente ou o novo).
        """
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

# mensagens (conversations)
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

    # code: CF 'codigo' (variação 'cdigo'), senão fallback domains[0]
    code_val = cf_pick(cf, "codigo")
    if not code_val:
        try:
            domains = c.get("domains") or []
            if isinstance(domains, list) and domains:
                code_val = domains[0]
        except Exception:
            code_val = None

    # type: usa tipo_de_cliente
    company_type = cf_pick(cf, "tipo_de_cliente")

    # numero: INT seguro
    numero_val = to_int_or_none(cf_pick(cf, "numero"))

    created_date = None
    try:
        dt = parse_dt(c.get("created_at"))
        if dt:
            created_date = dt.split(" ")[0]  # DATE (YYYY-MM-DD)
    except Exception:
        created_date = None

    return {
        "id": cid,                                   # PK BIGINT
        "fresh_company_id": fresh_company_id,        # INT ou NULL se estourar
        "name": c.get("name"),
        "code": code_val,
        "type": company_type,
        "raw_json": json.dumps(c, ensure_ascii=False),
        "fresh_created_at": created_date,

        # CFs normalizados (com/sem acento, com/sem prefixo cf_)
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
    # campos típicos da conversation: id, body (HTML), created_at, private, from_email, user_id, etc.
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
    """
    Retorna True se o ticket estiver dentro de TODOS os filtros informados.
    Campos usados do ticket: created_at, updated_at (ISO do Freshdesk).
    Intervalos inclusivos; *to* considera 23:59:59 do dia.
    """
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

# ========== Coleta/Persistência de anexos ==========

def collect_conversation_attachments(ticket: Dict[str, Any], max_mb: int = 15, download_dir: Optional[str] = None) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    convs = ticket.get("conversations") or []
    max_bytes = max_mb * 1024 * 1024
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

            name = a.get("name") or url.split("/")[-1].split("?")[0] or "attachment"
            name = safe_filename(name)
            content_type = a.get("content_type") or "application/octet-stream"
            size_guess = a.get("size") or a.get("bytes")
            size_guess = int(size_guess) if size_guess else None

            content = None
            size = size_guess
            try:
                if base_dir:
                    rr = requests.get(url, timeout=120)
                    rr.raise_for_status()
                    content = rr.content
                    size = len(content)
                    if size and size > max_bytes:
                        print(f"[warn] pulo anexo > {max_mb}MB: {url}", file=sys.stderr)
                        continue
                    content_type = rr.headers.get("Content-Type", content_type)
                else:
                    rr = requests.get(url, timeout=30, stream=True)
                    rr.raise_for_status()
                    if not size:
                        size = int(rr.headers.get("Content-Length") or 0) or None
                    content_type = rr.headers.get("Content-Type", content_type)
            except Exception as e:
                print(f"[warn] falha ao acessar anexo: {e}", file=sys.stderr)
                continue

            stored_url = None
            stored_at = None
            digest = None
            if base_dir and content is not None:
                dest = base_dir / name
                save_bytes(dest, content)
                stored_url = str(dest)
                stored_at = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
                digest = sha256_bytes(content)

            out.append({
                "name": name,
                "content_type": content_type,
                "size_bytes": size,
                "fresh_url": url,
                "fresh_url_expires_at": None,
                "stored_url": stored_url,
                "stored_at": stored_at,
                "sha256": digest,
                "conv_id": conv_id,
            })
    return out

def collect_inline_from_description(html: Optional[str], ticket_id: Optional[int] = None, download_dir: Optional[str] = None) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    if not html:
        return out

    base_dir = Path(download_dir) / str(ticket_id) if (download_dir and ticket_id) else None

    for idx, url in enumerate(INLINE_RE.findall(html), 1):
        name = url.split("/")[-1].split("?")[0] or f"inline_{idx}"
        name = f"inline_{idx}_{safe_filename(name)}"

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
                ctype = rr.headers.get("Content-Type")
                dest = base_dir / name
                save_bytes(dest, content)
                stored_url = str(dest)
                stored_at = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
                digest = sha256_bytes(content)
            except Exception as e:
                print(f"[warn] falha ao baixar inline: {e}", file=sys.stderr)

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
):
    # 1) IDs alvo
    if ticket_ids:
        found_ids = list(dict.fromkeys(int(t) for t in ticket_ids))
        print(f"[info] Tickets (IDs diretos): {len(found_ids)}")
    else:
        found_ids: List[int] = []
        # Se houver updated_from, usamos no endpoint como 'updated_since' para reduzir tráfego
        api_updated_since = None
        if updated_from:
            api_updated_since = updated_from.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        elif updated_since:
            api_updated_since = updated_since  # compatibilidade com --since

        for t in fd_paginate_tickets(domain, api_key, per_page=page_size, page_start=1, updated_since=api_updated_since):
            if ticket_in_period(t, created_from, created_to, updated_from, updated_to):
                if "id" in t:
                    found_ids.append(int(t["id"]))

        if not found_ids:
            print("[info] Nenhum ticket encontrado no período/filtro informado.")
            return
        print(f"[info] Tickets listados: {len(found_ids)}")

    # 2) Processa
    batch_rows: List[Dict[str, Any]] = []
    for idx, tid in enumerate(found_ids, 1):
        try:
            full = fd_get_ticket(domain, api_key, tid)
        except Exception as e:
            print(f"[warn] erro ao buscar ticket {tid}: {e}", file=sys.stderr)
            continue

        # ---- Ticket
        row = build_ticket_row(full)
        batch_rows.append(row)

        # Precisamos persistir o ticket antes de messages/attachments (FKs)
        if include_inline or inline_scrape:
            if batch_rows:
                persist_tickets(db, batch_rows)
                batch_rows = []
        else:
            if len(batch_rows) >= 200:
                persist_tickets(db, batch_rows)
                batch_rows = []

        # ---- b_groups (group do ticket)
        g_id = full.get("group_id")
        if g_id:
            g = fd_get_group(domain, api_key, int(g_id))
            if g:
                persist_groups(db, [build_group_row(g)])

        # ---- agents (responder do ticket)
        a_id = full.get("responder_id")
        if a_id:
            a = fd_get_agent(domain, api_key, int(a_id))
            if a:
                persist_agents(db, [build_agent_row(a)])

        # ---- contacts (requester) + companies (se houver)
        r_id = full.get("requester_id")
        if r_id:
            ct = fd_get_contact(domain, api_key, int(r_id))
            if ct:
                comp_id = ct.get("company_id")
                if comp_id:
                    comp = fd_get_company(domain, api_key, int(comp_id))
                    if comp:
                        persist_companies(db, [build_company_row(comp)])
                persist_contacts(db, [build_contact_row(ct)])

        # ---- messages (conversations) -> retorna mapa conv_id -> message_id
        convs = full.get("conversations") or []
        msg_rows = [build_message_row(c, ticket_id=tid) for c in convs if c.get("id")]
        conv_to_msg: Dict[int, int] = {}
        if msg_rows:
            conv_to_msg = persist_messages_return_map(db, msg_rows)

        # ---- attachments
        if include_inline or inline_scrape:
            atts = collect_conversation_attachments(full, max_mb=max_mb, download_dir=download_dir)
            if inline_scrape:
                atts += collect_inline_from_description(full.get("description"), ticket_id=tid, download_dir=download_dir)
            if atts:
                persist_attachments(db, tid, atts, conv_to_msg)

        if idx % 50 == 0:
            print(f"[info] Processados {idx}/{len(found_ids)} tickets...")

    if batch_rows:
        persist_tickets(db, batch_rows)

    print("[ok] Sincronização concluída.")

# ========== CLI ==========

def parse_args():
    p = argparse.ArgumentParser(description="Sync Freshdesk → MySQL (tickets, entities, messages, anexos; download opcional).")
    p.add_argument("--env-file", dest="env_file", default=".env", help="Caminho do arquivo .env (padrão: ./.env)")
    p.add_argument("--fd-domain", dest="fd_domain", help="Domínio Freshdesk. Ex: minhaempresa.freshdesk.com")
    p.add_argument("--fd-key", dest="fd_key", help="API Key Freshdesk")
    p.add_argument("--mysql-host", dest="mysql_host", help="Host MySQL")
    p.add_argument("--mysql-db", dest="mysql_db", help="Database MySQL (alias de MYSQL_DATABASE)")
    p.add_argument("--mysql-user", dest="mysql_user", help="User MySQL")
    p.add_argument("--mysql-pass", dest="mysql_pass", help="Password MySQL (alias de MYSQL_PASSWORD)")
    p.add_argument("--since", dest="updated_since", help="Filtro updated_since (ISO 8601, ex: 2024-01-01T00:00:00Z)")
    p.add_argument("--include-inline", action="store_true", help="Gravar metadados/arquivos de anexos em conversations")
    p.add_argument("--inline-scrape", action="store_true", help="Gravar metadados/arquivos de imagens inline da descrição")
    p.add_argument("--max-attach-mb", type=int, default=15, help="Tamanho máx por anexo (MB)")
    p.add_argument("--page-size", type=int, default=100, help="Tamanho da página (1-100) para paginação (modo --since)")
    p.add_argument("--ticket-id", type=int, action="append", help="ID de ticket específico (repetível)")
    p.add_argument("--ticket-ids", type=str, help="Lista de IDs separados por vírgula. Ex: --ticket-ids 123,456,789")
    p.add_argument("--download-dir", type=str, help="Se definido, baixa anexos para esta pasta (ex.: C:\\anexos_freshdesk_tickets)")

    # NOVO: filtros por período (criação/atualização)
    p.add_argument("--created-from", dest="created_from", help="YYYY-MM-DD (inclusive)")
    p.add_argument("--created-to",   dest="created_to",   help="YYYY-MM-DD (inclusive)")
    p.add_argument("--updated-from", dest="updated_from", help="YYYY-MM-DD (inclusive)")
    p.add_argument("--updated-to",   dest="updated_to",   help="YYYY-MM-DD (inclusive)")

    return p.parse_args()

def main():
    args = parse_args()
    load_dotenv(args.env_file)

    # Freshdesk
    fd_domain = args.fd_domain or env_or("FRESHDESK_DOMAIN")
    fd_key = args.fd_key or env_or("FRESHDESK_API_KEY")

    # MySQL
    mysql_host = args.mysql_host or env_or("MYSQL_HOST", default="127.0.0.1")
    mysql_db   = (args.mysql_db or env_or("MYSQL_DB", "MYSQL_DATABASE"))
    mysql_user = args.mysql_user or env_or("MYSQL_USER", default="root")
    mysql_pass = args.mysql_pass or env_or("MYSQL_PASS", "MYSQL_PASSWORD", default="")

    # Comportamento
    include_inline_default = env_bool("INCLUDE_INLINE_ATTACHMENTS", default=False)
    inline_scrape_default  = env_bool("INCLUDE_HTML_INLINE_SCRAPE", default=False)
    include_inline = args.include_inline or include_inline_default
    inline_scrape  = args.inline_scrape  or inline_scrape_default
    download_dir = args.download_dir or env_or("DOWNLOAD_DIR")

    # Filtros de período
    created_from_dt = parse_date_ymd(args.created_from) if args.created_from else None
    created_to_dt   = parse_date_ymd(args.created_to)   if args.created_to   else None
    updated_from_dt = parse_date_ymd(args.updated_from) if args.updated_from else None
    updated_to_dt   = parse_date_ymd(args.updated_to)   if args.updated_to   else None
    # 'to' inclusivo até 23:59:59
    if created_to_dt:
        created_to_dt = created_to_dt.replace(hour=23, minute=59, second=59)
    if updated_to_dt:
        updated_to_dt = updated_to_dt.replace(hour=23, minute=59, second=59)

    # Checagens
    missing = []
    if not fd_domain: missing.append("FRESHDESK_DOMAIN/--fd-domain")
    if not fd_key: missing.append("FRESHDESK_API_KEY/--fd-key")
    if not mysql_db: missing.append("MYSQL_DATABASE/--mysql-db")
    if missing:
        print(f"[fatal] Variáveis obrigatórias faltando: {', '.join(missing)}", file=sys.stderr)
        sys.exit(2)

    # IDs
    ids: List[int] = []
    if args.ticket_id:
        ids.extend(args.ticket_id)
    if args.ticket_ids:
        for x in args.ticket_ids.split(","):
            x = x.strip()
            if x.isdigit():
                ids.append(int(x))
    ids = list(dict.fromkeys(ids))

    # Page size
    page_size = max(1, min(100, args.page_size))

    # Conexão DB
    db = MySQL(
        host=mysql_host,
        user=mysql_user,
        password=mysql_pass,
        database=mysql_db,
        pool_size=5,
    )

    # Run
    sync_tickets(
        domain=fd_domain,
        api_key=fd_key,
        db=db,
        updated_since=args.updated_since,  # compat
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
    )

if __name__ == "__main__":
    main()