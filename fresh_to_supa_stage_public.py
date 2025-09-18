#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os, sys, base64, argparse, time, hashlib, re, json
from datetime import datetime, date
from typing import Dict, Any, List, Optional
from urllib.parse import quote
import requests
import mysql.connector
from mysql.connector import pooling, Error

INLINE_RE = re.compile(r'https://attachment\.freshdesk\.com/[^"\'\s>]+')

# ---------------- util ----------------
def load_env(path: str):
    env = {}
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            for ln in f:
                ln = ln.strip()
                if not ln or ln.startswith("#"): continue
                if "=" in ln:
                    k,v = ln.split("=",1)
                    env[k.strip()] = v.strip()
    for k,v in os.environ.items():
        if k not in env: env[k]=v
    return env

def b64_auth_freshdesk(api_key: str) -> str:
    return "Basic " + base64.b64encode(f"{api_key}:X".encode("ascii")).decode("ascii")

def dt_iso(s: Optional[str]):
    if not s: return None
    try: return datetime.fromisoformat(s.replace("Z","+00:00"))
    except: return None

def sha256_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()

def json_dumps_dt(obj):
    def _enc(o):
        if isinstance(o, (datetime, date)):
            return o.isoformat()
        raise TypeError(f"Objeto não serializável: {type(o).__name__}")
    return json.dumps(obj, default=_enc)

# ---------------- MySQL (db) ----------------
class MySQL:
    def __init__(self, host, user, password, database):
        self.pool = pooling.MySQLConnectionPool(
            pool_name="db_pool",
            pool_size=5,
            host=host,
            user=user,
            password=password,
            database=database
        )
    
    def upsert(self, table: str, rows: list):
        if not rows: return
        try:
            conn = self.pool.get_connection()
            cursor = conn.cursor()
            
            columns = rows[0].keys()
            cols_str = ", ".join([f"`{c}`" for c in columns])
            
            vals_placeholder = ", ".join(["%s"] * len(columns))
            updates = ", ".join([f"`{c}`=VALUES(`{c}`)" for c in columns])
            
            sql = f"INSERT INTO `{table}` ({cols_str}) VALUES ({vals_placeholder}) ON DUPLICATE KEY UPDATE {updates}"
            
            data_to_insert = [[json.dumps(r[c]) if isinstance(r[c], (list, dict)) else r[c] for c in columns] for r in rows]
            
            cursor.executemany(sql, data_to_insert)
            conn.commit()
            
        except Error as e:
            print(f"[mysql][UPSERT {table}] Falha: {e}", file=sys.stderr)
            raise e
        finally:
            if 'conn' in locals() and conn.is_connected():
                cursor.close()
                conn.close()

    def get_by_field(self, table: str, field: str, value: Any):
        rows = []
        try:
            conn = self.pool.get_connection()
            cursor = conn.cursor(dictionary=True)
            
            if table == "messages":
                sql = f"SELECT `{field}`, `id`, `freshdesk_conv_id` FROM `{table}` WHERE `{field}` = %s"
                cursor.execute(sql, (value,))
            else:
                sql = f"SELECT `{field}`, `id` FROM `{table}` WHERE `{field}` = %s"
                cursor.execute(sql, (value,))
            
            rows = cursor.fetchall()

        except Error as e:
            print(f"[mysql][GET {table}] Falha: {e}", file=sys.stderr)
            raise e
        finally:
            if 'conn' in locals() and conn.is_connected():
                cursor.close()
                conn.close()
        return rows
    
# ---------------- Freshdesk ----------------
def fd_get(fd_domain, fd_key, path, params=""):
    h = {"Authorization": b64_auth_freshdesk(fd_key)}
    url = f"https://{fd_domain}.freshdesk.com/api/v2{path}{params}"
    r = requests.get(url, headers=h, timeout=90)
    r.raise_for_status()
    return r.json()

def fd_get_ticket(fd_domain, fd_key, tid: int):
    return fd_get(fd_domain, fd_key, f"/tickets/{tid}", "?include=conversations,stats")

def fd_search_ids(fd_domain, fd_key, date_from: str, date_to: str):
    h = {"Authorization": b64_auth_freshdesk(fd_key)}
    q = f"\"created_at:>'{date_from}' AND created_at:<'{date_to}'\""
    page, ids = 1, []
    while True:
        url = f"https://{fd_domain}.freshdesk.com/api/v2/search/tickets?query={quote(q)}&page={page}"
        r = requests.get(url, headers=h, timeout=90)
        r.raise_for_status()
        js = r.json()
        res = js.get("results") or []
        if not res: break
        ids += [it["id"] for it in res if "id" in it]
        page += 1
        if page > 50: break
    return ids

def fd_get_contact(fd_domain, fd_key, cid: int):
    return fd_get(fd_domain, fd_key, f"/contacts/{cid}")

def fd_get_agent(fd_domain, fd_key, aid: int):
    return fd_get(fd_domain, fd_key, f"/agents/{aid}")

def fd_get_group(fd_domain, fd_key, gid: int):
    return fd_get(fd_domain, fd_key, f"/groups/{gid}")

def fd_get_company(fd_domain, fd_key, cid: int):
    return fd_get(fd_domain, fd_key, f"/companies/{cid}")

def collect_attachments(ticket: dict, include_inline: bool, html_scrape: bool, max_mb: int):
    max_bytes = int(max_mb)*1024*1024 if max_mb else None
    out: List[Dict[str,Any]] = []
    def from_list(lst, conv_id=None):
        for a in lst or []:
            url, size = a.get("attachment_url"), int(a.get("size") or 0)
            if not url: continue
            content = None
            if not max_bytes or (size and size <= max_bytes) or size == 0:
                try:
                    rr = requests.get(url, timeout=120); rr.raise_for_status()
                    content = rr.content
                except Exception as e:
                    print(f"[warn] download falhou ({a.get('name')}): {e}", file=sys.stderr)
            out.append({
                "name": a.get("name") or "arquivo", "content_type": a.get("content_type") or "application/octet-stream",
                "size_bytes": size or (len(content) if content else 0), "fresh_url": url,
                "inline": bool(a.get("inline")), "content": content, "conv_id": conv_id
            })
    from_list(ticket.get("attachments"))
    for c in ticket.get("conversations") or []:
        from_list(c.get("attachments") or [], conv_id=c.get("id"))
    return out

def download_attachments_to_local_folder(attachments: List[Dict[str, Any]], ticket_id: int):
    base_dir = f"C:\\anexos_freshdesk_tickets\\{ticket_id}"
    os.makedirs(base_dir, exist_ok=True)
    
    print(f"Baixando anexos para o ticket {ticket_id}...")
    
    for a in attachments:
        url = a.get("fresh_url")
        if not url: continue
        
        try:
            file_name = a.get("name")
            if not file_name:
                file_name = url.split('/')[-1].split('?')[0]
                
            file_path = os.path.join(base_dir, file_name)
            
            with requests.get(url, stream=True, timeout=120) as r:
                r.raise_for_status()
                with open(file_path, 'wb') as f:
                    for chunk in r.iter_content(chunk_size=8192):
                        f.write(chunk)
            
            print(f"    - Baixado com sucesso: {file_name}")
            
        except Exception as e:
            print(f"    - Falha ao baixar o anexo {a.get('name')}: {e}", file=sys.stderr)

# ---------------- main ----------------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--env", default=".env")
    ap.add_argument("--ids", help="IDs de tickets (ex: 307430,307429)")
    ap.add_argument("--from", dest="date_from", help="UTC ex: 2025-08-01T00:00:00Z")
    ap.add_argument("--to",   dest="date_to",   help="UTC ex: 2025-09-01T00:00:00Z")
    ap.add_argument("--include-inline", action="store_true")
    ap.add_argument("--inline-scrape",  action="store_true")
    ap.add_argument("--max-attach-mb",  type=int, default=25)
    args = ap.parse_args()

    env = load_env(args.env)
    fd_domain = env["FRESHDESK_DOMAIN"]
    fd_key    = env["FRESHDESK_API_KEY"]
    
    mysql_db = MySQL(
        host=env.get("MYSQL_HOST"),
        user=env.get("MYSQL_USER"),
        password=env.get("MYSQL_PASSWORD"),
        database=env.get("MYSQL_DATABASE")
    )
    
    if args.ids:
        ids = [int(x) for x in args.ids.split(",") if x.strip()]
    else:
        if not (args.date_from and args.date_to):
            print("Informe --ids OU (--from e --to).", file=sys.stderr); sys.exit(2)
        ids = fd_search_ids(fd_domain, fd_key, args.date_from, args.date_to)
        print(f"Encontrados {len(ids)} tickets.")

    for tid in ids:
        t = fd_get_ticket(fd_domain, fd_key, tid)
        
        # contatos
        if t.get("requester_id"):
            try:
                c = fd_get_contact(fd_domain, fd_key, t["requester_id"])
                
                company_id = c.get("company_id")
                if company_id is not None:
                    try:
                        comp = fd_get_company(fd_domain, fd_key, company_id)
                        company_row = {
                            "id": comp["id"],
                            "name": comp.get("name"),
                            "code": comp.get("custom_fields", {}).get("cdigo"),
                            "type": comp.get("custom_fields", {}).get("tipo_de_cliente"),
                            "raw_json": json.dumps(comp)
                        }
                        mysql_db.upsert("companies", [company_row])
                    except Exception as e:
                        print(f"[warn] empresa {company_id}: {e}", file=sys.stderr)

                contact_row = {
                    "freshdesk_id": c["id"], 
                    "email": c.get("email") or "", 
                    "name":  c.get("name"), 
                    "company_id": c.get("company_id"), 
                    "raw_json": json.dumps(c)
                }
                mysql_db.upsert("contacts", [contact_row])
            except Exception as e:
                print(f"[warn] contato {t['requester_id']}: {e}", file=sys.stderr)

        # agentes
        if t.get("responder_id"):
            try:
                a = fd_get_agent(fd_domain, fd_key, t["responder_id"])
                agent_row = {
                    "freshdesk_id": a["id"], 
                    "email": (a.get("contact") or {}).get("email") or "", 
                    "name":  (a.get("contact") or {}).get("name"), 
                    "raw_json": json.dumps(a)
                }
                mysql_db.upsert("agents", [agent_row])
            except Exception as e:
                print(f"[warn] agente {t['responder_id']}: {e}", file=sys.stderr)

        # grupos
        if t.get("group_id") is not None:
            try:
                g = fd_get_group(fd_domain, fd_key, t["group_id"])
                group_row = {
                    "freshdesk_group_id": g["id"], 
                    "name": g.get("name"), 
                    "raw_json": json.dumps(g)
                }
                mysql_db.upsert("b_groups", [group_row])
            except Exception as e:
                print(f"[warn] grupo {t['group_id']}: {e}", file=sys.stderr)

        ticket_row = {
            "freshdesk_ticket_id": t["id"], "subject": t.get("subject"),
            "description_html": t.get("description"), "status": t.get("status"),
            "priority": t.get("priority"), "type": t.get("type"),
            "group_id": t.get("group_id"), "requester_id": t.get("requester_id"),
            "responder_id": t.get("responder_id"), "source": t.get("source"),
            "tags": json.dumps(t.get("tags", [])), "cc_emails": json.dumps(t.get("cc_emails", [])),
            "fwd_emails": json.dumps(t.get("fwd_emails", [])), "reply_cc_emails": json.dumps(t.get("reply_cc_emails", [])),
            "email_config_id": t.get("email_config_id"), "is_escalated": t.get("is_escalated"),
            "due_by": dt_iso(t.get("due_by")), "fr_due_by": dt_iso(t.get("fr_due_by")),
            "created_at_fd": dt_iso(t.get("created_at")), "updated_at_fd": dt_iso(t.get("updated_at")),
            "raw_json": json.dumps(t)
        }
        mysql_db.upsert("tickets", [ticket_row])

        msg_rows=[]
        for c in t.get("conversations") or []:
            msg_rows.append({
                "freshdesk_ticket_id": t["id"], "freshdesk_conv_id": c.get("id"),
                "created_at_fd": dt_iso(c.get("created_at")),
                "author_email": c.get("from_email"), "author_name": str(c.get("user_id") or ""),
                "is_private": bool(c.get("private")), "body_html": c.get("body")
            })
        if msg_rows:
            mysql_db.upsert("messages", msg_rows)

        msg_map = {}
        for m in mysql_db.get_by_field("messages", "freshdesk_ticket_id", t["id"]):
            msg_map[m["freshdesk_conv_id"]] = m["id"]

        cfs = []
        for k,v in (t.get("custom_fields") or {}).items():
            if v not in (None,"",[]):
                cfs.append({
                    "freshdesk_ticket_id": t["id"], "key": str(k).lower().strip(),
                    "value": str(v)
                })
        if cfs:
            mysql_db.upsert("ticket_cfs", cfs)
        
        atts = collect_attachments(t,
            include_inline = args.include_inline or (os.getenv("INCLUDE_INLINE_ATTACHMENTS","false").lower()=="true"),
            html_scrape = args.inline_scrape, max_mb = args.max_attach_mb
        )
        
        # NOVO: Baixa os anexos para uma pasta local
        download_attachments_to_local_folder(atts, tid)
        
        att_rows=[]
        for a in atts:
            # O stored_url agora será nulo, pois não fazemos upload
            stored_url=None
            
            att_rows.append({
                "freshdesk_ticket_id": t["id"],
                "message_id": msg_map.get(a.get("conv_id")),
                "name": a["name"],
                "content_type": a.get("content_type"), "size_bytes": a.get("size_bytes"),
                "fresh_url": a.get("fresh_url"), "stored_url": stored_url,
                "sha256": sha256_bytes(a["content"]) if a.get("content") is not None else None
            })
        if att_rows:
            mysql_db.upsert("attachments", att_rows)

        print(f"OK FD {tid} -> MySQL")

    print("Finalizado Freshdesk ➜ MySQL.")

if __name__ == "__main__":
    main()