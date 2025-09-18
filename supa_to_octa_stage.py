#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os, sys, base64, argparse, json, time, html
from typing import Dict, Any, List, Optional
from datetime import datetime
import requests
import mysql.connector
from mysql.connector import pooling, Error


# ---------------- API Octadesk ----------------
def octa_post(octa_url: str, octa_key: str, path: str, body: Dict[str, Any]):
    h = {"x-api-key": octa_key, "Content-Type": "application/json"}
    url = f"{octa_url}{path}"
    r = requests.post(url, headers=h, json=body, timeout=120)
    if not r.ok:
        print(f"[octadesk][POST {path}] {r.status_code} url={url} body={r.text}", file=sys.stderr)
    r.raise_for_status()
    return r.json()

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

    def get_all(self, table: str) -> List[Dict[str, Any]]:
        rows = []
        try:
            conn = self.pool.get_connection()
            cursor = conn.cursor(dictionary=True)
            sql = f"SELECT * FROM `{table}`"
            cursor.execute(sql)
            rows = cursor.fetchall()
        except Error as e:
            print(f"[mysql][GET {table}] Falha: {e}", file=sys.stderr)
            raise e
        finally:
            if 'conn' in locals() and conn.is_connected():
                cursor.close()
                conn.close()
        return rows

    def get_by_ticket_id(self, table: str, freshdesk_ticket_id: int) -> List[Dict[str, Any]]:
        rows = []
        try:
            conn = self.pool.get_connection()
            cursor = conn.cursor(dictionary=True)
            sql = f"SELECT * FROM `{table}` WHERE `freshdesk_ticket_id` = %s"
            cursor.execute(sql, (freshdesk_ticket_id,))
            rows = cursor.fetchall()
        except Error as e:
            print(f"[mysql][GET {table}] Falha: {e}", file=sys.stderr)
            raise e
        finally:
            if 'conn' in locals() and conn.is_connected():
                cursor.close()
                conn.close()
        return rows
        
    def get_ticket_ids(self) -> List[int]:
        ids = []
        try:
            conn = self.pool.get_connection()
            cursor = conn.cursor()
            sql = "SELECT `freshdesk_ticket_id` FROM `tickets` ORDER BY `freshdesk_ticket_id`"
            cursor.execute(sql)
            ids = [row[0] for row in cursor.fetchall()]
        except Error as e:
            print(f"[mysql][GET tickets] Falha ao buscar IDs: {e}", file=sys.stderr)
            raise e
        finally:
            if 'conn' in locals() and conn.is_connected():
                cursor.close()
                conn.close()
        return ids
    
    def get_by_field(self, table: str, field_name: str, field_value: Any) -> List[Dict[str, Any]]:
        rows = []
        try:
            conn = self.pool.get_connection()
            cursor = conn.cursor(dictionary=True)
            sql = f"SELECT * FROM `{table}` WHERE `{field_name}` = %s"
            cursor.execute(sql, (field_value,))
            rows = cursor.fetchall()
        except Error as e:
            print(f"[mysql][GET {table}] Falha: {e}", file=sys.stderr)
            raise e
        finally:
            if 'conn' in locals() and conn.is_connected():
                cursor.close()
                conn.close()
        return rows

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

# ---------------- Mapeamento de Campos e IDs ----------------
def create_mapping_dict(rows: List[Dict[str, Any]], fd_key: str, octa_key: str) -> Dict[Any, Any]:
    return {row[fd_key]: row[octa_key] for row in rows}

def find_octa_id(fd_id: Any, mapping_dict: Dict[Any, Any], default: Any = None) -> Any:
    if fd_id is None:
        return default
    return mapping_dict.get(fd_id, default)

def prepare_attachment_for_octa(local_file_path: str, file_name: str) -> Optional[Dict[str, Any]]:
    """Lê um arquivo local, codifica em Base64 e prepara o payload para a Octadesk API."""
    try:
        if not os.path.exists(local_file_path):
            print(f"[warn] Anexo local não encontrado: '{local_file_path}'", file=sys.stderr)
            return None
            
        with open(local_file_path, "rb") as f:
            content_base64 = base64.b64encode(f.read()).decode('utf-8')
            
        return {
            "name": file_name,
            "contentBase64": content_base64
        }
    except Exception as e:
        print(f"[warn] Falha ao ler o anexo local '{local_file_path}': {e}", file=sys.stderr)
        return None

# ---------------- main ----------------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--env", default=".env")
    ap.add_argument("--ids", help="IDs de tickets (ex: 307430,307429)")
    args = ap.parse_args()

    env_path = os.path.join(os.path.dirname(__file__), args.env)
    if not os.path.exists(env_path):
        print(f"ERRO: Arquivo de ambiente '{env_path}' não encontrado.", file=sys.stderr)
        sys.exit(1)
        
    env = load_env(env_path)

    octa_url = env.get("OCTADESK_BASE_URL")
    octa_key = env.get("OCTADESK_API_KEY")
    mysql_host = env.get("MYSQL_HOST")
    mysql_user = env.get("MYSQL_USER")
    mysql_password = env.get("MYSQL_PASSWORD")
    mysql_database = env.get("MYSQL_DATABASE")
    
    email_channel_id = "ce264e9d-7359-4b6a-b5d1-637386358e4d"

    if not all([octa_url, octa_key, mysql_host, mysql_user, mysql_password, mysql_database]):
        print("ERRO: As credenciais do Octadesk ou MySQL não foram encontradas no arquivo .env.", file=sys.stderr)
        sys.exit(1)

    mysql_db = MySQL(mysql_host, mysql_user, mysql_password, mysql_database)

    # ---------------- Buscando Mapeamentos no MySQL ----------------
    print("Buscando tabelas de mapeamento...")
    status_map = create_mapping_dict(mysql_db.get_all("status_map"), "freshdesk_status", "octa_status_id")
    priority_map = create_mapping_dict(mysql_db.get_all("priority_map"), "freshdesk_priority", "octa_priority_id")
    type_map = create_mapping_dict(mysql_db.get_all("type_map"), "freshdesk_type", "octa_type_id")
    group_map = create_mapping_dict(mysql_db.get_all("b_groups"), "freshdesk_group_id", "octa_group_id")
    agent_map = create_mapping_dict(mysql_db.get_all("agents"), "freshdesk_id", "octa_agent_id")
    contact_map = create_mapping_dict(mysql_db.get_all("contacts"), "freshdesk_id", "octa_contact_id")

    # Mapeamento de Custom Fields - Agora lido do .env
    custom_field_map = {
        env.get("OCTADESK_CF_TICKET_FRESH", "ticket_fresh"): env.get("TICKET_FRESH_CF_ID"),
        env.get("OCTADESK_CF_FRESH_CREATED", "fresh_created_at"): env.get("FRESH_CREATED_CF_ID"),
        env.get("OCTADESK_CF_TIPO_DE_TICKET", "tipo_de_ticket"): env.get("TIPO_DE_TICKET_CF_ID"),
        env.get("OCTADESK_CF_SOBRE_OQUE", "sobre_o_que"): env.get("SOBRE_O_QUE_CF_ID"),
        env.get("OCTADESK_CF_PROBLEMA", "problema"): env.get("PROBLEMA_CF_ID"),
    }
    # Filtra IDs vazios
    custom_field_map = {k: v for k, v in custom_field_map.items() if v}

    if args.ids:
        ids_to_migrate = [int(x) for x in args.ids.split(",") if x.strip()]
    else:
        ids_to_migrate = mysql_db.get_ticket_ids()
    
    print(f"\nIniciando a migração de {len(ids_to_migrate)} ticket(s)...")
    
    for freshdesk_ticket_id in ids_to_migrate:
        try:
            print(f"Migrando ticket: {freshdesk_ticket_id}...")
            
            # Buscando todos os dados necessários do MySQL
            ticket_data = mysql_db.get_by_ticket_id("tickets", freshdesk_ticket_id)[0]
            messages_data = mysql_db.get_by_ticket_id("messages", freshdesk_ticket_id)
            cfs_data = mysql_db.get_by_ticket_id("ticket_cfs", freshdesk_ticket_id)
            attachments_data = mysql_db.get_by_field("attachments", "freshdesk_ticket_id", freshdesk_ticket_id)
            
            octa_custom_fields = []
            
            # Mapeamento dos campos personalizados
            if custom_field_map.get("fresh_created_at") and ticket_data.get("created_at_fd"):
                octa_custom_fields.append({
                    "id": custom_field_map["fresh_created_at"],
                    "value": ticket_data["created_at_fd"].isoformat()
                })
                
            octa_type_id = find_octa_id(ticket_data.get("type"), type_map)
            if custom_field_map.get("tipo_de_ticket") and octa_type_id:
                octa_custom_fields.append({
                    "id": custom_field_map["tipo_de_ticket"],
                    "value": octa_type_id
                })

            freshdesk_cfs_map = {
                "cf_problema": "problema",
                "cf_sobre_oque": "sobre_o_que"
            }
            for cf in cfs_data:
                octa_cf_name = freshdesk_cfs_map.get(cf["key"])
                if octa_cf_name and custom_field_map.get(octa_cf_name):
                    octa_custom_fields.append({
                        "id": custom_field_map[octa_cf_name],
                        "value": cf["value"]
                    })
            
            # Mapeamento do responsável
            assigned_id = find_octa_id(ticket_data.get("responder_id"), agent_map)
            assigned_payload = None
            if assigned_id:
                assigned_payload = {"id": assigned_id}
            else:
                agent_data = mysql_db.get_by_field("agents", "freshdesk_id", ticket_data.get("responder_id"))
                if agent_data and agent_data[0].get("email"):
                    assigned_payload = {"email": agent_data[0]["email"]}
            
            # Mapeamento de mensagens e anexos
            octa_interactions = []
            for msg in messages_data:
                octa_attachments = []
                for a in attachments_data:
                    if a.get("message_id") == msg.get("id"):
                        file_path = f"C:\\anexos_freshdesk_tickets\\{freshdesk_ticket_id}\\{a['name']}"
                        attachment_payload = prepare_attachment_for_octa(file_path, a['name'])
                        if attachment_payload:
                            octa_attachments.append(attachment_payload)

                octa_interactions.append({
                    "time": msg["created_at_fd"].isoformat() if msg.get("created_at_fd") else None,
                    "body": msg["body_html"],
                    "author": {
                        "email": msg.get("author_email")
                    },
                    "attachments": octa_attachments
                })

            payload_description = ticket_data.get("description_html", "")

            octa_ticket = {
                "numberChannel": email_channel_id,
                "summary": ticket_data.get("subject", ""),
                "description": payload_description,
                "requester": {
                    "id": find_octa_id(ticket_data.get("requester_id"), contact_map)
                },
                "assigned": assigned_payload,
                "group": {
                    "id": find_octa_id(ticket_data.get("group_id"), group_map)
                },
                "status": {
                    "id": find_octa_id(ticket_data.get("status"), status_map)
                },
                "priority": {
                    "id": find_octa_id(ticket_data.get("priority"), priority_map)
                },
                "type": {
                    "id": find_octa_id(ticket_data.get("type"), type_map)
                },
                "customFields": octa_custom_fields,
                "interactions": octa_interactions,
                "createdAt": ticket_data.get("created_at_fd").isoformat() if ticket_data.get("created_at_fd") else None,
                "updatedAt": ticket_data.get("updated_at_fd").isoformat() if ticket_data.get("updated_at_fd") else None,
            }
            
            created_ticket = octa_post(octa_url, octa_key, "/tickets", octa_ticket)
            print(f"✓ Ticket {freshdesk_ticket_id} migrado com sucesso para Octadesk (ID: {created_ticket.get('id')}).")
            
        except Exception as e:
            print(f"ERRO ao migrar ticket {freshdesk_ticket_id}: {e}", file=sys.stderr)
            
    print("\nFinalizado a migração de Freshdesk ➜ MySQL ➜ Octadesk.")

if __name__ == "__main__":
    main()