#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os, sys, base64, argparse, json, time
from typing import Dict, Any, List, Optional
import requests
import mysql.connector
from mysql.connector import pooling, Error

# ---------------- API Octadesk ----------------
# Usa a autenticação x-api-key que identificamos
def octa_post(octa_url: str, octa_key: str, path: str, body: Dict[str, Any]):
    h = {"x-api-key": octa_key, "Content-Type": "application/json"}
    url = f"{octa_url}{path}"
    r = requests.post(url, headers=h, json=body, timeout=120)
    if not r.ok:
        print(f"[octadesk][POST {path}] {r.status_code} url={url} body={r.text}", file=sys.stderr)
    r.raise_for_status()
    return r.json()

# Nova função para buscar empresas por custom field
def octa_get_companies_by_cf(octa_url: str, octa_key: str, cf_id: str, cf_value: str):
    h = {"x-api-key": octa_key}
    url = f"{octa_url}/api/v1/companies?customFields.{cf_id}={cf_value}"
    r = requests.get(url, headers=h, timeout=120)
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

    def upsert_octa_id(self, table: str, freshdesk_id_column: str, octa_id_column: str, data: List[Dict[str, Any]]):
        if not data: return
        try:
            conn = self.pool.get_connection()
            cursor = conn.cursor()
            sql = f"UPDATE `{table}` SET `{octa_id_column}` = %s WHERE `{freshdesk_id_column}` = %s"
            
            rows_to_update = [
                (row[octa_id_column], row[freshdesk_id_column])
                for row in data
            ]
            
            cursor.executemany(sql, rows_to_update)
            conn.commit()
        except Error as e:
            print(f"[mysql][UPSERT {table}] Falha: {e}", file=sys.stderr)
            raise e
        finally:
            if 'conn' in locals() and conn.is_connected():
                cursor.close()
                conn.close()

# ---------------- main ----------------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--env", default=".env")
    args = ap.parse_args()

    env_path = os.path.join(os.path.dirname(__file__), args.env)
    if not os.path.exists(env_path):
        print(f"ERRO: Arquivo de ambiente '{env_path}' não encontrado.", file=sys.stderr)
        sys.exit(1)
        
    env = {}
    with open(env_path, "r", encoding="utf-8") as f:
        for ln in f:
            ln = ln.strip()
            if not ln or ln.startswith("#"): continue
            if "=" in ln:
                k,v = ln.split("=",1)
                env[k.strip()] = v.strip()

    octa_url = env.get("OCTADESK_BASE_URL")
    octa_key = env.get("OCTADESK_API_KEY")
    mysql_host = env.get("MYSQL_HOST")
    mysql_user = env.get("MYSQL_USER")
    mysql_password = env.get("MYSQL_PASSWORD")
    mysql_database = env.get("MYSQL_DATABASE")

    if not all([octa_url, octa_key, mysql_host, mysql_user, mysql_password, mysql_database]):
        print("ERRO: As credenciais do Octadesk ou MySQL não foram encontradas no arquivo .env.", file=sys.stderr)
        sys.exit(1)

    mysql_db = MySQL(mysql_host, mysql_user, mysql_password, mysql_database)

    # ---------------- Buscando e Mapeando Empresas Existentes ----------------
    print("Iniciando a busca e mapeamento de empresas...")
    companies_data = mysql_db.get_all("companies")
    companies_map = {}
    migrated_companies = []
    
    # ID do campo personalizado "código" no Octadesk
    CF_CODIGO_ID = "ed1431ac-ecfa-4160-942f-4c6d18c7c4ce"

    for company in companies_data:
        company_code = company.get("code")
        if not company_code:
            print(f"[warn] Empresa '{company.get('name')}' ignorada por não ter um campo 'code'.", file=sys.stderr)
            continue
            
        try:
            # Tenta encontrar a empresa no Octadesk pelo código
            search_results = octa_get_companies_by_cf(octa_url, octa_key, CF_CODIGO_ID, company_code)
            
            if search_results:
                octa_company_id = search_results[0].get("id")
                print(f"✓ Empresa '{company.get('name')}' ({company_code}) encontrada no Octadesk. ID: {octa_company_id}")
                companies_map[company["id"]] = octa_company_id
                migrated_companies.append({
                    "id": company["id"],
                    "octa_company_id": octa_company_id
                })
            else:
                print(f"[warn] Empresa '{company.get('name')}' ({company_code}) não encontrada no Octadesk. Apenas crie-a manualmente.", file=sys.stderr)
                
        except Exception as e:
            print(f"ERRO ao buscar empresa '{company.get('name')}': {e}", file=sys.stderr)

    if migrated_companies:
        mysql_db.upsert_octa_id("companies", "id", "octa_company_id", migrated_companies)


    # ---------------- Migrando Contatos ----------------
    print("\nIniciando a migração de contatos...")
    contacts_data = mysql_db.get_all("contacts")
    migrated_contacts = []
    
    for contact in contacts_data:
        try:
            octa_contact_id = contact.get("octa_contact_id")
            if octa_contact_id:
                print(f"Contato '{contact.get('email')}' já tem ID do Octadesk: {octa_contact_id}")
                migrated_contacts.append({"freshdesk_id": contact["freshdesk_id"], "octa_contact_id": octa_contact_id})
                continue
            
            payload = {
                "name": contact.get("name"),
                "email": contact.get("email")
            }

            company_id_from_db = contact.get("company_id")
            if company_id_from_db is not None and company_id_from_db in companies_map:
                payload["companyId"] = companies_map[company_id_from_db]
            elif company_id_from_db is not None:
                print(f"[warn] Contato '{contact.get('email')}' não pode ser associado à empresa {company_id_from_db} pois a empresa não foi encontrada.", file=sys.stderr)

            octa_contact = octa_post(octa_url, octa_key, "/contacts", payload)
            print(f"✓ Contato '{contact.get('email')}' ({contact['freshdesk_id']}) migrado com sucesso. Novo ID: {octa_contact['id']}.")
            migrated_contacts.append({
                "freshdesk_id": contact["freshdesk_id"],
                "octa_contact_id": octa_contact["id"]
            })

        except Exception as e:
            print(f"ERRO ao migrar contato '{contact.get('email')}': {e}", file=sys.stderr)
            migrated_contacts.append({
                "freshdesk_id": contact["freshdesk_id"],
                "octa_contact_id": None
            })

    if migrated_contacts:
        mysql_db.upsert_octa_id("contacts", "freshdesk_id", "octa_contact_id", migrated_contacts)
    
    print("\nFinalizada a migração de contatos e mapeamento de empresas.")

if __name__ == "__main__":
    main()