#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os, sys, base64, argparse, json, time
from typing import Dict, Any, List, Optional
from urllib.parse import quote
import requests

# ---------------- util ----------------
def load_env(path: str) -> Dict[str, str]:
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

# ---------------- API Octadesk ----------------
def octa_get(octa_url: str, octa_key: str, path: str, params=""):
    h = {"Authorization": f"Bearer {octa_key}"}
    url = f"{octa_url}/api/v1{path}{params}"
    r = requests.get(url, headers=h, timeout=90)
    r.raise_for_status()
    return r.json()

def octa_get_all(octa_url: str, octa_key: str, path: str):
    res = octa_get(octa_url, octa_key, path)
    return res.get("data", [])

# ---------------- Mapeamento de Freshdesk para Octadesk ----------------
FRESHDESK_PRIORITIES = {"Baixa": 1, "Média": 2, "Alta": 3, "Urgente": 4}
FRESHDESK_STATUS = {"Aberto": 2, "Pendente": 3, "Resolvido": 4, "Fechado": 5, "Aguardando resposta do cliente": 6, "Cancelado": 7}
FRESHDESK_TYPES = {"Problema": "Problema", "Dúvida": "Dúvida", "Incidente": "Incidente", "Requisicao de Servico": "Requisicao de Servico", "Requisicao de Analise": "Requisicao de Analise", "Solicitacao de Material": "Solicitacao de Material", "Requisicao de Acesso": "Requisicao de Acesso"}

# ---------------- main ----------------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--env", default=".env")
    args = ap.parse_args()

    env = load_env(args.env)

    octa_url = env.get("OCTADESK_BASE_URL")
    octa_key = env.get("OCTADESK_API_KEY")

    if not octa_url or not octa_key:
        print("Erro: As variáveis OCTADESK_URL ou OCTADESK_API_KEY não foram encontradas no .env", file=sys.stderr)
        sys.exit(1)

    print("Buscando IDs do Octadesk...")

    print("Mapeando prioridades...")
    octa_priorities = octa_get_all(octa_url, octa_key, "/tickets/priorities")
    priorities_map = []
    for op in octa_priorities:
        freshdesk_name = op.get("name")
        freshdesk_id = FRESHDESK_PRIORITIES.get(freshdesk_name)
        if freshdesk_id:
            priorities_map.append({
                "freshdesk_priority": freshdesk_id,
                "octa_priority_id": op.get("id")
            })

    print("Mapeando status...")
    octa_statuses = octa_get_all(octa_url, octa_key, "/tickets/status")
    statuses_map = []
    for os in octa_statuses:
        freshdesk_name = os.get("name")
        freshdesk_id = FRESHDESK_STATUS.get(freshdesk_name)
        if freshdesk_id:
            statuses_map.append({
                "freshdesk_status": freshdesk_id,
                "octa_status_id": os.get("id")
            })

    print("Mapeando tipos...")
    octa_types = octa_get_all(octa_url, octa_key, "/tickets/types")
    types_map = []
    for ot in octa_types:
        freshdesk_name = ot.get("name")
        freshdesk_id = FRESHDESK_TYPES.get(freshdesk_name)
        if freshdesk_id:
            types_map.append({
                "freshdesk_type": freshdesk_name,
                "octa_type_id": ot.get("id")
            })

    mapping_data = {
        "priorities": priorities_map,
        "statuses": statuses_map,
        "types": types_map
    }

    with open("octadesk_ids.json", "w", encoding="utf-8") as f:
        json.dump(mapping_data, f, indent=2, ensure_ascii=False)

    print("\nFinalizado a geração do 'octadesk_ids.json'.")

if __name__ == "__main__":
    main()