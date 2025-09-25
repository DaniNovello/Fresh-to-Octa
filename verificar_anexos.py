#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
verificar_anexos.py: Realiza uma verificação e sincronização robusta dos anexos.

Funcionalidades:
- Conecta ao MySQL para obter a lista atual de IDs de tickets.
- Compara com as pastas de tickets no diretório local.
- Se uma pasta existe localmente mas o ticket não está no banco, move a pasta para um subdiretório 'old'.
- Se um ticket existe no banco mas não há uma pasta local, baixa os anexos do Freshdesk.
- Cria uma pasta vazia para tickets sem anexos.
- Garante que o diretório de estado (.state) seja atualizado apenas para as pastas válidas.
"""
from __future__ import annotations

import argparse
import logging
import os
import re
import shutil
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

import requests
from requests.auth import HTTPBasicAuth

try:
    import mysql.connector as mysql
except ImportError:
    print("[ERRO] A biblioteca 'mysql-connector-python' não está instalada. "
          "Instale com: pip install mysql-connector-python", file=sys.stderr)
    sys.exit(1)

# Logger global
LOGGER = logging.getLogger("verificar_anexos")

# -------- Util: .env loader (sem dependências) --------
def load_dotenv(path: str = ".env") -> None:
    if not os.path.isfile(path):
        return
    try:
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                k, v = line.split("=", 1)
                k = k.strip()
                v = v.strip().strip('"').strip("'")
                if k and (k not in os.environ):
                    os.environ[k] = v
    except Exception as e:
        LOGGER.warning("Falha ao ler .env: %s", e)

# -------- Configuração de logging --------
def setup_logger(logfile: Optional[str] = None):
    global LOGGER
    LOGGER.setLevel(logging.INFO)
    fmt = logging.Formatter("[%(levelname)s] %(asctime)s - %(message)s")
    if not LOGGER.handlers:
        sh = logging.StreamHandler(sys.stdout)
        sh.setFormatter(fmt)
        LOGGER.addHandler(sh)
    if logfile:
        fh = logging.FileHandler(logfile, encoding="utf-8")
        fh.setFormatter(fmt)
        LOGGER.addHandler(fh)
    LOGGER.propagate = False

# -------- Config e argumentos --------
@dataclass
class EnvConfig:
    fd_domain: str
    fd_api_key: str
    mysql_host: str
    mysql_port: int
    mysql_user: str
    mysql_password: str
    mysql_db: str

def read_env(env_path: str) -> EnvConfig:
    load_dotenv(env_path)
    return EnvConfig(
        fd_domain=os.getenv("FRESHDESK_DOMAIN", ""),
        fd_api_key=os.getenv("FRESHDESK_API_KEY", ""),
        mysql_host=os.getenv("MYSQL_HOST", "127.0.0.1"),
        mysql_port=int(os.getenv("MYSQL_PORT", "3306")),
        mysql_user=os.getenv("MYSQL_USER", "root"),
        mysql_password=os.getenv("MYSQL_PASSWORD", ""),
        mysql_db=os.getenv("MYSQL_DATABASE", ""),
    )

# -------- Conexão com Banco de Dados --------
def get_all_ticket_ids_from_db(cfg: EnvConfig) -> Set[int]:
    ids: Set[int] = set()
    try:
        conn = mysql.connect(
            host=cfg.mysql_host, port=cfg.mysql_port, user=cfg.mysql_user,
            password=cfg.mysql_password, database=cfg.mysql_db
        )
        cursor = conn.cursor()
        cursor.execute("SELECT `freshdesk_ticket_id` FROM `tickets`")
        for (ticket_id,) in cursor:
            if ticket_id:
                ids.add(int(ticket_id))
        cursor.close()
        conn.close()
        LOGGER.info("CONTROLE: Encontrados %d tickets no banco de dados.", len(ids))
        return ids
    except mysql.Error as e:
        LOGGER.error("Erro ao conectar ou buscar tickets no MySQL: %s", e)
        return ids

# -------- Funções do Sistema de Arquivos --------
def get_existing_ticket_folders(base_dir: Path) -> Set[int]:
    if not base_dir.is_dir():
        return set()
    folder_ids = {int(p.name) for p in base_dir.iterdir() if p.is_dir() and p.name.isdigit()}
    LOGGER.info("CONTROLE: Encontradas %d pastas de tickets em '%s'.", len(folder_ids), base_dir)
    return folder_ids

def ensure_dir(p: Path):
    p.mkdir(parents=True, exist_ok=True)

# -------- Funções da API Freshdesk --------
def fd_base(domain: str) -> str:
    sd = (domain or "").strip().rstrip("/")
    if sd and "." not in sd:
        sd = f"{sd}.freshdesk.com"
    if not sd.startswith("http"):
        sd = "https://" + sd
    return sd

def fd_get(domain: str, api_key: str, path: str, params: Optional[Dict[str, str]] = None,
           max_retries: int = 5, session: Optional[requests.Session] = None) -> requests.Response:
    url = f"{fd_base(domain)}/api/v2{path}"
    sess = session or requests.Session()
    attempt = 0
    while True:
        try:
            r = sess.get(url, params=params or {}, headers={"Accept": "application/json"},
                         auth=HTTPBasicAuth(api_key, "X"), timeout=120)
            if r.status_code == 429:
                retry_after = r.headers.get("Retry-After")
                wait = float(retry_after) if retry_after and retry_after.isdigit() else (2 ** attempt)
                wait = min(wait, 60.0)
                LOGGER.warning("[429] Rate limit Freshdesk. Aguardando %.1fs...", wait)
                time.sleep(wait)
                attempt += 1
                if attempt > max_retries:
                    r.raise_for_status()
                continue
            r.raise_for_status()
            return r
        except requests.RequestException as e:
            if attempt < max_retries:
                wait = min(2 ** attempt, 30)
                LOGGER.warning("Falha de rede em %s: %s. Retentando em %ss...", path, e, wait)
                time.sleep(wait)
                attempt += 1
                continue
            raise

def fd_get_ticket_full(domain: str, api_key: str, ticket_id: int, session: Optional[requests.Session]=None) -> dict:
    r = fd_get(domain, api_key, f"/tickets/{ticket_id}", params={"include": "conversations"}, session=session)
    return r.json()

def safe_filename(name: str) -> str:
    return re.sub(r'[<>:"/\\|?*\x00-\x1F]', "_", name)[:180]

def download_binary(url: str, dest: Path, session: Optional[requests.Session]=None) -> Tuple[bool, int]:
    sess = session or requests.Session()
    try:
        with sess.get(url, timeout=120, stream=True) as rr:
            rr.raise_for_status()
            size = int(rr.headers.get("Content-Length", 0))
            ensure_dir(dest.parent)
            with open(dest, "wb") as f:
                for chunk in rr.iter_content(chunk_size=8192):
                    f.write(chunk)
            return True, size
    except requests.RequestException as e:
        LOGGER.warning("Falha ao baixar %s: %s", url, e)
        return False, 0

def download_attachments_for_ticket(ticket_data: dict, ticket_dir: Path, session: Optional[requests.Session]=None) -> int:
    saved_count = 0
    def handle_one(name: str, url: str):
        nonlocal saved_count
        if not url: return
        fn = safe_filename(name or url.split("/")[-1].split("?")[0] or "attachment")
        ok, size = download_binary(url, ticket_dir / fn, session=session)
        if ok:
            saved_count += 1
            LOGGER.info("Anexo salvo (%d B) para ticket %s: %s", size, ticket_data.get('id'), fn)
    for a in (ticket_data.get("attachments") or []):
        handle_one(a.get("name"), a.get("attachment_url"))
    for c in (ticket_data.get("conversations") or []):
        for a in (c.get("attachments") or []):
            handle_one(a.get("name"), a.get("attachment_url"))
    if saved_count == 0:
        LOGGER.info("Nenhum anexo encontrado para o ticket %s. Pasta criada.", ticket_data.get('id'))
    return saved_count

# -------- Lógica Principal --------
def main():
    parser = argparse.ArgumentParser(description="Verifica e baixa anexos de tickets Freshdesk ausentes.")
    parser.add_argument("--download-dir", required=True, help="Pasta base onde os anexos são armazenados.")
    parser.add_argument("--env-file", default=".env", help="Caminho do arquivo .env")
    parser.add_argument("--log-file", default="verificar_anexos.log", help="Arquivo para salvar os logs.")
    args = parser.parse_args()

    setup_logger(args.log_file)
    
    cfg = read_env(args.env_file)
    if not all([cfg.fd_domain, cfg.fd_api_key, cfg.mysql_db]):
        LOGGER.error("Variáveis de ambiente obrigatórias ausentes: FRESHDESK_DOMAIN, FRESHDESK_API_KEY, MYSQL_DATABASE")
        sys.exit(1)

    # 1. Obter todos os IDs do banco de dados e das pastas locais
    db_ticket_ids = get_all_ticket_ids_from_db(cfg)
    download_path = Path(args.download_dir)
    local_ticket_ids = get_existing_ticket_folders(download_path)

    # 2. NOVA LÓGICA: Identificar pastas "órfãs" (locais, mas não no banco)
    orphan_ids = local_ticket_ids - db_ticket_ids
    if orphan_ids:
        LOGGER.info("Encontradas %d pastas órfãs (não existem no banco de dados). Movendo para 'old/'...", len(orphan_ids))
        old_dir = download_path / "old"
        ensure_dir(old_dir)
        for tid in orphan_ids:
            source_path = download_path / str(tid)
            dest_path = old_dir / str(tid)
            try:
                shutil.move(str(source_path), str(dest_path))
                LOGGER.info("Pasta do ticket %d movida para '%s'.", tid, dest_path)
            except Exception as e:
                LOGGER.error("Erro ao mover a pasta do ticket %d: %s", tid, e)
    else:
        LOGGER.info("Nenhuma pasta órfã encontrada.")

    # 3. LÓGICA ANTIGA: Identificar tickets que precisam ser baixados (no banco, mas não locais)
    missing_ids = db_ticket_ids - local_ticket_ids
    if not missing_ids:
        LOGGER.info("Nenhum ticket novo para baixar. Sincronização de downloads concluída.")
    else:
        LOGGER.info("Encontrados %d tickets para baixar (presentes no banco, mas sem pasta local).", len(missing_ids))
        sess = requests.Session()
        processed_count, total_attachments_saved = 0, 0
        for tid in sorted(list(missing_ids)):
            try:
                LOGGER.info("Processando ticket faltante: %d", tid)
                ticket_data = fd_get_ticket_full(cfg.fd_domain, cfg.fd_api_key, tid, session=sess)
                ticket_dir = download_path / str(tid)
                ensure_dir(ticket_dir)
                saved_count = download_attachments_for_ticket(ticket_data, ticket_dir, session=sess)
                total_attachments_saved += saved_count
                processed_count += 1
            except requests.HTTPError as he:
                if he.response is not None and he.response.status_code == 404:
                    LOGGER.error("Ticket %s não encontrado no Freshdesk (HTTP 404).", tid)
                else:
                    LOGGER.error("Falha HTTP ao buscar ticket %s: %s", tid, he)
            except Exception as e:
                LOGGER.error("Falha inesperada ao processar ticket %s: %s", tid, e)
        LOGGER.info("Download de tickets faltantes concluído. Processados: %d. Anexos salvos: %d",
                    processed_count, total_attachments_saved)

    # 4. Atualizar o diretório .state para TODAS as pastas VÁLIDAS existentes
    LOGGER.info("Atualizando o diretório de estado (.state) para pastas válidas...")
    state_dir = download_path / ".state"
    ensure_dir(state_dir)
    
    final_local_ids = get_existing_ticket_folders(download_path)
    updated_state_files = 0
    for tid in final_local_ids:
        marker = state_dir / f"ticket_{tid}.done"
        if not marker.exists():
            marker.touch()
            updated_state_files += 1

    LOGGER.info("Verificação do .state concluída. %d arquivos de estado criados/atualizados.", updated_state_files)
    LOGGER.info("Script finalizado com sucesso.")

if __name__ == "__main__":
    main()