#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
relatorio_pastas.py: Gera um relatório de quais pastas de tickets locais
existem no banco de dados.

Este script escaneia um diretório local em busca de pastas de tickets (nomeadas com IDs),
compara essa lista com os IDs de tickets existentes no banco de dados MySQL e
gera um arquivo CSV com o status de cada pasta.
"""
from __future__ import annotations

import argparse
import csv
import logging
import os
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Set

try:
    import mysql.connector as mysql
except ImportError:
    print("[ERRO] A biblioteca 'mysql-connector-python' não está instalada. "
          "Instale com: pip install mysql-connector-python", file=sys.stderr)
    sys.exit(1)

# Logger global
LOGGER = logging.getLogger("relatorio_pastas")

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
def setup_logger():
    global LOGGER
    LOGGER.setLevel(logging.INFO)
    fmt = logging.Formatter("[%(levelname)s] %(asctime)s - %(message)s")
    if not LOGGER.handlers:
        sh = logging.StreamHandler(sys.stdout)
        sh.setFormatter(fmt)
        LOGGER.addHandler(sh)
    LOGGER.propagate = False

# -------- Config e argumentos --------
@dataclass
class EnvConfig:
    mysql_host: str
    mysql_port: int
    mysql_user: str
    mysql_password: str
    mysql_db: str

def read_env(env_path: str) -> EnvConfig:
    load_dotenv(env_path)
    return EnvConfig(
        mysql_host=os.getenv("MYSQL_HOST", "127.0.0.1"),
        mysql_port=int(os.getenv("MYSQL_PORT", "3306")),
        mysql_user=os.getenv("MYSQL_USER", "root"),
        mysql_password=os.getenv("MYSQL_PASSWORD", ""),
        mysql_db=os.getenv("MYSQL_DATABASE", ""),
    )

# -------- Funções Principais --------
def get_db_ticket_ids(cfg: EnvConfig) -> Set[int]:
    """Busca todos os IDs de tickets da tabela 'tickets' no MySQL."""
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
        LOGGER.info("Encontrados %d tickets no banco de dados.", len(ids))
        return ids
    except mysql.Error as e:
        LOGGER.error("Erro ao conectar ou buscar tickets no MySQL: %s", e)
        return ids

def get_local_ticket_folders(base_dir: Path) -> Set[int]:
    """Lista os diretórios de tickets existentes na pasta de anexos."""
    if not base_dir.is_dir():
        LOGGER.error("O diretório especificado não existe: %s", base_dir)
        return set()
    
    folder_ids = {int(p.name) for p in base_dir.iterdir() if p.is_dir() and p.name.isdigit()}
    LOGGER.info("Encontradas %d pastas de tickets em '%s'.", len(folder_ids), base_dir)
    return folder_ids

# -------- Lógica Principal --------
def main():
    parser = argparse.ArgumentParser(description="Gera um relatório comparando pastas locais com o banco de dados.")
    parser.add_argument("--scan-dir", required=True, help="Pasta a ser escaneada em busca de diretórios de tickets.")
    parser.add_argument("--output-csv", default="relatorio_pastas.csv", help="Arquivo CSV de saída para o relatório.")
    parser.add_argument("--env-file", default=".env", help="Caminho do arquivo .env")
    args = parser.parse_args()

    setup_logger()
    
    cfg = read_env(args.env_file)
    if not cfg.mysql_db:
        LOGGER.error("Variável de ambiente obrigatória ausente: MYSQL_DATABASE")
        sys.exit(1)

    # 1. Obter IDs do banco de dados
    db_ids = get_db_ticket_ids(cfg)
    if not db_ids:
        LOGGER.warning("Nenhum ticket encontrado no banco de dados para comparação.")

    # 2. Obter IDs das pastas locais
    scan_path = Path(args.scan_dir)
    local_ids = get_local_ticket_folders(scan_path)
    if not local_ids:
        LOGGER.warning("Nenhuma pasta de ticket encontrada no diretório especificado.")
        return

    # 3. Gerar relatório
    try:
        with open(args.output_csv, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["ticket_id", "status_no_banco"])

            for tid in sorted(list(local_ids)):
                status = "Encontrado" if tid in db_ids else "Não Encontrado"
                writer.writerow([tid, status])
        
        LOGGER.info("Relatório salvo com sucesso em: %s", args.output_csv)
    except IOError as e:
        LOGGER.error("Não foi possível escrever o arquivo de relatório: %s", e)

if __name__ == "__main__":
    main()