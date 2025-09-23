import os
import json
import requests
from dotenv import load_dotenv
from openpyxl import Workbook
import logging

# --- Configuração de Logging ---
logging.basicConfig(level=logging.INFO, format='[%(levelname)s] %(asctime)s - %(message)s')

# --- Carregar Variáveis de Ambiente ---
load_dotenv(override=True)

OCTADESK_BASE_URL = os.getenv("OCTADESK_BASE_URL", "").rstrip("/")
OCTADESK_API_KEY = os.getenv("OCTADESK_API_KEY", "").strip()
OCTADESK_AGENT_EMAIL = os.getenv("OCTADESK_AGENT_EMAIL", "").strip()

# --- Nomes dos campos personalizados (como aparecem na API) ---
CONTACT_CUSTOM_FIELD_KEY = "id_contato"
ORG_CUSTOM_FIELD_KEY = "org_id"

# --- Validação das Variáveis de Ambiente ---
if not all([OCTADESK_BASE_URL, OCTADESK_API_KEY, OCTADESK_AGENT_EMAIL]):
    logging.error("As variáveis de ambiente da Octadesk não estão definidas. Verifique seu arquivo .env.")
    exit()

# --- Sessão HTTP ---
session = requests.Session()
session.headers.update({
    "x-api-key": OCTADESK_API_KEY,
    "x-agent-email": OCTADESK_AGENT_EMAIL,
    "Content-Type": "application/json",
})

def get_paged_data(endpoint_url):
    """
    Busca dados de um endpoint da Octadesk com paginação.
    """
    page = 1
    all_items = []
    while True:
        try:
            # Mantendo o limite de 100 por página, que é um padrão seguro
            response = session.get(f"{endpoint_url}?page={page}&limit=100")
            response.raise_for_status()
            data = response.json()
            
            items = data if isinstance(data, list) else data.get("items", []) or data.get("data", [])

            if not items:
                break
            
            all_items.extend(items)
            logging.info(f"Buscando página {page} de {endpoint_url.split('/')[-1]}... {len(items)} itens encontrados.")
            page += 1
            
        except requests.exceptions.RequestException as e:
            logging.error(f"Erro ao buscar dados de {endpoint_url} na página {page}: {e}")
            break
            
    return all_items

def get_custom_field_value(custom_fields, field_key_to_find):
    """
    Extrai o valor de um campo personalizado da lista, buscando pela 'key'.
    """
    if not isinstance(custom_fields, list):
        return ""
        
    for field in custom_fields:
        if isinstance(field, dict) and field.get("key") == field_key_to_find:
            return field.get("value", "")
                
    return ""

def export_to_xlsx():
    """
    Busca contatos e organizações e os exporta para um arquivo .xlsx.
    """
    logging.info("Iniciando a busca por contatos...")
    contacts = get_paged_data(f"{OCTADESK_BASE_URL}/contacts")
    logging.info(f"Total de {len(contacts)} contatos encontrados.")

    logging.info("Iniciando a busca por organizações...")
    organizations = get_paged_data(f"{OCTADESK_BASE_URL}/organizations")
    logging.info(f"Total de {len(organizations)} organizações encontradas.")

    # --- Criação do Workbook e Planilhas ---
    wb = Workbook()
    
    # Planilha de Contatos
    ws_contacts = wb.active
    ws_contacts.title = "Contatos"
    ws_contacts.append(["Octa ID", "Nome", "id_contato"]) # Adicionada nova coluna

    for contact in contacts:
        octa_id = contact.get("id", "N/A") # Extrai o ID principal do contato
        contact_name = contact.get("name", "N/A")
        custom_fields = contact.get("customFields", [])
        contact_id_fresh = get_custom_field_value(custom_fields, CONTACT_CUSTOM_FIELD_KEY)
        ws_contacts.append([octa_id, contact_name, contact_id_fresh])

    # Planilha de Organizações
    ws_orgs = wb.create_sheet("Organizações")
    ws_orgs.append(["Octa ID", "Nome", "org_id"]) # Adicionada nova coluna

    for org in organizations:
        octa_id = org.get("id", "N/A") # Extrai o ID principal da organização
        org_name = org.get("name", "N/A")
        custom_fields = org.get("customFields", [])
        org_id_fresh = get_custom_field_value(custom_fields, ORG_CUSTOM_FIELD_KEY)
        ws_orgs.append([octa_id, org_name, org_id_fresh])

    # --- Salvar o arquivo ---
    output_filename = "octadesk_data.xlsx"
    try:
        wb.save(output_filename)
        logging.info(f"Dados exportados com sucesso para o arquivo '{output_filename}'!")
    except Exception as e:
        logging.error(f"Não foi possível salvar o arquivo .xlsx: {e}")


if __name__ == "__main__":
    export_to_xlsx()