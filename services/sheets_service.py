import gspread
import os
from functools import lru_cache
from typing import List, Dict, Any
from dotenv import load_dotenv

load_dotenv()

SPREADSHEET_NAME = os.getenv("SPREADSHEET_NAME")
WORKSHEET_NAME = os.getenv("WORKSHEET_NAME")

SERVICE_ACCOUNT_KEY_PATH = os.path.join(
    os.path.dirname(os.path.dirname(__file__)),
    'service_account_key.json'
)

def get_sheets_client():
    try:
        gc = gspread.service_account(filename=SERVICE_ACCOUNT_KEY_PATH)
        return gc
    except FileNotFoundError:
        print(f"Erro: Arquivo {SERVICE_ACCOUNT_KEY_PATH} não encontrado.")
        return None
    except Exception as e:
        print(f"Erro ao autenticar com o Google Sheets: {e}")
        return None

# Busca os dados da planilha e armazena em cache
# Usa a notação @lru_cache pra evitar múltiplas chamadas
# Atualiza cache a cada reiniciação do servidor
@lru_cache(maxsize=1)
def fetch_all_data() -> List[Dict[str, Any]]:
    client = get_sheets_client()
    if not client:
        return []
    
    try:
        print("Buscando dados do Google Sheets...")
        spreadsheet = client.open(SPREADSHEET_NAME)
        worksheet = spreadsheet.worksheet(WORKSHEET_NAME)
        data = worksheet.get_all_records()
        print(f"Dados buscados com sucesso. Total de {len(data)} linhas.")
        return data
    except gspread.exceptions.SpreadsheetNotFound:
        print(f"Erro: Planilha '{SPREADSHEET_NAME}' não encontrada.")
        return []
    except gspread.exceptions.WorksheetNotFound:
        print(f"Erro: Aba '{WORKSHEET_NAME}' não encontrada.")
        return []
    except Exception as e:
        print(f"Erro ao buscar dados da planilha: {e}")
        return []