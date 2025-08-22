import gspread
import os
from functools import lru_cache
from typing import List, Dict, Any
from dotenv import load_dotenv

load_dotenv()

SPREADSHEET_NAME = os.getenv("SPREADSHEET_NAME")
WORKSHEETS_TO_FETCH = ["BANCO_INSIGHTS", "MARCAS", "PLATAFORMAS"]

SERVICE_ACCOUNT_KEY_PATH = os.path.join(
    os.path.dirname(os.path.dirname(__file__)),
    'service_account_key.json'
)

def get_sheets_client():
    try:
        return gspread.service_account(filename=SERVICE_ACCOUNT_KEY_PATH)
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
def fetch_all_data() -> dict:
    client = get_sheets_client()
    if not client:
        return {}

    try:
        print("Buscando dados do Google Sheets...")
        spreadsheet = client.open(SPREADSHEET_NAME)

        all_data = {}

        for worksheet_name in WORKSHEETS_TO_FETCH:
            try:
                print(f" Buscando dados da aba '{worksheet_name}'...")
                ws = spreadsheet.worksheet(worksheet_name)
                data = ws.get_all_records()
                # normaliza nomes de aba para chaves consistentes
                key = worksheet_name.strip().lower()
                if "insight" in key:
                    key = "insights"
                elif "marca" in key:
                    key = "marcas"
                elif "plataform" in key:
                    key = "plataformas"
                all_data[key] = data
                print(f"Os dados da aba '{worksheet_name}' buscados com sucesso. ({len(data)} linhas)")
            except gspread.exceptions.WorksheetNotFound:
                print(f"Erro: Aba '{worksheet_name}' não encontrada.")
                all_data[worksheet_name] = []
            except Exception as e:
                print(f"Erro ao buscar da aba '{worksheet_name}': {e}")
                all_data[worksheet_name] = []

        return all_data
    
    except gspread.exceptions.SpreadsheetNotFound:
        print(f"Erro: Planilha '{SPREADSHEET_NAME}' não encontrada.")
        return {}
    except Exception as e:
        print(f"Erro ao buscar dados da planilha: {e}")
        return {}
