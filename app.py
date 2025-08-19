import json
import pandas as pd
from typing import Optional, List, Dict, Any
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from services.sheets_service import fetch_all_data
from pydantic import BaseModel, Field
import uvicorn

class InsightRaw(BaseModel):
    Autor: Optional[str] = None
    Marca: Optional[str] = None
    Plataforma: Optional[str] = None
    Insight: Optional[str] = None
    data_do_report_status: Optional[str] = Field(None, alias='Data do report/status')
    mes: Optional[str] = Field(None, alias='Mês')
    Links: Optional[str] = None
    tipo_de_insight: Optional[str] = Field(None, alias='Tipo de insight')

app = FastAPI(
    title="Painel Insights API",
    description="API para servir dados de insights do Google Sheets."
)

# Configuração de CORS para permitir acesso do Angular
origins = [
    "http://localhost",
    "http://localhost:4200",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

import pandas as pd
from typing import List, Dict, Any, Optional

def apply_filters_and_search(data: List[Dict[str, Any]], filters: Optional[Dict[str, Any]], search_term: Optional[str]) -> List[Dict[str, Any]]:
    """
    Aplica filtros, termo de busca e ordena os dados pela coluna 'Mês'.
    """
    if not data:
        return []

    df = pd.DataFrame(data)

    month_mapping = {
        'janeiro': 1, 'fevereiro': 2, 'março': 3, 'abril': 4,
        'maio': 5, 'junho': 6, 'julho': 7, 'agosto': 8,
        'setembro': 9, 'outubro': 10, 'novembro': 11, 'dezembro': 12
    }

    df['mes_numero'] = df['Mês'].str.lower().map(month_mapping)

    df['Data do report/status'] = df['Data do report/status'].astype(str).replace({'nan': ''})

    if filters:
        for key, value in filters.items():
            if value and key in df.columns:
                df = df[df[key].isin(value)]

    if search_term:
        search_term = search_term.lower()
        search_columns = ['Marca', 'Plataforma', 'Insight', "Data do report/status", "Mês", "Tipo de insight"]
        df = df[df[search_columns].apply(
            lambda row: row.astype(str).str.lower().str.contains(search_term, na=False).any(),
            axis=1
        )]
    
    df = df.sort_values(by=['mes_numero'], ascending=True, na_position='last')

    df = df.drop(columns=['mes_numero'])
    
    return df.to_dict('records')

@app.get("/api/getData")
def get_insights(
    filters: Optional[str] = Query(None, description="Filtros em formato JSON. Ex: '{\"Marca\":[\"Budweiser\",\"Corona\"]}'"),
    search: Optional[str] = Query(None, description="Termo de busca para os insights.")
):
    """
    Endpoint para buscar, filtrar e pesquisar insights.
    Retorna uma lista de dicionários.
    """
    try:
        data = fetch_all_data()

        filter_dict = json.loads(filters) if filters else None

        filtered_data = apply_filters_and_search(data, filter_dict, search)

        return {"insights": filtered_data}
    
    except json.JSONDecodeError:
        raise HTTPException(status_code=400, detail="Formato de filtros inválido. O parâmetro 'filters' deve ser um JSON válido.")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro interno do servidor: {e}")

if __name__ == '__main__':
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)