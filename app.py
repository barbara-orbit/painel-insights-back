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

def apply_filters_and_search(data: List[Dict[str, Any]], filters: Optional[Dict[str, Any]], search_term: Optional[str]) -> List[Dict[str, Any]]:
    """
    Aplica filtros e termo de busca sobre os dados.
    """
    if not data:
        return []

    df = pd.DataFrame(data)

    # Filtro
    if filters:
        for key, value in filters.items():
            if value:
                df = df[df[key].isin(value)]
    
    # Busca
    #if search_term:
    #    search_term = search_term.lower()
    #    search_columns = ['Marca', 'Plataforma', 'Insight', "Data do report/status", "Mês", "Tipo de insight"]
    #    df = df[df[search_columns].apply(
    #        lambda row: row.astype(str).str.lower().str.contains(search_term, na=False).any(),
    #        axis=1
    #    )]
    
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