import json
from typing import Optional, List, Dict, Any
import pandas as pd

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from services.sheets_service import fetch_all_data
import uvicorn

REQUIRED_COLS = ['Marca', 'Plataforma', 'Insight', 'Data do report/status', 'Mês', 'Tipo de insight']

app = FastAPI(
    title="Painel Insights API",
    description="API para servir dados de insights do Google Sheets."
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    #allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

def apply_filters_and_search(data: List[Dict[str, Any]],
                             filters: Optional[Dict[str, Any]],
                             search_term: Optional[str]) -> List[Dict[str, Any]]:
    if not data:
        return []

    df = pd.DataFrame(data)

    for col in REQUIRED_COLS:
        if col not in df.columns:
            df[col] = None

    df['Data do report/status'] = pd.to_datetime(
        df['Data do report/status'], format='%d/%m/%Y', errors='coerce'
    )

    df.info()

    if filters:
        for key, value in filters.items():
            if value and key in df.columns:
                df = df[df[key].isin(value)]

    if search_term:
        st = str(search_term).lower()
        search_cols = REQUIRED_COLS
        df = df[df[search_cols].apply(
            lambda row: row.astype(str).str.lower().str.contains(st, na=False).any(), axis=1
        )]

    df = df.sort_values(by=['Data do report/status'], ascending=True, na_position='last')

    df['Data do report/status'] = df['Data do report/status'].dt.strftime('%Y-%m-%dT%H:%M:%SZ')

    df = df.where(pd.notna(df), None)

    result = df.to_dict('records')
    print(f"[DBG] resultados: {len(result)}; primeiro: {result[0] if result else 'Nenhum'}")
    return result

@app.get("/api/getMetadata")
def get_metadata():
    try:
        all_data = fetch_all_data()
        marcas_rows = all_data.get('marcas') or all_data.get('MARCAS') or []
        plataformas_rows = all_data.get('plataformas') or all_data.get('PLATAFORMAS') or []

        brands = sorted({r.get('Marca') for r in marcas_rows if r.get('Marca')})
        platforms = sorted({r.get('Plataforma') for r in plataformas_rows if r.get('Plataforma')})

        return {"brands": brands, "platforms": platforms}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Falha ao carregar metadados: {e}")

@app.get("/api/getData")
def get_insights(
    filters: Optional[str] = Query(None, description='JSON de filtros. Ex: {"Marca":["Budweiser"]}'),
    search: Optional[str] = Query(None, description="Termo de busca para os insights.")
):
    try:
        all_data = fetch_all_data()
        insights_rows = all_data.get('insights') or all_data.get('BANCO_INSIGHTS') or []
        marcas_rows = all_data.get('marcas') or all_data.get('MARCAS') or []
        plataformas_rows = all_data.get('plataformas') or all_data.get('PLATAFORMAS') or []

        insights: List[Dict[str, Any]] = insights_rows or []

        brands_all = sorted({r.get('Marca') for r in marcas_rows if r.get('Marca')})
        platforms_all = sorted({r.get('Plataforma') for r in plataformas_rows if r.get('Plataforma')})

        filter_dict = json.loads(filters) if filters else None

        filtered_insights = apply_filters_and_search(insights, filter_dict, search) or []

        print(f"[DBG] /api/getData -> filtered_insights={len(filtered_insights)}")

        return {
            "insights": filtered_insights,
            "brands": brands_all,
            "platforms": platforms_all,
        }

    except json.JSONDecodeError:
        raise HTTPException(status_code=400, detail="Formato de filtros inválido. O parâmetro 'filters' deve ser um JSON válido.")
    except Exception as e:
        print(f"ERRO: {e}")
        return {"insights": [], "brands": [], "platforms": []}

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000, reload=True)
