import json
from typing import Optional, List, Dict, Any
import pandas as pd
import unidecode

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from services.sheets_service import fetch_all_data
import uvicorn

def norm(s: Optional[str]) -> Optional[str]:
    if s is None:
        return None
    return unidecode.unidecode(str(s)).strip().lower()

MONTH_MAP = {
    'janeiro': 1, 'fevereiro': 2, 'março': 3, 'abril': 4,
    'maio': 5, 'junho': 6, 'julho': 7, 'agosto': 8,
    'setembro': 9, 'outubro': 10, 'novembro': 11, 'dezembro': 12
}

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

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
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

    for col in ['Marca', 'Plataforma', 'Insight', 'Data do report/status', 'Mês', 'Tipo de insight']:
        if col not in df.columns:
            df[col] = None

    df['mes_numero'] = df['Mês'].astype(str).str.lower().map(MONTH_MAP)

    df['Data do report/status'] = df['Data do report/status'].astype(str).replace({'nan': ''})

    if filters:
        for key, value in filters.items():
            if value and key in df.columns:
                df = df[df[key].isin(value)]

    if search_term:
        st = str(search_term).lower()
        search_cols = ['Marca', 'Plataforma', 'Insight', 'Data do report/status', 'Mês', 'Tipo de insight']
        df = df[df[search_cols].apply(lambda row: row.astype(str).str.lower().str.contains(st, na=False).any(), axis=1)]

    df = df.sort_values(by=['mes_numero'], ascending=True, na_position='last')
    df = df.drop(columns=['mes_numero'])

    return df.to_dict('records')

@app.get("/api/getMetadata")
def get_metadata():
    """
    Retorna o catálogo completo de marcas e plataformas.
    """
    try:
        all_data = fetch_all_data()
        marcas_rows = all_data.get('marcas') or all_data.get('MARCAS') or []
        plataformas_rows = all_data.get('plataformas') or all_data.get('PLATAFORMAS') or []

        marcas_list = [r.get('Marca') for r in marcas_rows]
        plataformas_list = [r.get('Plataforma') for r in plataformas_rows]
        
        brands = sorted(list(set(m for m in marcas_list if m)))
        platforms = sorted(list(set(p for p in plataformas_list if p)))
        
        return {
            "brands": brands,
            "platforms": platforms
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Falha ao carregar metadados: {e}")

@app.get("/api/getData")
def get_insights(
    filters: Optional[str] = Query(None, description='JSON de filtros. Ex: {"Marca":["Budweiser"]}'),
    search: Optional[str] = Query(None, description="Termo de busca para os insights.")
):
    """
    Retorna os insights e os catálogos filtrados.
    """
    try:
        all_data = fetch_all_data()

        insights_rows = all_data.get('insights') or all_data.get('BANCO_INSIGHTS') or []
        marcas_rows = all_data.get('marcas') or all_data.get('MARCAS') or []
        plataformas_rows = all_data.get('plataformas') or all_data.get('PLATAFORMAS') or []

        insights: list[dict] = insights_rows or []
        
        marcas_list_all = sorted(list(set(r.get('Marca') for r in marcas_rows if r.get('Marca'))))
        plataformas_list_all = sorted(list(set(r.get('Plataforma') for r in plataformas_rows if r.get('Plataforma'))))
        
        print(f"[DBG] counts insights={len(insights)} marcas={len(marcas_list_all)} plataformas={len(plataformas_list_all)}")

        filter_dict = json.loads(filters) if filters else None
        filtered_insights = apply_filters_and_search(insights, filter_dict, search) or []

        # Retorna as marcas e plataformas que estão presentes nos insights filtrados
        brands_f = sorted(list(set(i.get('Marca') for i in filtered_insights if i.get('Marca'))))
        plats_f = sorted(list(set(i.get('Plataforma') for i in filtered_insights if i.get('Plataforma'))))
        
        df_ins = pd.DataFrame(filtered_insights) if filtered_insights else pd.DataFrame(columns=['Marca', 'Plataforma'])
        if not df_ins.empty:
            df_ins['__mk'] = df_ins['Marca'].map(norm)
            df_ins['__pk'] = df_ins['Plataforma'].map(norm)
            if 'Mês' in df_ins.columns:
                df_ins['__mes_num'] = df_ins['Mês'].astype(str).str.lower().map(MONTH_MAP)
        else:
            df_ins['__mk'] = []
            df_ins['__pk'] = []

        brand_key = {b: norm(b) for b in marcas_list_all}
        plat_key = {p: norm(p) for p in plataformas_list_all}

        pairs = []
        for b in marcas_list_all:
            mk = brand_key[b]
            for p in plataformas_list_all:
                pk = plat_key[p]
                if not df_ins.empty:
                    sub = df_ins[(df_ins['__mk'] == mk) & (df_ins['__pk'] == pk)].copy()
                    if '__mes_num' in sub.columns and sub['__mes_num'].notna().any():
                        sub = sub.sort_values(['__mes_num'], ascending=True)
                    sub = sub.drop(columns=[c for c in ['__mk', '__pk', '__mes_num'] if c in sub.columns])
                    ins_list = sub.to_dict('records')
                else:
                    ins_list = []

                pairs.append({
                    "Marca": b,
                    "Plataforma": p,
                    "has_insights": len(ins_list) > 0,
                    "insights_count": len(ins_list),
                    "insights": ins_list
                })
        
        print(f"[DBG] /api/getData -> filtered_insights={len(filtered_insights)} brands={len(brands_f)} plats={len(plats_f)} pairs={len(pairs)}")
        print(f"[DBG] keys(all_data)={list(all_data.keys())}")

        return {
            "insights": filtered_insights,
            "brands": marcas_list_all,
            "platforms": plataformas_list_all,
            "pairs": pairs
        }

    except json.JSONDecodeError:
        raise HTTPException(status_code=400, detail="Formato de filtros inválido. O parâmetro 'filters' deve ser um JSON válido.")
    except Exception as e:
        return {"insights": [], "brands": [], "platforms": [], "pairs": []}

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000, reload=True)