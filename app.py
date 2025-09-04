import json
from typing import Optional, List, Dict, Any
import pandas as pd
from fastapi import FastAPI, HTTPException, Query, Body
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from services.sheets_service import fetch_all_data
import uvicorn

REQUIRED_COLS = ['Marca', 'Plataforma', 'Insight', 'Data do report/status', 'Mês', 'Tipo de insight']
DATE_COL = 'Data do report/status' 
DATE_COLS = ['Data do report/status', 'LTV']
DATE_INPUT_FMT = '%d/%m/%Y'
OUTPUT_DATE_FMT = '%d-%m-%Y'

CANON_TO_COL = {
    "brand": "Marca",
    "platform": "Plataforma",
    "insight_type": "Tipo de insight",
    "month": "Mês",
}

SEARCHABLE_COLS = ['Marca', 'Plataforma', 'Insight', 'Tipo de insight', 'Mês', 'LTV']

app = FastAPI(title="Painel Insights API", description="API para servir dados de insights do Google Sheets.")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"],
)

class FilterPayload(BaseModel):
    brand: Optional[List[str]] = None
    platform: Optional[List[str]] = None
    insight_type: Optional[List[str]] = None
    month: Optional[List[str]] = None

class DataRequest(BaseModel):
    filters: Optional[FilterPayload] = None
    search: Optional[str] = Field(default=None, description="Busca textual")
    start_date: Optional[str] = Field(default=None, description="YYYY-MM-DD")
    end_date: Optional[str] = Field(default=None, description="YYYY-MM-DD")

class DataResponse(BaseModel):
    insights: List[Dict[str, Any]]
    brands: List[str]
    platforms: List[str]

class OptionsResponse(BaseModel):
    brand: List[str]
    platform: List[str]
    insight_type: List[str]
    month: List[str]

def _ensure_required_cols(df: pd.DataFrame) -> pd.DataFrame:
    for col in REQUIRED_COLS:
        if col not in df.columns:
            df[col] = None
    return df

def _parse_sheet_dates(df: pd.DataFrame) -> pd.DataFrame:
    for col in DATE_COLS:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], format=DATE_INPUT_FMT, errors='coerce')
    return df


def _apply_canonical_filters(df: pd.DataFrame, f: Optional[FilterPayload]) -> pd.DataFrame:
    if f is None:
        return df
    out = df.copy()
    for canon_key, colname in CANON_TO_COL.items():
        values = getattr(f, canon_key, None)
        if values and colname in out.columns:
            out = out[out[colname].isin(values)]
    return out

def _apply_search(df: pd.DataFrame, search_term: Optional[str]) -> pd.DataFrame:
    if not search_term:
        return df
    st = str(search_term).lower()
    cols = [c for c in SEARCHABLE_COLS if c in df.columns]
    if not cols:
        return df
    mask = df[cols].apply(lambda row: row.astype(str).str.lower().str.contains(st, na=False).any(), axis=1)
    return df[mask]

def _apply_date_range(df: pd.DataFrame, start_date: Optional[str], end_date: Optional[str]) -> pd.DataFrame:
    out = df.copy()
    if DATE_COL not in out.columns:
        return out
    if start_date:
        sdt = pd.to_datetime(start_date, errors='coerce')
        if pd.notna(sdt): out = out[out[DATE_COL] >= sdt]
    if end_date:
        edt = pd.to_datetime(end_date, errors='coerce')
        if pd.notna(edt): out = out[out[DATE_COL] <= edt]
    return out

def _finalize_df(df: pd.DataFrame) -> pd.DataFrame:
    # mantém a ordenação pela data padrão
    if DATE_COL in df.columns:
        df = df.sort_values(by=[DATE_COL], ascending=False, na_position='last')

    # formata TODAS as colunas de data conhecidas no novo formato
    for col in DATE_COLS:
        if col in df.columns and pd.api.types.is_datetime64_any_dtype(df[col]):
            df[col] = df[col].dt.strftime(OUTPUT_DATE_FMT)

    # normaliza NaN -> None
    return df.where(pd.notna(df), None)

def _filter_pipeline(data: List[Dict[str, Any]], payload: DataRequest) -> pd.DataFrame:
    if not data:
        return pd.DataFrame(columns=REQUIRED_COLS)
    df = pd.DataFrame(data)
    df = _ensure_required_cols(df)
    df = _parse_sheet_dates(df)
    df = _apply_canonical_filters(df, payload.filters)
    df = _apply_search(df, payload.search)
    df = _apply_date_range(df, payload.start_date, payload.end_date)
    return _finalize_df(df)

def _calc_options(df: pd.DataFrame) -> OptionsResponse:
    def vals(col: str) -> List[str]:
        if col not in df.columns: return []
        return sorted([v for v in df[col].dropna().astype(str).unique().tolist() if v != "None"])
    return OptionsResponse(
        brand=vals("Marca"),
        platform=vals("Plataforma"),
        insight_type=vals("Tipo de insight"),
        month=vals("Mês"),
    )

@app.get("/")
def read_root():
    return {"message": "Bem-vindo à página principal!"}

@app.post("/api/data", response_model=DataResponse)
def post_data(req: DataRequest = Body(...)):
    try:
        all_data = fetch_all_data()
        insights_rows = all_data.get('insights') or all_data.get('BANCO_INSIGHTS') or []
        dff = _filter_pipeline(insights_rows, req)

        marcas_rows = all_data.get('marcas') or all_data.get('MARCAS') or []
        plataformas_rows = all_data.get('plataformas') or all_data.get('PLATAFORMAS') or []
        brands_all = sorted({r.get('Marca') for r in marcas_rows if r.get('Marca')})
        platforms_all = sorted({r.get('Plataforma') for r in plataformas_rows if r.get('Plataforma')})

        return DataResponse(
            insights=dff.to_dict(orient='records'),
            brands=brands_all,
            platforms=platforms_all,
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Falha ao carregar dados: {e}")

@app.post("/api/options", response_model=OptionsResponse)
def post_options(req: DataRequest = Body(...)):
    try:
        all_data = fetch_all_data()
        insights_rows = all_data.get('insights') or all_data.get('BANCO_INSIGHTS') or []
        dff = _filter_pipeline(insights_rows, req)
        return _calc_options(dff)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Falha ao calcular opções: {e}")

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

@app.get("/api/getData", response_model=DataResponse)
def get_insights(
    filters: Optional[str] = Query(None, description='JSON de filtros. Ex: {"Marca":["Budweiser"]}'),
    search: Optional[str] = Query(None, description="Termo de busca."),
    start_date: Optional[str] = Query(None, description="YYYY-MM-DD."),
    end_date: Optional[str] = Query(None, description="YYYY-MM-DD.")
):
    try:
        parsed = json.loads(filters) if filters else {}

        def pick(key_canon: str, legacy_col: str):
            if key_canon in parsed and parsed[key_canon]: return parsed[key_canon]
            if legacy_col in parsed and parsed[legacy_col]:
                v = parsed[legacy_col]; return v if isinstance(v, list) else [v]
            return None

        f = FilterPayload(
            brand=pick("brand", "Marca"),
            platform=pick("platform", "Plataforma"),
            insight_type=pick("insight_type", "Tipo de insight"),
            month=pick("month", "Mês"),
        )
        req = DataRequest(filters=f, search=search, start_date=start_date, end_date=end_date)

        all_data = fetch_all_data()
        insights_rows = all_data.get('insights') or all_data.get('BANCO_INSIGHTS') or []
        dff = _filter_pipeline(insights_rows, req)

        marcas_rows = all_data.get('marcas') or all_data.get('MARCAS') or []
        plataformas_rows = all_data.get('plataformas') or all_data.get('PLATAFORMAS') or []
        brands_all = sorted({r.get('Marca') for r in marcas_rows if r.get('Marca')})
        platforms_all = sorted({r.get('Plataforma') for r in plataformas_rows if r.get('Plataforma')})

        return DataResponse(
            insights=dff.to_dict(orient='records'),
            brands=brands_all,
            platforms=platforms_all,
        )
    except json.JSONDecodeError:
        raise HTTPException(status_code=400, detail="Formato de filtros inválido.")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Falha ao processar: {e}")

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000, reload=True)