import streamlit as st
import pandas as pd
import numpy as np
import datetime as dt
import os
import uuid
import json
import tempfile
import plotly.express as px
import plotly.graph_objects as go
import calendar
import base64
import urllib.parse
import requests

# =========================================================
# 1. CONFIGURAÇÕES GERAIS E CONSTANTES
# =========================================================
st.set_page_config(
    page_title="DASH - Gestão de Masterclass | IAM",
    page_icon="📊",
    layout="wide"
)

TENANT_ID = "IAM"

DATA_FILE   = "masterclass_oficial.csv"
METAS_FILE  = "metas_oficial.csv"
CONFIG_FILE = "config.json"
AUDIT_FILE  = "audit_log.csv"

CSV_ENCODING = "utf-8-sig"

# Paleta IAM refinada
IAM_ROYAL = "#0034B8"
IAM_NAVY  = "#002279"
IAM_BG    = "#EEF3F8"
IAM_CARD  = "#FFFFFF"
IAM_G100  = "#F8FAFC"
IAM_G200  = "#E2E8F0"
IAM_G300  = "#CBD5E1"
IAM_G500  = "#64748B"
IAM_G700  = "#334155"
IAM_BLACK = "#0F172A"
IAM_SUCCESS = "#16A34A"
IAM_DANGER  = "#DC2626"
IAM_INFO    = "#0EA5E9"
IAM_WARNING = "#F59E0B"

MASTERCLASS_COLS = ["ID", "Tenant", "Data", "Polo", "Cidade", "Sala", "Inscricoes", "Palestrante"]
METAS_COLS       = ["ID", "Tenant", "Mes", "Ano", "Polo", "Quantidade_MC", "Meta_Vendas_Por_MC", "Meta_Inscricoes"]
AUDIT_COLS       = ["Timestamp", "Tenant", "Acao", "Detalhes", "Registros"]
COMISSAO_RULE_KEYS = ["f1_max", "f1_val", "f2_max", "f2_val", "f3_max", "f3_val", "f4_max", "f4_val", "f5_min", "f5_val"]

DEFAULT_POLOS = [
    "Americana", "Balneário Camboriú", "Londrina", "Belo Horizonte",
    "Porto Alegre", "Fortaleza", "Goiânia", "Vitória"
]

DEFAULT_CIDADES_REL = {
    "Americana": [
        "CAMPINAS", "SOROCABA", "JUNDIAÍ", "PIRACICABA", "LIMEIRA", "INDAIATUBA",
        "AMERICANA", "MOGIGUAÇU", "VALINHOS", "PAULÍNIA", "JAGUARIÚNA", "BRAGANÇA PAULISTA",
        "ITATIBA", "OSASCO", "GUARULHOS", "RIBEIRÃO PRETO", "FRANCA", "SÃO JOSÉ DO RIO PRETO",
        "ARARAQUARA", "SANTOS", "PRAIA GRANDE", "GUARUJÁ", "SANTO ANDRÉ", "SÃO BERNARDO DO CAMPO",
        "SÃO JOSÉ DOS CAMPOS", "JACAREÍ", "TAUBATÉ"
    ],
    "Londrina": [
        "LONDRINA", "PRESIDENTE PRUDENTE/SP", "MARÍLIA/SP", "BAURU/SP", "MARINGÁ", "CAMPO MOURÃO"
    ],
    "Belo Horizonte": [
        "BH", "SETE LAGOAS", "VESPASIANO", "DIVINÓPOLIS"
    ],
    "Balneário Camboriú": [
        "B. CAMBORIÚ", "FLORIANÓPOLIS", "ITAPEMA", "ITAJAÍ", "BRUSQUE", "BLUMENAU",
        "JOINVILLE", "JARAGUÁ DO SUL", "PALHOÇA"
    ],
    "Porto Alegre": [
        "PORTO ALEGRE", "CANOAS", "GRAVATAÍ", "NOVO HAMBURGO", "GRAMADO", "CAXIAS DO SUL",
        "BENTO GONÇALVES", "LAJEADO", "CRICIÚMA/SC", "TUBARÃO/SC"
    ],
    "Vitória": [
        "VITÓRIA", "SERRA", "VILA VELHA", "CARIACICA", "COLATINA", "LINHARES"
    ],
    "Fortaleza": [
        "FORTALEZA"
    ],
    "Goiânia": [
        "GOIÂNIA", "APARECIDA DE GOIÂNIA", "ANÁPOLIS", "BRASÍLIA/DF", "SENADOR CANEDO",
        "RIO VERDE", "TRINDADE", "CALDAS NOVAS"
    ]
}

DEFAULT_PALESTRANTES = [
    "Guilherme", "Vinicius", "Pedro", "Palestrante", "Michelle", "Natalia", "Cassi",
    "Jaquiele", "Ygor", "Bia", "Ezequiel", "Isabella"
]

# =========================================================
# 1.5. SUPABASE CONFIGURATION
# =========================================================
SUPABASE_URL = "https://omjqpblyjzbaryrsbhco.supabase.co"
SUPABASE_KEY = "sb_publishable_SrbvclPeSfhqSYGY-pufdw_CXG_bhDk"
TENANT_UUID = "00000000-0000-0000-0000-000000000001"

def _sb_headers():
    return {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "return=representation"
    }

def _sb_get(table, params=None):
    """GET request to Supabase REST API"""
    r = requests.get(f"{SUPABASE_URL}/rest/v1/{table}", headers=_sb_headers(), params=params or {})
    r.raise_for_status()
    return r.json()

def _sb_post(table, data):
    """POST (insert) to Supabase REST API"""
    h = _sb_headers()
    r = requests.post(f"{SUPABASE_URL}/rest/v1/{table}", headers=h, json=data)
    r.raise_for_status()
    return r.json()

def _sb_patch(table, data, params):
    """PATCH (update) to Supabase REST API"""
    h = _sb_headers()
    r = requests.patch(f"{SUPABASE_URL}/rest/v1/{table}", headers=h, json=data, params=params)
    r.raise_for_status()
    return r.json()

def _sb_delete(table, params):
    """DELETE from Supabase REST API"""
    r = requests.delete(f"{SUPABASE_URL}/rest/v1/{table}", headers=_sb_headers(), params=params)
    r.raise_for_status()
    return r.json() if r.text else []

@st.cache_data(ttl=300)
def _load_lookup_tables():
    """Load polo, cidade, and palestrante lookup tables for ID mapping"""
    # Polos: nome -> id
    polos_data = _sb_get("polos", {"tenant_id": f"eq.{TENANT_UUID}", "select": "id,nome"})
    polos = {r["nome"]: r["id"] for r in polos_data}

    # Cidades: (polo_id, nome) -> id
    cidades_raw = _sb_get("cidades", {"select": "id,nome,polo_id"})
    cidades = {}
    for c in cidades_raw:
        cidades[(c["polo_id"], c["nome"])] = c["id"]

    # Palestrantes: nome -> id
    pals_data = _sb_get("palestrantes", {"tenant_id": f"eq.{TENANT_UUID}", "select": "id,nome"})
    pals = {r["nome"]: r["id"] for r in pals_data}

    return polos, cidades, pals

# =========================================================
# 2. UI/UX E DESIGN SYSTEM
# =========================================================
def render_html(html_str):
    """Remove quebras de linha que confundem o parser do Streamlit."""
    clean_html = html_str.replace('\n', ' ')
    st.markdown(clean_html, unsafe_allow_html=True)

IAM_THEME_CSS = f"""
<style>
:root {{
  --iam-royal: {IAM_ROYAL};
  --iam-navy: {IAM_NAVY};
  --iam-bg: {IAM_BG};
  --iam-card: {IAM_CARD};
  --iam-g100: {IAM_G100};
  --iam-g200: {IAM_G200};
  --iam-g300: {IAM_G300};
  --iam-g500: {IAM_G500};
  --iam-g700: {IAM_G700};
  --iam-black: {IAM_BLACK};
  --iam-success: {IAM_SUCCESS};
  --iam-danger: {IAM_DANGER};
  --iam-warning: {IAM_WARNING};
  --iam-info: {IAM_INFO};
}}

html, body, [class*="css"] {{
  font-family: 'Inter', system-ui, -apple-system, sans-serif !important;
  color: var(--iam-black);
}}

body, .stApp {{
  background: linear-gradient(180deg, #F6F9FC 0%, var(--iam-bg) 100%);
}}

header[data-testid="stHeader"], 
div[data-testid="stToolbar"], 
div[data-testid="stDecoration"], 
[data-testid="stSidebar"] {{ 
  display: none !important; 
}}

section.main {{
  width: 100% !important;
}}

.block-container {{
  max-width: 1560px !important;
  padding-top: 1.5rem !important;
  padding-bottom: 2rem !important;
  padding-left: 2rem !important;
  padding-right: 2rem !important;
}}

/* ========================================================================= */
/* ESTILO DOS CONTAINERS NATIVOS (PAINÉIS E FORMS)                           */
/* Substitui as divs HTML antigas garantindo segurança nativa no Streamlit   */
/* ========================================================================= */
[data-testid="stVerticalBlockBorderWrapper"],
[data-testid="stForm"] {{
    background-color: #ffffff !important;
    border: 1px solid rgba(203, 213, 225, 0.75) !important;
    border-radius: 20px !important;
    box-shadow: 0 10px 30px rgba(2, 12, 43, 0.05) !important;
    padding: 1.25rem !important;
    transition: transform 0.2s ease, box-shadow 0.2s ease;
}}

[data-testid="stVerticalBlockBorderWrapper"]:hover,
[data-testid="stForm"]:hover {{
    box-shadow: 0 14px 32px rgba(0, 52, 184, 0.08) !important;
}}

/* Supressão de margens internas */
[data-testid="stVerticalBlockBorderWrapper"] > div[data-testid="stVerticalBlock"] > div.element-container {{
    margin-bottom: 0 !important;
}}

[data-testid="stVerticalBlockBorderWrapper"] [data-testid="stForm"] {{
    border: none !important;
    box-shadow: none !important;
    padding: 0 !important;
    background: transparent !important;
}}

div[data-testid="stHorizontalBlock"] {{
  gap: 0.8rem !important;
}}

/* ELEMENTOS GERAIS DE UI E TOOLTIPS */
[data-testid="stWidgetLabel"] p {{
    font-size: 11px !important;
    font-weight: 800 !important;
    color: var(--iam-g500) !important;
    text-transform: uppercase;
    letter-spacing: 0.7px;
    margin-bottom: 3px !important;
}}

div[data-baseweb="tab-list"] {{
    gap: 8px; border-bottom: 1px solid #E2E8F0; margin-bottom: 14px; padding-bottom: 0;
}}

button[data-baseweb="tab"] {{
    background-color: transparent !important; padding: 9px 14px !important;
    font-size: 13px !important; font-weight: 700 !important; color: var(--iam-g500) !important;
    border-radius: 0 !important; border: none !important; border-bottom: 2px solid transparent !important;
    transition: all 0.2s ease !important;
}}
button[data-baseweb="tab"][aria-selected="true"] {{
    color: var(--iam-royal) !important; border-bottom: 2px solid var(--iam-royal) !important;
}}

div[data-baseweb="select"] > div,
div[data-baseweb="input"] > div,
div[data-baseweb="base-input"] > div,
[data-testid="stDateInput"] > div > div,
[data-testid="stNumberInput"] > div > div {{
  border-radius: 12px !important; border: 1px solid #D7E0EA !important;
  background-color: #ffffff !important; min-height: 42px !important;
  box-shadow: 0 2px 6px rgba(15,23,42,0.02) !important; transition: all 0.2s ease !important;
}}

button[kind="primary"], div[data-testid="stFormSubmitButton"] > button, div[data-testid="stButton"] > button[kind="primary"] {{
  background: linear-gradient(135deg, var(--iam-royal), var(--iam-navy)) !important;
  color: #ffffff !important; border: none !important; border-radius: 12px !important;
  min-height: 42px !important; padding: 0.58rem 1rem !important; font-weight: 800 !important;
  font-size: 13px !important; letter-spacing: 0.25px !important; box-shadow: 0 8px 20px rgba(0, 52, 184, 0.20) !important;
}}

button[kind="secondary"] {{
  background-color: #ffffff !important; color: var(--iam-black) !important;
  border: 1px solid #D7E0EA !important; border-radius: 12px !important;
  font-weight: 700 !important; font-size: 13px !important;
}}

/* Componente Tooltip Customizado */
.iam-tooltip {{
  position: relative; display: inline-flex; align-items: center; justify-content: center;
  cursor: help; color: var(--iam-g700); background: var(--iam-g200); border-radius: 50%;
  width: 15px; height: 15px; font-size: 10px; font-weight: 900; margin-left: 8px;
}}
.iam-tooltip .tooltip-text {{
  visibility: hidden; width: 240px; background-color: var(--iam-black); color: #fff;
  text-align: left; border-radius: 8px; padding: 8px 12px; position: absolute;
  z-index: 100; bottom: 140%; left: 50%; transform: translateX(-50%); opacity: 0;
  transition: opacity 0.3s; font-size: 11px; font-weight: 500; line-height: 1.4;
  box-shadow: 0 8px 24px rgba(0,0,0,0.2); white-space: normal; pointer-events: none;
}}
.iam-tooltip .tooltip-text::after {{
  content: ""; position: absolute; top: 100%; left: 50%; transform: translateX(-50%);
  border-width: 5px; border-style: solid; border-color: var(--iam-black) transparent transparent transparent;
}}
.iam-tooltip:hover .tooltip-text {{
  visibility: visible; opacity: 1;
}}

/* NAVEGAÇÃO SUPERIOR */
.topbar {{
  width: 100%; box-sizing: border-box; background: linear-gradient(135deg, #02112F 0%, var(--iam-navy) 72%);
  border-radius: 24px; padding: 14px 18px; margin-bottom: 14px; box-shadow: 0 12px 30px rgba(0, 34, 121, 0.18);
  color: #fff; display: flex; justify-content: space-between; align-items: center; gap: 14px;
}}
.brand {{ display: flex; align-items: center; gap: 12px; }}
.brand-title {{ font-weight: 850; font-size: 19px; line-height: 1.1; letter-spacing: -0.4px; }}
.brand-sub {{ font-size: 12px; opacity: 0.78; font-weight: 500; letter-spacing: 0.2px; }}
.topbar-right {{ display: flex; gap: 8px; flex-wrap: wrap; justify-content: flex-end; }}

.chip {{
  display: inline-flex; align-items: center; gap: 8px; padding: 8px 13px; border-radius: 999px;
  font-weight: 700; font-size: 12px; background: rgba(255,255,255,0.10); border: 1px solid rgba(255,255,255,0.12);
  backdrop-filter: blur(8px); white-space: nowrap;
}}
.chip-dot {{ width: 8px; height: 8px; border-radius: 50%; }}

/* RADIO NAVIGATION OVERRIDE */
div[role="radiogroup"] label > div:first-child {{ display: none !important; }}
div[role="radiogroup"] {{
    background: #ffffff; border: 1px solid #E2E8F0; padding: 6px; border-radius: 16px;
    display: inline-flex; gap: 8px; margin-bottom: 16px; box-shadow: 0 4px 15px rgba(0,0,0,0.03);
}}
div[role="radiogroup"] > label {{
    background-color: transparent !important; border: none !important; box-shadow: none !important;
    padding: 10px 22px !important; border-radius: 12px; margin: 0; cursor: pointer; transition: all 0.25s ease !important;
}}
div[role="radiogroup"] > label p {{ font-weight: 700 !important; font-size: 14px !important; color: var(--iam-g500) !important; margin: 0 !important; }}
div[role="radiogroup"] > label:hover {{ background-color: var(--iam-g100) !important; }}
div[role="radiogroup"] > label:has(input:checked) {{ background: linear-gradient(135deg, var(--iam-royal), var(--iam-navy)) !important; box-shadow: 0 6px 16px rgba(0, 52, 184, 0.25) !important; }}
div[role="radiogroup"] > label:has(input:checked) * {{ color: #ffffff !important; font-weight: 850 !important; }}

/* COMPONENTES HTML MONOLÍTICOS (Apenas Miolo dos Painéis) */
.section-wrap {{ margin: 6px 0 10px 0; }}
.section-head {{ display: flex; justify-content: space-between; align-items: end; gap: 10px; padding-bottom: 8px; margin-bottom: 12px; }}
.section-title {{ margin: 0; font-size: 20px; font-weight: 850; color: var(--iam-black); letter-spacing: -0.4px; }}
.section-sub {{ margin: 3px 0 0 0; font-size: 13px; color: var(--iam-g500); font-weight: 500; }}

.panel-head {{ display: flex; justify-content: space-between; align-items: end; gap: 10px; padding-bottom: 8px; margin-bottom: 14px; border-bottom: 1px solid #EEF2F7; }}
.panel-title {{ margin: 0; font-size: 15px; font-weight: 850; color: var(--iam-black); letter-spacing: -0.3px; display:flex; align-items:center; }}
.panel-sub {{ margin: 3px 0 0 0; font-size: 11px; color: var(--iam-g500); font-weight: 600; }}

/* KPIs */
.kpi-grid {{ display: grid; grid-template-columns: repeat(6, minmax(0, 1fr)); gap: 12px; margin-bottom: 16px; }}
.kpi {{ background: #ffffff; border: 1px solid rgba(203, 213, 225, 0.75); border-radius: 18px; padding: 16px; box-shadow: 0 10px 30px rgba(2, 12, 43, 0.06); transition: transform 0.2s; position: relative; overflow: hidden; }}
.kpi::before {{ content: ''; position: absolute; top: 0; left: 0; width: 3px; height: 100%; background: linear-gradient(180deg, var(--iam-royal), var(--iam-navy)); }}
.kpi:hover {{ transform: translateY(-2px); }}
.kpi-label {{ font-size: 11px; font-weight: 800; color: var(--iam-g500); text-transform: uppercase; letter-spacing: 0.8px; margin-bottom: 8px; }}
.kpi-value {{ font-size: 30px; font-weight: 900; color: var(--iam-black); line-height: 1; letter-spacing: -1px; margin-bottom: 6px; }}
.small-muted {{ color: var(--iam-g500); font-size: 11px; font-weight: 600; line-height: 1.35; }}

.kpi.kpi-accent {{ background: linear-gradient(135deg, var(--iam-royal), var(--iam-navy)); }}
.kpi.kpi-accent::before {{ background: rgba(255,255,255,0.25); }}
.kpi.kpi-accent .kpi-label {{ color: #BFDBFE; }}
.kpi.kpi-accent .kpi-value {{ color: #ffffff; }}
.kpi.kpi-accent .small-muted {{ color: #DBEAFE; }}

/* Blocos Internos */
.metric-slab {{ display: flex; justify-content: space-between; align-items: center; padding: 12px 14px; background: #ffffff; border-radius: 14px; border: 1px solid #E2E8F0; margin-bottom: 8px; }}
.metric-slab strong {{ font-size: 18px; }}
.metric-slab.meta {{ background: #F8FAFC; }}
.metric-slab.real {{ background: #EFF6FF; border-color: #BFDBFE; }}
.metric-slab.gap {{ background: #FEF2F2; border-color: #FECACA; }}

.mini-list {{ display: flex; flex-direction: column; gap: 8px; }}
.mini-item {{ display: flex; justify-content: space-between; align-items: center; padding: 11px 12px; background: #ffffff; border-radius: 13px; border: 1px solid #E2E8F0; transition: transform 0.2s; }}
.mini-item:hover {{ transform: translateX(2px); }}
.mini-item-left {{ display: flex; align-items: center; gap: 10px; }}
.mini-rank {{ font-weight: 900; color: var(--iam-royal); background: #E0F2FE; padding: 6px 9px; border-radius: 8px; font-size: 11px; min-width: 30px; text-align: center; }}
.mini-name {{ font-weight: 850; color: var(--iam-black); font-size: 12px; text-transform: uppercase; letter-spacing: 0.35px; }}
.mini-val {{ font-weight: 900; color: var(--iam-black); font-size: 16px; }}
.mini-sub {{ font-size: 10px; color: var(--iam-g500); font-weight: 800; text-transform: uppercase; letter-spacing: 0.45px; }}

.rc-container {{ display: flex; flex-direction: column; gap: 8px; }}
.rc-card {{ background: #ffffff; border: 1px solid #E2E8F0; border-radius: 14px; padding: 12px 14px; display: flex; align-items: center; justify-content: space-between; gap: 12px; transition: transform 0.2s; }}
.rc-card:hover {{ transform: translateY(-1px); }}
.rc-card.top-3 {{ border-left: 4px solid var(--iam-royal); }}

.rc-left {{ display: flex; align-items: center; gap: 10px; min-width: 135px; }}
.rc-rank {{ font-size: 16px; font-weight: 900; color: var(--iam-g500); width: 44px; text-align: center; }}
.rc-name {{ font-size: 13px; font-weight: 850; color: var(--iam-black); text-transform: uppercase; max-width: 160px; overflow: hidden; text-overflow: ellipsis; }}
.rc-metrics {{ display: flex; gap: 12px; justify-content: flex-end; align-items: center; flex: 1; }}
.rc-metric {{ display: flex; flex-direction: column; align-items: center; gap: 1px; min-width: 64px; }}
.rc-metric-lbl {{ font-size: 9px; font-weight: 800; color: var(--iam-g500); text-transform: uppercase; }}
.rc-metric-val {{ font-size: 15px; font-weight: 900; color: var(--iam-black); }}
.rc-right {{ width: 68px; display: flex; justify-content: flex-end; }}
.rc-badge {{ padding: 7px 10px; border-radius: 10px; font-weight: 900; font-size: 13px; text-align: center; width: 100%; }}

a.clickable-name {{
    color: var(--iam-black) !important;
    text-decoration: none !important;
    border-bottom: 1px dashed var(--iam-g300);
    transition: all 0.2s ease;
}}
a.clickable-name:hover {{
    color: var(--iam-royal) !important;
    border-bottom: 1px solid var(--iam-royal) !important;
}}

div[data-testid="stPlotlyChart"] {{ background: transparent !important; border: none !important; padding: 0 !important; margin-top: -10px; }}

/* ========================================================================= */
/* MÓDULO AGENDA (CALENDÁRIO)                                                */
/* ========================================================================= */
.calendar-container {{ width: 100%; border: 1px solid #E2E8F0; border-radius: 16px; overflow: hidden; background: #ffffff; box-shadow: 0 4px 15px rgba(0,0,0,0.03); }}
.calendar-header {{ display: grid; grid-template-columns: repeat(7, 1fr); background: #F8FAFC; border-bottom: 1px solid #E2E8F0; text-align: center; font-weight: 850; font-size: 11px; color: var(--iam-g500); text-transform: uppercase; padding: 12px 0; letter-spacing: 0.5px; }}
.calendar-grid {{ display: grid; grid-template-columns: repeat(7, 1fr); }}
.calendar-day {{ border-right: 1px solid #EEF2F7; border-bottom: 1px solid #EEF2F7; padding: 8px; background: #ffffff; display: flex; flex-direction: column; height: 150px; overflow: hidden; transition: background 0.2s; }}
.calendar-day.other-month {{ background: #F8FAFC; opacity: 0.6; }}
.calendar-day:nth-child(7n) {{ border-right: none; }}
.day-number-wrap {{ display: flex; justify-content: flex-end; margin-bottom: 6px; }}
.day-number {{ font-weight: 800; font-size: 12px; color: var(--iam-g700); border-radius: 50%; width: 24px; height: 24px; display: inline-flex; align-items: center; justify-content: center; }}
.day-number.today {{ background: linear-gradient(135deg, var(--iam-royal), var(--iam-navy)); color: #ffffff; box-shadow: 0 4px 10px rgba(0, 52, 184, 0.3); }}
.day-events-wrapper {{ flex: 1; overflow-y: auto; display: flex; flex-direction: column; gap: 5px; padding-right: 4px; }}
.day-events-wrapper::-webkit-scrollbar {{ width: 4px; }}
.day-events-wrapper::-webkit-scrollbar-track {{ background: transparent; }}
.day-events-wrapper::-webkit-scrollbar-thumb {{ background: #CBD5E1; border-radius: 4px; }}
.cal-event {{ padding: 6px 8px; border-radius: 6px; font-size: 10px; line-height: 1.3; display: flex; flex-direction: column; gap: 2px; cursor: pointer; transition: transform 0.1s; border-left: 3px solid transparent; }}
.cal-event:hover {{ transform: scale(1.02); box-shadow: 0 4px 8px rgba(0,0,0,0.05); }}
.cal-event-title {{ font-weight: 850; font-size: 10px; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; text-transform: uppercase; }}
.cal-event-sub {{ font-weight: 600; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; font-size: 9px; opacity: 0.85; }}

@media (max-width: 1400px) {{ .kpi-grid {{ grid-template-columns: repeat(3, minmax(0, 1fr)); }} }}
@media (max-width: 900px) {{
  .kpi-grid {{ grid-template-columns: repeat(2, minmax(0, 1fr)); }}
  .topbar {{ flex-direction: column; align-items: flex-start; }}
  .topbar-right {{ width: 100%; justify-content: flex-start; }}
  .calendar-header {{ font-size: 9px; }}
  .cal-event-title, .cal-event-sub {{ font-size: 9px; }}
}}
</style>
"""
render_html(IAM_THEME_CSS)

# =========================================================
# 3. HELPERS E COMPONENTES VISUAIS
# =========================================================
def fmt_br(num):
    return f"{int(num):,}".replace(",", ".")

def fmt_br_float(num, decimals=1):
    return f"{num:.{decimals}f}".replace(".", ",")

def fmt_br_money(num):
    return f"R$ {num:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")

def section_header(title: str, subtitle: str = "", right_text: str = ""):
    html = f"""<div class="section-wrap">
    <div class="section-head">
        <div><h3 class="section-title">{title}</h3><p class="section-sub">{subtitle}</p></div>
        <div class="section-sub" style="text-align:right;">{right_text}</div>
    </div></div>"""
    render_html(html)

def render_empty_state(msg="Nenhum dado encontrado para o período.", icon="📭"):
    """Renderiza uma tela vazia elegante com segurança para preencher os cartões."""
    return f"""
    <div style="text-align:center; padding: 30px 15px; color: var(--iam-g500); background: #F8FAFC; border-radius: 14px; border: 1px dashed #CBD5E1; display:flex; flex-direction:column; align-items:center; justify-content:center; height: 100%; min-height: 150px; margin-bottom: 8px;">
        <div style="font-size: 28px; margin-bottom: 8px; opacity: 0.7;">{icon}</div>
        <div style="font-weight: 600; font-size: 13px;">{msg}</div>
    </div>
    """

def ok_modal(msg="Realizado com sucesso"):
    st.session_state["_ok_open"] = True
    st.session_state["_ok_msg"] = msg

def render_ok_modal():
    if not st.session_state.get("_ok_open"):
        return
    msg = st.session_state.get("_ok_msg", "Realizado com sucesso")

    @st.dialog("✅ Sucesso")
    def _dlg():
        st.success(msg)
        if st.button("Fechar", type="primary", width='stretch'):
            st.session_state["_ok_open"] = False
            st.rerun()
    _dlg()

def get_image_base64(filepath):
    try:
        with open(filepath, "rb") as img_file:
            return base64.b64encode(img_file.read()).decode()
    except Exception:
        return ""

def topbar(period_label: str, status_label: str):
    dot_color = IAM_DANGER if status_label == "CRÍTICO" else IAM_SUCCESS
    img_b64 = get_image_base64("IAM.png")
    if img_b64:
        logo_html = f'<img src="data:image/png;base64,{img_b64}" alt="IAM Logo" style="max-height: 26px; width: auto; object-fit: contain;">'
    else:
        logo_html = '<span translate="no" class="notranslate" style="font-weight: 900; font-size: 22px; color: var(--iam-royal); letter-spacing: -1px; font-family: sans-serif;">IAM</span>'

    html = f"""<div class="topbar">
  <div class="brand">
    <div class="brand-logo" style="display:flex; align-items:center; justify-content:center; background:#ffffff; padding:6px 14px; border-radius:12px; height: 40px; min-width: 80px; box-shadow: 0 4px 12px rgba(0,0,0,0.10);">
        {logo_html}
    </div>
    <div style="display:flex; flex-direction:column; gap:2px; min-width:0;">
      <div class="brand-title">Métricas da Masterclass</div>
      <div class="brand-sub">Painel executivo com visão comercial, metas e performance</div>
    </div>
  </div>
  <div class="topbar-right">
    <span class="chip">Período:&nbsp;<b>{period_label}</b></span>
    <span class="chip"><span class="chip-dot" style="background: {dot_color}; box-shadow: 0 0 8px {dot_color}88;"></span>Status:&nbsp;<b>{status_label}</b></span>
  </div>
</div>"""
    render_html(html)

def apply_plotly_theme(fig):
    fig.update_layout(
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font=dict(family="Inter, system-ui", size=12, color=IAM_G700),
        margin=dict(l=8, r=8, t=16, b=12),
        height=320, 
        xaxis=dict(showgrid=False, zeroline=False, tickfont=dict(color=IAM_G500)),
        yaxis=dict(showgrid=True, gridcolor="#EEF2F7", zeroline=False, tickfont=dict(color=IAM_G500)),
        legend=dict(title=None, bgcolor="rgba(255,255,255,0.88)", bordercolor="#E2E8F0", borderwidth=1, orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        colorway=[IAM_ROYAL, "#38BDF8", "#10B981", "#F59E0B"]
    )
    return fig

# =========================================================
# 4. CAMADA DE DADOS
# =========================================================
def _atomic_write_csv(path: str, df: pd.DataFrame) -> None:
    """Deprecated: CSV writing no longer used with Supabase backend"""
    pass

def _read_csv_safe(path: str) -> pd.DataFrame:
    """Deprecated: CSV reading no longer used with Supabase backend"""
    return pd.DataFrame()

def load_config() -> dict:
    """Load configuration from Supabase database"""
    default_comissao = {
        "f1_max": 7, "f1_val": 0.0,
        "f2_max": 12, "f2_val": 60.0,
        "f3_max": 16, "f3_val": 70.0,
        "f4_max": 20, "f4_val": 80.0,
        "f5_min": 21,
        "f5_val": 100.0,
    }

    try:
        # Fetch tenant info
        tenant_data = _sb_get("tenants", {"id": f"eq.{TENANT_UUID}", "select": "nome,produto"})
        if not tenant_data:
            return {
                "empresa": "IAM",
                "produto": "",
                "polos": DEFAULT_POLOS,
                "cidades_rel": DEFAULT_CIDADES_REL,
                "palestrantes": DEFAULT_PALESTRANTES,
                "regras_comissao_default": default_comissao,
                "regras_comissao_periodo": {},
                "regras_comissao_historico": [],
            }

        tenant = tenant_data[0]

        # Fetch polos
        polos_data = _sb_get("polos", {"tenant_id": f"eq.{TENANT_UUID}", "select": "id,nome"})
        polos = sorted([p["nome"] for p in polos_data])

        # Fetch cities grouped by polo
        cidades_raw = _sb_get("cidades", {"select": "id,nome,polo_id"})
        cidades_rel = {}
        for p in polos:
            polo_id_match = next((p_data["id"] for p_data in polos_data if p_data["nome"] == p), None)
            if polo_id_match:
                cidades_rel[p] = sorted([c["nome"] for c in cidades_raw if c["polo_id"] == polo_id_match])
            else:
                cidades_rel[p] = []

        # Fetch palestrantes
        pals_data = _sb_get("palestrantes", {"tenant_id": f"eq.{TENANT_UUID}", "select": "id,nome"})
        palestrantes = sorted([p["nome"] for p in pals_data])

        # Fetch default commission rules
        regras_default_data = _sb_get("regras_comissao_default", {"tenant_id": f"eq.{TENANT_UUID}", "select": "*"})
        regras_comissao_default = default_comissao.copy()
        if regras_default_data:
            r = regras_default_data[0]
            for key in COMISSAO_RULE_KEYS:
                if key in r:
                    regras_comissao_default[key] = r[key]

        # Fetch historical commission rules
        regras_hist_data = _sb_get("regras_comissao_historico", {"tenant_id": f"eq.{TENANT_UUID}", "select": "*"})
        regras_comissao_historico = []
        for r in regras_hist_data:
            item = {
                "id": r.get("id"),
                "data_inicio": r.get("data_inicio"),
                "created_at": r.get("created_at", ""),
            }
            for key in COMISSAO_RULE_KEYS:
                if key in r:
                    item[key] = r[key]
            regras_comissao_historico.append(item)

        return {
            "empresa": tenant.get("nome", "IAM"),
            "produto": tenant.get("produto", ""),
            "polos": polos,
            "cidades_rel": cidades_rel,
            "palestrantes": palestrantes,
            "regras_comissao_default": regras_comissao_default,
            "regras_comissao_periodo": {},
            "regras_comissao_historico": regras_comissao_historico,
        }
    except Exception as e:
        st.error(f"Erro ao carregar configuração do Supabase: {str(e)}")
        return {
            "empresa": "IAM",
            "produto": "",
            "polos": DEFAULT_POLOS,
            "cidades_rel": DEFAULT_CIDADES_REL,
            "palestrantes": DEFAULT_PALESTRANTES,
            "regras_comissao_default": default_comissao,
            "regras_comissao_periodo": {},
            "regras_comissao_historico": [],
        }

def save_config(cfg: dict) -> None:
    """Save configuration changes to Supabase database"""
    try:
        # Get current polos from DB
        polos_data = _sb_get("polos", {"tenant_id": f"eq.{TENANT_UUID}", "select": "id,nome"})
        existing_polos = {p["nome"]: p["id"] for p in polos_data}

        # Add new polos
        for polo_name in cfg.get("polos", []):
            if polo_name not in existing_polos:
                result = _sb_post("polos", {"tenant_id": TENANT_UUID, "nome": polo_name})
                if result:
                    existing_polos[polo_name] = result[0]["id"]

        # Get current cidades from DB
        cidades_raw = _sb_get("cidades", {"select": "id,nome,polo_id"})
        existing_cidades = {(c["polo_id"], c["nome"]): c["id"] for c in cidades_raw}

        # Add new cidades
        for polo_name, cidades_list in cfg.get("cidades_rel", {}).items():
            polo_id = existing_polos.get(polo_name)
            if polo_id:
                for cidade_name in cidades_list:
                    if (polo_id, cidade_name) not in existing_cidades:
                        _sb_post("cidades", {"polo_id": polo_id, "nome": cidade_name})

        # Get current palestrantes from DB
        pals_data = _sb_get("palestrantes", {"tenant_id": f"eq.{TENANT_UUID}", "select": "id,nome"})
        existing_pals = {p["nome"]: p["id"] for p in pals_data}

        # Add new palestrantes
        for pal_name in cfg.get("palestrantes", []):
            if pal_name not in existing_pals:
                _sb_post("palestrantes", {"tenant_id": TENANT_UUID, "nome": pal_name})

        # Update default commission rules
        regras_default_data = _sb_get("regras_comissao_default", {"tenant_id": f"eq.{TENANT_UUID}", "select": "id"})
        regra_update = {}
        for key in COMISSAO_RULE_KEYS:
            if key in cfg.get("regras_comissao_default", {}):
                regra_update[key] = cfg["regras_comissao_default"][key]

        if regras_default_data:
            # Update existing
            _sb_patch("regras_comissao_default", regra_update, {"tenant_id": f"eq.{TENANT_UUID}"})
        else:
            # Create new
            regra_update["tenant_id"] = TENANT_UUID
            _sb_post("regras_comissao_default", regra_update)

        # ---- Sync regras_comissao_historico ----
        hist_cfg = cfg.get("regras_comissao_historico", [])

        # Get existing historico IDs from DB
        existing_hist = _sb_get("regras_comissao_historico", {
            "tenant_id": f"eq.{TENANT_UUID}",
            "select": "id"
        })
        existing_hist_ids = {str(r["id"]) for r in existing_hist}

        # IDs that should exist according to cfg
        cfg_hist_ids = {str(item.get("id")) for item in hist_cfg if item.get("id")}

        # Delete rules that were removed from cfg
        ids_to_delete = existing_hist_ids - cfg_hist_ids
        for del_id in ids_to_delete:
            _sb_delete("regras_comissao_historico", {"id": f"eq.{del_id}"})

        # Upsert rules from cfg (insert new, update existing)
        for item in hist_cfg:
            item_id = str(item.get("id", ""))
            if not item_id:
                continue
            row = {
                "id": item_id,
                "tenant_id": TENANT_UUID,
                "data_inicio": str(item.get("data_inicio", "")),
            }
            for key in COMISSAO_RULE_KEYS:
                if key in item:
                    row[key] = item[key]

            if item_id in existing_hist_ids:
                # Update existing
                _sb_patch("regras_comissao_historico", row, {"id": f"eq.{item_id}"})
            else:
                # Insert new
                _sb_post("regras_comissao_historico", row)

        # Clear lookup caches after config change
        _load_lookup_tables.clear()

    except Exception as e:
        st.error(f"Erro ao salvar configuração: {str(e)}")

def audit(action: str, details: str = "", records: int = 0):
    """Log an action to the audit log in Supabase"""
    try:
        _sb_post("audit_log", {
            "tenant_id": TENANT_UUID,
            "acao": action,
            "detalhes": details,
            "registros": int(records),
        })
    except Exception as e:
        st.warning(f"Aviso: Não foi possível registrar no audit log: {str(e)}")

def validate_masterclass_df(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=MASTERCLASS_COLS)
    for c in MASTERCLASS_COLS:
        if c not in df.columns:
            df[c] = TENANT_ID if c == "Tenant" else ("" if c in ["ID", "Polo", "Cidade", "Palestrante"] else 0)
    df = df[MASTERCLASS_COLS].copy()
    df["ID"] = df["ID"].astype(str)
    missing_ids = df["ID"].str.strip() == ""
    if missing_ids.any():
        df.loc[missing_ids, "ID"] = [str(uuid.uuid4()) for _ in range(missing_ids.sum())]
    df["Data"] = pd.to_datetime(df["Data"], errors="coerce").dt.date
    df["Sala"] = pd.to_numeric(df["Sala"], errors="coerce").fillna(0).astype(int)
    df["Inscricoes"] = pd.to_numeric(df["Inscricoes"], errors="coerce").fillna(0).astype(int)
    
    # --- CORREÇÃO DE NOMENCLATURAS (TYPOS) AUTOMÁTICA ---
    if "Palestrante" in df.columns:
        df["Palestrante"] = df["Palestrante"].apply(lambda x: "Vinicius" if str(x).strip().upper() in ["VINÍCIO", "VINICIO"] else x)
    if "Cidade" in df.columns:
        df["Cidade"] = df["Cidade"].apply(lambda x: "LONDRINA" if str(x).strip().upper() == "LONDRESA" else ("ITU" if str(x).strip().upper() in ["UIT", "IUT"] else x))
    # ----------------------------------------------------

    df = df.dropna(subset=["Data"])
    df = df[df["Tenant"] == TENANT_ID]
    return df.sort_values(by=["Data", "Polo"]).reset_index(drop=True)

def validate_metas_df(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=METAS_COLS)
    for c in METAS_COLS:
        if c not in df.columns:
            df[c] = TENANT_ID if c == "Tenant" else ("" if c in ["ID", "Polo"] else 0)
    df = df[METAS_COLS].copy()
    df["ID"] = df["ID"].astype(str)
    missing_ids = df["ID"].str.strip() == ""
    if missing_ids.any():
        df.loc[missing_ids, "ID"] = [str(uuid.uuid4()) for _ in range(missing_ids.sum())]
    df["Mes"] = pd.to_numeric(df["Mes"], errors="coerce").fillna(1).astype(int)
    df["Ano"] = pd.to_numeric(df["Ano"], errors="coerce").fillna(2020).astype(int)
    df["Quantidade_MC"] = pd.to_numeric(df["Quantidade_MC"], errors="coerce").fillna(0).astype(int)
    df["Meta_Vendas_Por_MC"] = pd.to_numeric(df["Meta_Vendas_Por_MC"], errors="coerce").fillna(0).astype(int)
    df["Meta_Inscricoes"] = (df["Quantidade_MC"] * df["Meta_Vendas_Por_MC"]).astype(int)
    df = df[df["Tenant"] == TENANT_ID]
    return df.sort_values(by=["Ano", "Mes"]).drop_duplicates(subset=["Mes", "Ano", "Polo"], keep="last").reset_index(drop=True)

@st.cache_data(show_spinner=False, ttl=60)
def load_data():
    """Load masterclass and metas data from Supabase, returning DataFrames in legacy format"""
    try:
        # Load masterclass data with joined references
        mc_data = _sb_get("masterclass", {"tenant_id": f"eq.{TENANT_UUID}", "select": "*"})

        # Load lookup tables
        polos_lookup, cidades_lookup, pals_lookup = _load_lookup_tables()
        polo_by_id = {v: k for k, v in polos_lookup.items()}
        pals_by_id = {v: k for k, v in pals_lookup.items()}

        # Build masterclass dataframe
        mc_rows = []
        for row in mc_data:
            polo_name = polo_by_id.get(row.get("polo_id"), "")
            cidade_id = row.get("cidade_id")
            cidade_name = next((c_name for (p_id, c_name), c_id in cidades_lookup.items() if c_id == cidade_id), "")
            pal_name = pals_by_id.get(row.get("palestrante_id"), "")

            mc_rows.append({
                "ID": row.get("id", ""),
                "Tenant": TENANT_ID,
                "Data": row.get("data"),
                "Polo": polo_name,
                "Cidade": cidade_name,
                "Sala": row.get("sala", 0),
                "Inscricoes": row.get("inscricoes", 0),
                "Palestrante": pal_name,
            })

        mc = validate_masterclass_df(pd.DataFrame(mc_rows))
        if not mc.empty:
            mc["Data_dt"] = pd.to_datetime(mc["Data"])
            mc["Mes/Ano"] = mc["Data_dt"].dt.strftime("%m/%Y")
        else:
            mc = pd.DataFrame(columns=MASTERCLASS_COLS + ["Data_dt", "Mes/Ano"])

        # Load metas data
        mt_data = _sb_get("metas", {"tenant_id": f"eq.{TENANT_UUID}", "select": "*"})

        mt_rows = []
        for row in mt_data:
            polo_id = row.get("polo_id")
            polo_name = polo_by_id.get(polo_id, "")

            mt_rows.append({
                "ID": row.get("id", ""),
                "Tenant": TENANT_ID,
                "Mes": row.get("mes", 1),
                "Ano": row.get("ano", 2020),
                "Polo": polo_name,
                "Quantidade_MC": row.get("quantidade_mc", 0),
                "Meta_Vendas_Por_MC": row.get("meta_vendas_por_mc", 0),
                "Meta_Inscricoes": row.get("meta_inscricoes", 0),
            })

        mt = validate_metas_df(pd.DataFrame(mt_rows))

        return mc, mt
    except Exception as e:
        st.error(f"Erro ao carregar dados do Supabase: {str(e)}")
        return pd.DataFrame(columns=MASTERCLASS_COLS + ["Data_dt", "Mes/Ano"]), pd.DataFrame(columns=METAS_COLS)

def save_masterclass(df_in: pd.DataFrame):
    """Save masterclass data to Supabase, replacing existing records"""
    try:
        df_validated = validate_masterclass_df(df_in)

        # Load lookup tables for ID mapping
        polos_lookup, cidades_lookup, pals_lookup = _load_lookup_tables()

        # Delete existing masterclass records for this tenant
        _sb_delete("masterclass", {"tenant_id": f"eq.{TENANT_UUID}"})

        # Insert new records
        for idx, row in df_validated.iterrows():
            polo_id = polos_lookup.get(row["Polo"])
            cidade_id = cidades_lookup.get((polo_id, row["Cidade"])) if polo_id else None
            pal_id = pals_lookup.get(row["Palestrante"])

            if polo_id and cidade_id and pal_id:
                _sb_post("masterclass", {
                    "id": row["ID"],
                    "tenant_id": TENANT_UUID,
                    "data": str(row["Data"]),
                    "polo_id": polo_id,
                    "cidade_id": cidade_id,
                    "sala": int(row["Sala"]),
                    "inscricoes": int(row["Inscricoes"]),
                    "palestrante_id": pal_id,
                })

        audit("SAVE_MASTERCLASS", f"Saved {len(df_validated)} masterclass records", len(df_validated))
        load_data.clear()
    except Exception as e:
        st.error(f"Erro ao salvar Masterclass: {str(e)}")

def save_metas(df_in: pd.DataFrame):
    """Save metas data to Supabase, replacing existing records"""
    try:
        df_validated = validate_metas_df(df_in)

        # Load lookup tables for ID mapping
        polos_lookup, cidades_lookup, pals_lookup = _load_lookup_tables()

        # Delete existing metas records for this tenant
        _sb_delete("metas", {"tenant_id": f"eq.{TENANT_UUID}"})

        # Insert new records
        for idx, row in df_validated.iterrows():
            polo_id = polos_lookup.get(row["Polo"])

            if polo_id:
                _sb_post("metas", {
                    "id": row["ID"],
                    "tenant_id": TENANT_UUID,
                    "mes": int(row["Mes"]),
                    "ano": int(row["Ano"]),
                    "polo_id": polo_id,
                    "quantidade_mc": int(row["Quantidade_MC"]),
                    "meta_vendas_por_mc": int(row["Meta_Vendas_Por_MC"]),
                })

        audit("SAVE_METAS", f"Saved {len(df_validated)} meta records", len(df_validated))
        load_data.clear()
    except Exception as e:
        st.error(f"Erro ao salvar Metas: {str(e)}")

# =========================================================
# 5. REGRAS DE NEGÓCIO E COMISSÃO
# =========================================================
def metas_prorated_for_window(df_metas: pd.DataFrame, start_date: dt.date, end_date: dt.date, polos_filter: list) -> pd.DataFrame:
    if df_metas.empty:
        return pd.DataFrame(columns=["Polo", "Meta"])
    base = df_metas.copy()
    if polos_filter:
        base = base[base["Polo"].isin(polos_filter)]
    if base.empty:
        return pd.DataFrame(columns=["Polo", "Meta"])

    rows = []
    y1, m1 = start_date.year, start_date.month
    y2, m2 = end_date.year, end_date.month

    for ano in range(y1, y2 + 1):
        m_start = m1 if ano == y1 else 1
        m_end = m2 if ano == y2 else 12
        for mes in range(m_start, m_end + 1):
            last_day = calendar.monthrange(ano, mes)[1]
            m_s = dt.date(ano, mes, 1)
            m_e = dt.date(ano, mes, last_day)
            inter_start = max(m_s, start_date)
            inter_end = min(m_e, end_date)
            if inter_start > inter_end:
                continue
            weight = ((inter_end - inter_start).days + 1) / last_day
            mm = base[(base["Ano"] == ano) & (base["Mes"] == mes)].copy()
            if mm.empty:
                continue
            mm["Meta"] = (mm["Meta_Inscricoes"] * weight).round().astype(int)
            rows.append(mm[["Polo", "Meta"]])

    if not rows:
        return pd.DataFrame(columns=["Polo", "Meta"])
    return pd.concat(rows, ignore_index=True).groupby("Polo", as_index=False).agg(Meta=("Meta", "sum"))

def parse_iso_date(v):
    try:
        return dt.date.fromisoformat(str(v))
    except:
        return None

def regra_comissao_formatada(regra_raw: dict, regra_default: dict) -> dict:
    r = dict(regra_default)
    if isinstance(regra_raw, dict):
        for k in COMISSAO_RULE_KEYS:
            if k in regra_raw:
                r[k] = regra_raw[k]
    try:
        r["f1_max"] = int(r["f1_max"]); r["f2_max"] = int(r["f2_max"]); r["f3_max"] = int(r["f3_max"])
        r["f4_max"] = int(r["f4_max"]); r["f5_min"] = int(r["f5_min"])
        r["f1_val"] = float(r["f1_val"]); r["f2_val"] = float(r["f2_val"]); r["f3_val"] = float(r["f3_val"])
        r["f4_val"] = float(r["f4_val"]); r["f5_val"] = float(r["f5_val"])
    except:
        return dict(regra_default)
    return r

def obter_inicio_sistema(df_mc: pd.DataFrame) -> dt.date:
    if df_mc is None or df_mc.empty:
        return dt.date(2020, 1, 1)
    s = pd.to_datetime(df_mc.get("Data"), errors="coerce").dropna()
    if s.empty:
        return dt.date(2020, 1, 1)
    return s.min().date()

def regras_historico_normalizadas(cfg: dict) -> list:
    # Modelo de linha do tempo: cada regra tem apenas data_inicio.
    # A data_fim é derivada automaticamente: próximo início - 1 dia.
    por_inicio = {}
    for item in cfg.get("regras_comissao_historico", []):
        if not isinstance(item, dict):
            continue
        data_inicio = parse_iso_date(item.get("data_inicio"))
        if not data_inicio:
            continue
        regra_fmt = regra_comissao_formatada(item, cfg["regras_comissao_default"])
        candidato = {
            "id": str(item.get("id", str(uuid.uuid4()))),
            "data_inicio": data_inicio,
            "created_at": str(item.get("created_at", "")),
            "regra": regra_fmt,
        }
        atual = por_inicio.get(data_inicio)
        if not atual or candidato["created_at"] >= atual["created_at"]:
            por_inicio[data_inicio] = candidato

    regras = [por_inicio[k] for k in sorted(por_inicio.keys())]
    for i, r in enumerate(regras):
        proximo_inicio = regras[i + 1]["data_inicio"] if i + 1 < len(regras) else None
        r["data_fim"] = (proximo_inicio - dt.timedelta(days=1)) if proximo_inicio else None
    return regras

def regra_para_data(data_ref: dt.date, regra_default: dict, regras_hist: list, inicio_sistema: dt.date) -> dict:
    if not regras_hist:
        return {
            "regra": regra_comissao_formatada(regra_default, regra_default),
            "periodo_label": "Padrão do sistema",
            "origem": "Padrão",
            "regra_id": "default",
        }

    primeira = regras_hist[0]
    if data_ref < primeira["data_inicio"]:
        return {
            "regra": regra_comissao_formatada(regra_default, regra_default),
            "periodo_label": f"{inicio_sistema.strftime('%d/%m/%Y')} a {(primeira['data_inicio'] - dt.timedelta(days=1)).strftime('%d/%m/%Y')}",
            "origem": "Padrão",
            "regra_id": "default",
        }

    regra_escolhida = None
    for item in regras_hist:
        if item["data_inicio"] <= data_ref:
            regra_escolhida = item
        else:
            break

    if regra_escolhida:
        fim_txt = regra_escolhida["data_fim"].strftime("%d/%m/%Y") if regra_escolhida["data_fim"] else "em vigor"
        return {
            "regra": regra_escolhida["regra"],
            "periodo_label": f"{regra_escolhida['data_inicio'].strftime('%d/%m/%Y')} a {fim_txt}",
            "origem": "Histórico",
            "regra_id": regra_escolhida["id"],
        }
    return {
        "regra": regra_comissao_formatada(regra_default, regra_default),
        "periodo_label": "Padrão do sistema",
        "origem": "Padrão",
        "regra_id": "default",
    }

def faixa_info_por_inscricao(insc: int, regra: dict):
    if insc <= regra["f1_max"]:
        return f"0 a {int(regra['f1_max'])} (Não recebe)", 0.0
    if insc <= regra["f2_max"]:
        return f"{int(regra['f1_max'] + 1)} a {int(regra['f2_max'])}", float(regra["f2_val"])
    if insc <= regra["f3_max"]:
        return f"{int(regra['f2_max'] + 1)} a {int(regra['f3_max'])}", float(regra["f3_val"])
    if insc <= regra["f4_max"]:
        return f"{int(regra['f3_max'] + 1)} a {int(regra['f4_max'])}", float(regra["f4_val"])
    return f"{int(regra['f5_min'])} ou mais", float(regra["f5_val"])

def enriquecer_df_com_regras(df_base: pd.DataFrame, cfg: dict, inicio_sistema: dt.date) -> pd.DataFrame:
    if df_base.empty:
        return df_base.copy()
    regra_default = regra_comissao_formatada(cfg["regras_comissao_default"], cfg["regras_comissao_default"])
    regras_hist = regras_historico_normalizadas(cfg)
    out = df_base.copy()
    out["Data_base"] = pd.to_datetime(out["Data"]).dt.date

    def _resolve_row(row):
        sel = regra_para_data(row["Data_base"], regra_default, regras_hist, inicio_sistema)
        faixa_txt, val_por_insc = faixa_info_por_inscricao(int(row["Inscricoes"]), sel["regra"])
        comissao = float(row["Inscricoes"]) * float(val_por_insc)
        return pd.Series({
            "Regra_Periodo": sel["periodo_label"],
            "Regra_Origem": sel["origem"],
            "Faixa Atingida": faixa_txt,
            "Valor por Insc. (R$)": val_por_insc,
            "Valor_Comissao": comissao,
            "Regra_ID": sel["regra_id"],
        })

    extras = out.apply(_resolve_row, axis=1)
    return pd.concat([out, extras], axis=1)

def calcular_comissao_masterclass(inscricoes: int, regra: dict) -> float:
    """Calcula a comissão baseada nas faixas definidas."""
    if inscricoes <= regra['f1_max']: return inscricoes * regra['f1_val']
    elif inscricoes <= regra['f2_max']: return inscricoes * regra['f2_val']
    elif inscricoes <= regra['f3_max']: return inscricoes * regra['f3_val']
    elif inscricoes <= regra['f4_max']: return inscricoes * regra['f4_val']
    else: return inscricoes * regra['f5_val']

# =========================================================
# DIÁLOGOS (MODAIS)
# =========================================================
@st.dialog("📊 Detalhamento do Palestrante", width="large")
def dialog_detalhes_palestrante(palestrante, df):
    st.markdown(f"### 🎤 {palestrante}")
    df_pal = df[df["Palestrante"] == palestrante].copy()
    
    if df_pal.empty:
        st.warning("Nenhum dado de Masterclass encontrado para o período selecionado.")
        return

    vendas = int(df_pal["Inscricoes"].sum())
    presentes = int(df_pal["Sala"].sum())
    conv = (vendas / presentes * 100) if presentes > 0 else 0.0
    qtd_mc = len(df_pal)
    media_insc = vendas / qtd_mc if qtd_mc > 0 else 0.0

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Total de Masterclasses", fmt_br(qtd_mc))
    c2.metric("Total de Inscrições", fmt_br(vendas))
    c3.metric("Média por Inscrição", fmt_br_float(media_insc, 1))
    c4.metric("Conversão Global", f"{fmt_br_float(conv, 1)}%")
    
    st.markdown("---")
    st.markdown("#### 📋 Histórico de Eventos")
    
    view_df = df_pal[["Data", "Polo", "Cidade", "Sala", "Inscricoes"]].sort_values("Data", ascending=False).copy()
    view_df["Data"] = pd.to_datetime(view_df["Data"]).dt.strftime("%d/%m/%Y")
    view_df["Conversão"] = np.where(view_df["Sala"] > 0, (view_df["Inscricoes"] / view_df["Sala"] * 100).round(1).astype(str) + "%", "0%")
    
    st.dataframe(view_df, hide_index=True, width='stretch')

@st.dialog("📄 Relatório Detalhado de Comissão", width="large")
def dialog_relatorio_comissao(palestrante, df_mes, mes, ano):
    st.markdown(f"### 🎤 {palestrante}")
    st.caption(f"**Mês de Referência:** {mes:02d}/{ano}")
    
    df_pal = df_mes[df_mes["Palestrante"] == palestrante].copy()
    
    if df_pal.empty:
        st.warning("Nenhuma masterclass encontrada para este palestrante no mês selecionado.")
        return
    if "Regra_Periodo" not in df_pal.columns:
        df_pal["Regra_Periodo"] = "Padrão do sistema"
    if "Faixa Atingida" not in df_pal.columns:
        df_pal["Faixa Atingida"] = "-"
    if "Valor por Insc. (R$)" not in df_pal.columns:
        df_pal["Valor por Insc. (R$)"] = 0.0
        
    if "Valor_Comissao" in df_pal.columns:
        df_pal["Comissão Total (R$)"] = df_pal["Valor_Comissao"]
    else:
        df_pal["Comissão Total (R$)"] = 0.0
    
    # Resumo KPIs
    total_comissao = df_pal["Comissão Total (R$)"].sum()
    total_insc = df_pal["Inscricoes"].sum()
    mcs_zeradas = len(df_pal[df_pal["Comissão Total (R$)"] == 0])
    mcs_pagas = len(df_pal[df_pal["Comissão Total (R$)"] > 0])
    
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("💰 Total a Receber", fmt_br_money(total_comissao))
    c2.metric("🎟️ Inscrições Totais", fmt_br(total_insc))
    c3.metric("✅ MCs Comissionadas", fmt_br(mcs_pagas))
    c4.metric("❌ MCs Zeradas", fmt_br(mcs_zeradas))
    
    st.markdown("---")
    st.markdown("#### 📋 Detalhamento dos Eventos")
    cols_view = ["Data", "Polo", "Cidade", "Sala", "Inscricoes", "Regra_Periodo", "Faixa Atingida", "Valor por Insc. (R$)", "Comissão Total (R$)"]
    view_df = df_pal[cols_view].sort_values("Data", ascending=False).copy()
    
    # Formatando para exibição na tela
    view_df_fmt = view_df.copy()
    view_df_fmt["Data"] = pd.to_datetime(view_df_fmt["Data"]).dt.strftime("%d/%m/%Y")
    view_df_fmt["Valor por Insc. (R$)"] = view_df_fmt["Valor por Insc. (R$)"].apply(fmt_br_money)
    view_df_fmt["Comissão Total (R$)"] = view_df_fmt["Comissão Total (R$)"].apply(fmt_br_money)
    
    st.dataframe(view_df_fmt, hide_index=True, width='stretch')
    
    st.markdown("<br>", unsafe_allow_html=True)
    
    # Exportação CSV Avançada (Gerando cabeçalho personalizado conforme a tela)
    csv_header = f"RELATÓRIO DE COMISSÕES\n"
    csv_header += f"Palestrante:;{palestrante}\n"
    csv_header += f"Mês de Referência:;{mes:02d}/{ano}\n\n"
    csv_header += f"Total a Receber:;{fmt_br_money(total_comissao)}\n"
    csv_header += f"Inscrições Totais:;{total_insc}\n"
    csv_header += f"Masterclasses Comissionadas:;{mcs_pagas}\n"
    csv_header += f"Masterclasses Não Comissionadas:;{mcs_zeradas}\n\n"
    csv_header += "DETALHAMENTO DOS EVENTOS\n"
    
    df_export = view_df.copy()
    df_export["Data"] = pd.to_datetime(df_export["Data"]).dt.strftime("%d/%m/%Y")
    csv_body = df_export.to_csv(index=False, sep=";", decimal=",")
    
    final_csv_str = csv_header + csv_body
    csv_bytes = final_csv_str.encode(CSV_ENCODING)

    st.download_button(
        label="📥 Baixar Relatório Completo (CSV Excel)",
        data=csv_bytes,
        file_name=f"comissao_{palestrante}_{mes:02d}_{ano}.csv",
        mime="text/csv",
        type="primary",
        width='content'  # Botão padrão sem expandir
    )

# =========================================================
# 6. ESTADO E INICIALIZAÇÃO
# =========================================================
cfg = load_config()
df_mc, df_mt = load_data()

if "page" not in st.session_state:
    st.session_state["page"] = "📊 DASH"
if "flt_polos" not in st.session_state:
    st.session_state["flt_polos"] = []
if "flt_pals" not in st.session_state:
    st.session_state["flt_pals"] = []
if "flt_cids" not in st.session_state:
    st.session_state["flt_cids"] = []

if "flt_from" not in st.session_state:
    if not df_mc.empty and pd.notnull(df_mc["Data_dt"].min()):
        min_date = df_mc["Data_dt"].min().date()
        st.session_state["flt_from"] = dt.date(min_date.year, min_date.month, 1)
    else:
        st.session_state["flt_from"] = dt.date.today().replace(day=1)

if "flt_to" not in st.session_state:
    if not df_mc.empty and pd.notnull(df_mc["Data_dt"].max()):
        max_date = df_mc["Data_dt"].max().date()
        last_day = calendar.monthrange(max_date.year, max_date.month)[1]
        st.session_state["flt_to"] = dt.date(max_date.year, max_date.month, last_day)
    else:
        last_day = calendar.monthrange(dt.date.today().year, dt.date.today().month)[1]
        st.session_state["flt_to"] = dt.date.today().replace(day=last_day)

is_critical = False
if not df_mc.empty and (df_mc["Inscricoes"] > df_mc["Sala"]).any():
    is_critical = True
if not df_mt.empty and (df_mt["Meta_Inscricoes"] != (df_mt["Quantidade_MC"] * df_mt["Meta_Vendas_Por_MC"])).any():
    is_critical = True

period_str = f"{st.session_state['flt_from'].strftime('%d/%m/%Y')} - {st.session_state['flt_to'].strftime('%d/%m/%Y')}"
topbar(period_str, "CRÍTICO" if is_critical else "OK")

nav_col, _ = st.columns([8, 1])
with nav_col:
    page = st.radio(
        "Navegação",
        ["📊 DASH", "📝 Masterclass", "🎯 Metas", "💰 Comissões", "📅 Agenda", "🧩 Cadastro"],
        horizontal=True,
        label_visibility="collapsed",
        index=["📊 DASH", "📝 Masterclass", "🎯 Metas", "💰 Comissões", "📅 Agenda", "🧩 Cadastro"].index(st.session_state["page"])
    )
    if page != st.session_state["page"]:
        st.session_state["page"] = page
        st.rerun()

# =========================================================
# 7. MÓDULOS DE RENDERIZAÇÃO
# =========================================================

def module_dashboard(df_mc, df_mt, cfg):
    # --- FILTROS NO DASHBOARD ---
    with st.container(border=True):
        if st.session_state["flt_polos"]:
            cidades_disponiveis = []
            for p in st.session_state["flt_polos"]:
                cidades_disponiveis.extend(cfg["cidades_rel"].get(p, []))
            cidades_disponiveis = sorted(list(set(cidades_disponiveis)))
        else:
            cidades_disponiveis = sorted(list(set([c for sublist in cfg["cidades_rel"].values() for c in sublist])))

        valid_cids = [c for c in st.session_state["flt_cids"] if c in cidades_disponiveis]

        f_col1, f_col2, f_col3, f_col4, f_col5 = st.columns([1.8, 1.6, 1.8, 1.15, 1.15])
        with f_col1:
            sel_polos = st.multiselect("📍 Polos", cfg["polos"], default=st.session_state["flt_polos"])
        with f_col2:
            sel_pals = st.multiselect("🎤 Palestrantes", cfg["palestrantes"], default=st.session_state["flt_pals"])
        with f_col3:
            sel_cids = st.multiselect("🏙️ Cidades", cidades_disponiveis, default=valid_cids)
        with f_col4:
            sel_from = st.date_input("📅 Data Inicial", value=st.session_state["flt_from"], format="DD/MM/YYYY")
        with f_col5:
            sel_to = st.date_input("📅 Data Final", value=st.session_state["flt_to"], format="DD/MM/YYYY")

        if sel_from > sel_to:
            st.error("⚠️ A Data Inicial não pode ser maior que a Data Final.")
        else:
            if (valid_cids != st.session_state["flt_cids"] or sel_polos != st.session_state["flt_polos"] or
                sel_pals != st.session_state["flt_pals"] or sel_cids != valid_cids or
                sel_from != st.session_state["flt_from"] or sel_to != st.session_state["flt_to"]):
                st.session_state["flt_polos"] = sel_polos
                st.session_state["flt_pals"] = sel_pals
                st.session_state["flt_cids"] = sel_cids
                st.session_state["flt_from"] = sel_from
                st.session_state["flt_to"] = sel_to
                st.rerun()

    # --- LÓGICA DE DADOS ---
    df_f = df_mc.copy()
    if not df_f.empty:
        df_f = df_f[
            (df_f["Data_dt"].dt.date >= st.session_state["flt_from"]) &
            (df_f["Data_dt"].dt.date <= st.session_state["flt_to"])
        ]
        if st.session_state["flt_polos"]:
            df_f = df_f[df_f["Polo"].isin(st.session_state["flt_polos"])]
        if st.session_state["flt_pals"]:
            df_f = df_f[df_f["Palestrante"].isin(st.session_state["flt_pals"])]
        if st.session_state["flt_cids"]:
            df_f = df_f[df_f["Cidade"].isin(st.session_state["flt_cids"])]

    vendas = int(df_f["Inscricoes"].sum()) if not df_f.empty else 0
    presentes = int(df_f["Sala"].sum()) if not df_f.empty else 0
    conv = (vendas / presentes * 100) if presentes > 0 else 0.0
    qtd_mc = len(df_f)
    media_insc = (vendas / qtd_mc) if qtd_mc > 0 else 0.0
    media_sala = (presentes / qtd_mc) if qtd_mc > 0 else 0.0

    metas_periodo = metas_prorated_for_window(df_mt, st.session_state["flt_from"], st.session_state["flt_to"], st.session_state["flt_polos"])
    total_meta = int(metas_periodo["Meta"].sum()) if not metas_periodo.empty else 0
    ating = (vendas / total_meta * 100) if total_meta > 0 else 0.0
    gap_meta = max(0, total_meta - vendas)

    # --- UI: VISÃO EXECUTIVA ---
    section_header(
        "Visão Executiva",
        f"Leitura consolidada do período de {st.session_state['flt_from'].strftime('%d/%m/%Y')} até {st.session_state['flt_to'].strftime('%d/%m/%Y')}"
    )

    html_kpi = f"""<div class="kpi-grid">
        <div class="kpi"><div class="kpi-label">Vendas Efetuadas</div><div class="kpi-value">{fmt_br(vendas)}</div><div class="small-muted">Total de inscrições realizadas</div></div>
        <div class="kpi"><div class="kpi-label">Presença em Sala</div><div class="kpi-value">{fmt_br(presentes)}</div><div class="small-muted">Média de {fmt_br_float(media_sala, 0)} por evento</div></div>
        <div class="kpi"><div class="kpi-label">Conversão</div><div class="kpi-value">{fmt_br_float(conv, 1)}%</div><div class="small-muted">Relação entre vendas e presença</div></div>
        <div class="kpi"><div class="kpi-label">Masterclass</div><div class="kpi-value">{fmt_br(qtd_mc)}</div><div class="small-muted">Eventos realizados no período</div></div>
        <div class="kpi"><div class="kpi-label">Média de Inscrições</div><div class="kpi-value">{fmt_br_float(media_insc, 1)}</div><div class="small-muted">Inscrições por masterclass</div></div>
        <div class="kpi kpi-accent"><div class="kpi-label">Atingimento da Meta</div><div class="kpi-value">{fmt_br_float(ating, 1)}%</div><div class="small-muted">Meta: {fmt_br(total_meta)} | Gap: {fmt_br(gap_meta)}</div></div>
    </div>"""
    render_html(html_kpi)

    # --- UI: GRÁFICOS ---
    chart_col1, chart_col2 = st.columns([1, 1], gap="medium")

    with chart_col1:
        with st.container(border=True):
            render_html("""<div class="panel-head" style="margin-bottom: 8px;">
                <div><h4 class="panel-title">Mapa Comparativo</h4><p class="panel-sub">Realizado x projetado ao longo do período</p></div>
            </div>""")

            start_d, end_d = st.session_state["flt_from"], st.session_state["flt_to"]
            periods = []
            curr = start_d.replace(day=1)
            end_curr = end_d.replace(day=1)
            while curr <= end_curr:
                periods.append((curr.year, curr.month))
                curr = dt.date(curr.year + 1, 1, 1) if curr.month == 12 else dt.date(curr.year, curr.month + 1, 1)

            meses_pt = {1: "Jan", 2: "Fev", 3: "Mar", 4: "Abr", 5: "Mai", 6: "Jun", 7: "Jul", 8: "Ago", 9: "Set", 10: "Out", 11: "Nov", 12: "Dez"}
            timeline_data = []

            for y, m in periods:
                realizado = 0
                if not df_mc.empty:
                    real_df = df_mc[(df_mc["Data_dt"].dt.year == y) & (df_mc["Data_dt"].dt.month == m)]
                    if st.session_state["flt_polos"]: real_df = real_df[real_df["Polo"].isin(st.session_state["flt_polos"])]
                    if st.session_state["flt_cids"]: real_df = real_df[real_df["Cidade"].isin(st.session_state["flt_cids"])]
                    if st.session_state["flt_pals"]: real_df = real_df[real_df["Palestrante"].isin(st.session_state["flt_pals"])]
                    realizado = int(real_df["Inscricoes"].sum())

                last_day = calendar.monthrange(y, m)[1]
                m_start = max(start_d, dt.date(y, m, 1))
                m_end = min(end_d, dt.date(y, m, last_day))
                prorated_meta = metas_prorated_for_window(df_mt, m_start, m_end, st.session_state["flt_polos"])
                projetado = int(prorated_meta["Meta"].sum()) if not prorated_meta.empty else 0

                timeline_data.append({"Mês": f"{meses_pt[m]}/{str(y)[2:]}", "Projetado": projetado, "Realizado": realizado})

            timeline = pd.DataFrame(timeline_data)

            if timeline.empty or (timeline["Projetado"].sum() == 0 and timeline["Realizado"].sum() == 0):
                render_html(render_empty_state("Nenhum dado financeiro ou de metas para o período.", "📊"))
            else:
                if len(timeline) == 1:
                    fig1 = px.bar(timeline, x="Mês", y=["Projetado", "Realizado"], barmode="group")
                    fig1.update_traces(marker_line_width=0, opacity=0.92)
                else:
                    fig1 = px.line(timeline, x="Mês", y=["Projetado", "Realizado"], markers=True)
                    fig1.update_traces(line=dict(width=3), marker=dict(size=7, opacity=0.92))
                fig1.update_layout(yaxis_title="Inscrições", xaxis_title=None)
                st.plotly_chart(apply_plotly_theme(fig1), width='stretch')

    with chart_col2:
        with st.container(border=True):
            render_html("""<div class="panel-head" style="margin-bottom: 8px;">
                <div><h4 class="panel-title">Projetado x Realizado por Polo</h4><p class="panel-sub">Comparativo do período filtrado</p></div>
            </div>""")

            real_polo = df_f.groupby("Polo", as_index=False).agg(Realizado=("Inscricoes", "sum")) if not df_f.empty else pd.DataFrame(columns=["Polo", "Realizado"])
            view = metas_periodo.merge(real_polo, on="Polo", how="outer").fillna(0)
            view = view.rename(columns={"Meta": "Projetado"})
            proj = pd.to_numeric(view["Projetado"], errors="coerce").fillna(0)
            real = pd.to_numeric(view["Realizado"], errors="coerce").fillna(0)
            view["Projetado"] = proj.astype(float)
            view["Realizado"] = real.astype(float)
            view["Atingimento"] = np.divide(
                real.to_numpy(dtype=float),
                proj.to_numpy(dtype=float),
                out=np.zeros(len(view), dtype=float),
                where=proj.to_numpy(dtype=float) != 0
            ) * 100
            view = view.sort_values("Atingimento", ascending=False)

            if view.empty or (view["Projetado"].sum() == 0 and view["Realizado"].sum() == 0):
                render_html(render_empty_state("Nenhuma distribuição de polos no período.", "🗺️"))
            else:
                fig2 = px.bar(view, x="Polo", y=["Projetado", "Realizado"], barmode="group")
                fig2.update_traces(marker_line_width=0, opacity=0.92)
                fig2.update_layout(yaxis_title="Inscrições", xaxis_title=None)
                st.plotly_chart(apply_plotly_theme(fig2), width='stretch')

    # --- UI: GRID DE INFORMAÇÕES DETALHADAS ---
    col_pal, col_cid, col_est = st.columns([1.5, 1.1, 1.1], gap="medium")

    # --- Coluna 1: Palestrantes ---
    with col_pal:
        with st.container(border=True):
            render_html("""<div class="panel-head" style="margin-bottom: 8px;">
                <div>
                    <h4 class="panel-title" style="display:flex; align-items:center;">
                        Top Palestrantes
                        <div class="iam-tooltip">?<span class="tooltip-text">Score estratégico (Conversão multiplicada pela Média).</span></div>
                    </h4>
                    <p class="panel-sub">Score de média e conversão</p>
                </div>
            </div>""")

            if df_f.empty:
                render_html(render_empty_state("Aguardando lançamentos de masterclasses.", "🎤"))
            else:
                real_pal = df_f.groupby("Palestrante", as_index=False).agg(Qtd_MC=("ID", "count"), Pessoas_Sala=("Sala", "sum"), Inscricoes=("Inscricoes", "sum"))
                real_pal["Conversao"] = np.where(real_pal["Pessoas_Sala"] > 0, (real_pal["Inscricoes"] / real_pal["Pessoas_Sala"]) * 100, 0.0)
                real_pal["Media_Inscricoes"] = np.where(real_pal["Qtd_MC"] > 0, real_pal["Inscricoes"] / real_pal["Qtd_MC"], 0.0)
                real_pal = real_pal.sort_values("Inscricoes", ascending=False).reset_index(drop=True)
                real_pal.index = real_pal.index + 1
                real_pal.reset_index(inplace=True)
                real_pal.rename(columns={"index": "Rank"}, inplace=True)

                total_mc, total_sala, total_insc = real_pal["Qtd_MC"].sum(), real_pal["Pessoas_Sala"].sum(), real_pal["Inscricoes"].sum()
                total_conv = (total_insc / total_sala * 100) if total_sala > 0 else 0.0
                total_media = (total_insc / total_mc) if total_mc > 0 else 0.0

                real_pal_top10 = real_pal.head(10)
                html_cards_inner = ""
                for _, row in real_pal_top10.iterrows():
                    rank_num = int(row["Rank"])
                    if rank_num == 1: rank_display, top3_class = "🥇 1º", "top-3"
                    elif rank_num == 2: rank_display, top3_class = "🥈 2º", "top-3"
                    elif rank_num == 3: rank_display, top3_class = "🥉 3º", "top-3"
                    else: rank_display, top3_class = f"{rank_num}º", ""

                    badge_style = "background: var(--iam-navy); color: #ffffff; border: 1px solid var(--iam-navy);" if row["Media_Inscricoes"] >= 8.0 else "background: #E0F2FE; color: var(--iam-royal); border: 1px solid #BAE6FD;"
                    
                    # Nome clicável passando Parâmetro de URL
                    pal_encoded = urllib.parse.quote(row['Palestrante'])

                    html_cards_inner += f"""<div class="rc-card {top3_class}">
                        <div class="rc-left">
                            <span class="rc-rank">{rank_display}</span>
                            <a href="/?p={pal_encoded}" target="_self" class="rc-name clickable-name" title="Ver detalhes de {row['Palestrante']}">{row["Palestrante"]}</a>
                        </div>
                        <div class="rc-metrics">
                            <div class="rc-metric"><span class="rc-metric-lbl">MCLASS</span><span class="rc-metric-val">{fmt_br(row["Qtd_MC"])}</span></div>
                            <div class="rc-metric"><span class="rc-metric-lbl">SALA</span><span class="rc-metric-val">{fmt_br(row["Pessoas_Sala"])}</span></div>
                            <div class="rc-metric"><span class="rc-metric-lbl">INSCRIÇÕES</span><span class="rc-metric-val">{fmt_br(row["Inscricoes"])}</span></div>
                            <div class="rc-metric"><span class="rc-metric-lbl">CONV.</span><span class="rc-metric-val">{fmt_br_float(row["Conversao"], 1)}%</span></div>
                        </div>
                        <div class="rc-right"><div class="rc-badge" style="{badge_style}">{fmt_br_float(row["Media_Inscricoes"], 1)}</div></div>
                    </div>"""

                badge_style_tot = "background: var(--iam-navy); color: #ffffff; border: 1px solid var(--iam-navy);" if total_media >= 8.0 else "background: #E0F2FE; color: var(--iam-royal); border: 1px solid #BAE6FD;"

                render_html(f"""<div class="rc-container" style="margin-bottom: 12px;">{html_cards_inner}</div>
                    <div class="rc-card total" style="margin-top: auto; position: relative;">
                        <div class="rc-left"><span class="rc-rank">—</span><span class="rc-name">TOTAL GERAL</span></div>
                        <div class="rc-metrics">
                            <div class="rc-metric"><span class="rc-metric-lbl">MCLASS</span><span class="rc-metric-val">{fmt_br(total_mc)}</span></div>
                            <div class="rc-metric"><span class="rc-metric-lbl">SALA</span><span class="rc-metric-val">{fmt_br(total_sala)}</span></div>
                            <div class="rc-metric"><span class="rc-metric-lbl">INSCRIÇÕES</span><span class="rc-metric-val">{fmt_br(total_insc)}</span></div>
                            <div class="rc-metric"><span class="rc-metric-lbl">CONV.</span><span class="rc-metric-val">{fmt_br_float(total_conv, 1)}%</span></div>
                        </div>
                        <div class="rc-right"><div class="rc-badge" style="{badge_style_tot}">{fmt_br_float(total_media, 1)}</div></div>
                    </div>""")

    # --- Coluna 2: Geografia ---
    with col_cid:
        with st.container(border=True):
            render_html("""<div class="panel-head" style="margin-bottom: 8px;">
                <div><h4 class="panel-title">Top 5 Cidades</h4><p class="panel-sub">Maior volume de inscrições</p></div>
            </div>""")
            if df_f.empty:
                render_html(render_empty_state("Aguardando lançamentos na geografia.", "🏙️"))
            else:
                top_cid = df_f.groupby("Cidade", as_index=False)["Inscricoes"].sum().sort_values("Inscricoes", ascending=False).head(5)
                if top_cid["Inscricoes"].sum() == 0:
                    render_html(render_empty_state("Nenhuma inscrição validada nas cidades.", "🏙️"))
                else:
                    html_cidades = '<div class="mini-list">'
                    for i, (_, row) in enumerate(top_cid.iterrows(), 1):
                        html_cidades += f"""<div class="mini-item"><div class="mini-item-left"><span class="mini-rank">{i}º</span><span class="mini-name">{row['Cidade']}</span></div><span class="mini-val">{fmt_br(row['Inscricoes'])}</span></div>"""
                    html_cidades += "</div>"
                    render_html(html_cidades)

        with st.container(border=True):
            render_html("""<div class="panel-head" style="margin-bottom: 8px;">
                <div><h4 class="panel-title">Cidades em Alerta</h4><p class="panel-sub">Média &lt; 5 (Prioridade >2 Masterclasses)</p></div>
            </div>""")
            if df_f.empty:
                render_html(render_empty_state("Sem alertas no momento.", "🛡️"))
            else:
                worst_cid = df_f.groupby("Cidade", as_index=False).agg(
                    Qtd_MC=("ID", "count"),
                    Inscricoes=("Inscricoes", "sum"),
                    Sala=("Sala", "sum"),
                    Palestrantes=("Palestrante", lambda x: ", ".join(x.unique()))
                )
                
                worst_cid["Media"] = np.where(worst_cid["Qtd_MC"] > 0, worst_cid["Inscricoes"] / worst_cid["Qtd_MC"], 0.0)
                worst_cid["Conversao"] = np.where(worst_cid["Sala"] > 0, (worst_cid["Inscricoes"] / worst_cid["Sala"]) * 100, 0.0)
                
                worst_cid = worst_cid[worst_cid["Media"] < 5.0]
                
                # NOVO FLUXO DE PRIORIDADE:
                # Prioridade 1: > 2 MCs
                # Prioridade 2: == 2 MCs
                # Prioridade 3: == 1 MC
                worst_cid["Prioridade"] = np.where(worst_cid["Qtd_MC"] > 2, 1, np.where(worst_cid["Qtd_MC"] == 2, 2, 3))
                
                worst_cid = worst_cid.sort_values(by=["Prioridade", "Media", "Conversao"], ascending=[True, True, True]).head(5)

                if worst_cid.empty:
                    render_html(render_empty_state("Nenhuma cidade na zona de alerta (média < 5).", "🛡️"))
                else:
                    html_worst = '<div class="mini-list">'
                    for i, (_, row) in enumerate(worst_cid.iterrows(), 1):
                        html_worst += f"""<div class="mini-item" style="border-left: 4px solid {IAM_DANGER}; align-items:flex-start;">
                            <div class="mini-item-left" style="align-items:flex-start; flex-direction:column; gap:4px;">
                                <div style="display:flex; align-items:center; gap:8px;">
                                    <span class="mini-rank" style="background:#FEE2E2; color:{IAM_DANGER};">{i}º</span>
                                    <span class="mini-name">{row['Cidade']}</span>
                                </div>
                                <span style="font-size:10px; color:var(--iam-g500); line-height:1.2;"><b>{row['Qtd_MC']} MCs</b> | 🎤 {row['Palestrantes']}</span>
                            </div>
                            <div style="text-align:right;">
                                <div class="mini-val" style="color:{IAM_DANGER};">{fmt_br(row['Inscricoes'])} totais</div>
                                <div class="mini-sub">Média: {fmt_br_float(row['Media'], 1)} | {fmt_br_float(row['Conversao'], 1)}% conv.</div>
                            </div>
                        </div>"""
                    html_worst += "</div>"
                    render_html(html_worst)

    # --- Coluna 3: Estratégia ---
    with col_est:
        with st.container(border=True):
            render_html(f"""<div class="panel-head" style="margin-bottom: 8px;">
                <div><h4 class="panel-title">Resumo de Metas</h4><p class="panel-sub">Acompanhamento do período</p></div>
            </div>
            <div class="metric-slab meta"><span style="font-size:12px; color:{IAM_G500}; font-weight:800; letter-spacing:0.4px;">PROJETADO</span><strong style="color:{IAM_BLACK};">{fmt_br(total_meta)}</strong></div>
            <div class="metric-slab real"><span style="font-size:12px; color:{IAM_G500}; font-weight:800; letter-spacing:0.4px;">REALIZADO</span><strong style="color:{IAM_ROYAL};">{fmt_br(vendas)}</strong></div>
            <div class="metric-slab gap"><span style="font-size:12px; color:{IAM_DANGER}; font-weight:800; letter-spacing:0.4px;">FALTA P/ META</span><strong style="color:{IAM_DANGER};">{fmt_br(gap_meta)}</strong></div>""")

        with st.container(border=True):
            render_html("""<div class="panel-head" style="margin-bottom: 8px;">
                <div>
                    <h4 class="panel-title" style="display:flex; align-items:center;">
                        Sugestão de Escalas
                        <div class="iam-tooltip">?<span class="tooltip-text">Score estratégico (Conversão multiplicada pela Média).</span></div>
                    </h4>
                    <p class="panel-sub">Score de média e conversão</p>
                </div>
            </div>""")
            
            if df_f.empty:
                render_html(render_empty_state("Sem volume para calcular score.", "⏳"))
            else:
                sug_pal = df_f.groupby("Palestrante", as_index=False).agg(Qtd_MC=("ID", "count"), Pessoas_Sala=("Sala", "sum"), Inscricoes=("Inscricoes", "sum"))
                sug_pal["Conversao"] = np.where(sug_pal["Pessoas_Sala"] > 0, sug_pal["Inscricoes"] / sug_pal["Pessoas_Sala"], 0.0)
                sug_pal["Media"] = np.where(sug_pal["Qtd_MC"] > 0, sug_pal["Inscricoes"] / sug_pal["Qtd_MC"], 0.0)
                sug_pal["Score"] = sug_pal["Conversao"] * sug_pal["Media"]
                
                sug_pal_valid = sug_pal[sug_pal["Qtd_MC"] > 0].sort_values("Score", ascending=False).head(5)

                if sug_pal_valid.empty:
                    render_html(render_empty_state("Nenhum palestrante encontrado no período.", "⏳"))
                else:
                    html_sug = '<div class="mini-list">'
                    for _, row in sug_pal_valid.iterrows():
                        html_sug += f"""<div class="mini-item" style="border-left: 4px solid {IAM_ROYAL};">
                            <div class="mini-item-left"><span class="mini-name">{row['Palestrante']}</span></div>
                            <div style="text-align:right;"><div class="mini-val">{fmt_br_float(row['Media'], 1)} média</div><div class="mini-sub">{fmt_br_float(row['Conversao']*100, 1)}% conv.</div></div>
                        </div>"""
                    html_sug += "</div>"
                    render_html(html_sug)

    # ALVO 1: DETALHAMENTO DAS MASTERCLASSES (Ativado quando palestrante é filtrado, ou de forma geral)
    if st.session_state.get("flt_pals"):
        st.write("---")
        section_header(f"Detalhes das Masterclasses: {', '.join(st.session_state['flt_pals'])}", "Visão analítica registro a registro dos palestrantes selecionados")
        
        with st.container(border=True):
            if df_f.empty:
                render_html(render_empty_state("Nenhuma masterclass encontrada para este(s) palestrante(s).", "📊"))
            else:
                det_cols = ["Data", "Polo", "Cidade", "Palestrante", "Sala", "Inscricoes"]
                df_det = df_f[det_cols].sort_values("Data", ascending=False).copy()
                df_det["Data"] = pd.to_datetime(df_det["Data"]).dt.strftime("%d/%m/%Y")
                df_det["Conversão"] = np.where(df_det["Sala"] > 0, (df_det["Inscricoes"] / df_det["Sala"] * 100).round(1).astype(str) + "%", "0%")
                st.dataframe(df_det, hide_index=True, width='stretch')

    # ---------------------------------------------------------
    # CAPTURA DO CLIQUE NO NOME DO PALESTRANTE VIA QUERY PARAM
    # ---------------------------------------------------------
    if "p" in st.query_params:
        pal_selecionado = st.query_params["p"]
        # Limpa o URL instantaneamente para não reabrir sozinho ao atualizar a página
        del st.query_params["p"]
        # Abre o modal de Detalhamento
        dialog_detalhes_palestrante(pal_selecionado, df_f)

def module_masterclass(df_mc, cfg):
    # ALVO 2: SEPARAÇÃO EM ABAS (NOVO LANÇAMENTO E EDIÇÃO)
    section_header("Gestão de Masterclass", "Cadastro de novas masterclasses e edição do histórico")

    t_nova, t_lote = st.tabs(["➕ Novo Lançamento", "📋 Editar Histórico"])

    with t_nova:
        with st.container(border=True):
            c1, c2 = st.columns(2, gap="medium")

            with c1:
                dt_pal = st.date_input("Data", dt.date.today())
                polo = st.selectbox("Polo", ["Selecione..."] + cfg["polos"])
                cidades_polo = ["Selecione..."] + cfg["cidades_rel"].get(polo, []) if polo != "Selecione..." else ["Selecione um Polo primeiro"]
                cidade = st.selectbox("Cidade", cidades_polo)

            with c2:
                palestrante = st.selectbox("Palestrante", ["Selecione..."] + cfg["palestrantes"])
                sala = st.number_input("Presença (Pessoas na Sala)", min_value=0, step=1)
                vendas = st.number_input("Inscrições (Vendas)", min_value=0, step=1)

            render_html('<hr style="margin-top: 8px; margin-bottom: 16px; border-color: #EEF2F7;" />')
            if st.button("💾 Salvar Registro", type="primary"):
                if "Selecione" in polo or "Selecione" in cidade or "Selecione" in palestrante:
                    st.error("Preencha Polo, Cidade e Palestrante corretamente.")
                elif vendas > sala:
                    st.error("Vendas não podem superar a presença.")
                else:
                    novo_id = str(uuid.uuid4())
                    nova_linha = pd.DataFrame([{"ID": novo_id, "Tenant": TENANT_ID, "Data": dt_pal, "Polo": polo, "Cidade": cidade, "Sala": sala, "Inscricoes": vendas, "Palestrante": palestrante}])

                    if not df_mc.empty:
                        mask = ((df_mc["Data"] == dt_pal) & (df_mc["Polo"] == polo) & (df_mc["Cidade"] == cidade) & (df_mc["Palestrante"] == palestrante))
                        if mask.any():
                            nova_linha["ID"] = df_mc.loc[mask, "ID"].values[0]
                            df_new = df_mc[~mask].copy()
                        else:
                            df_new = df_mc.copy()
                    else:
                        df_new = pd.DataFrame()

                    df_final = pd.concat([df_new, nova_linha], ignore_index=True)
                    save_masterclass(df_final)
                    audit("Masterclass Upsert", f"{polo} - {dt_pal}")
                    ok_modal("Registro salvo com sucesso!")

    # ALVO 2: ABA DE EDIÇÃO DE MASTERCLASS
    with t_lote:
        with st.container(border=True):
            if not df_mc.empty:
                st.caption("Edite direto na tabela ou marque 'Excluir?' para remover registros incorretos.")
                view_mc = df_mc.copy()
                view_mc.insert(0, "Excluir?", False)
                cols_ordem = ["Excluir?", "Data", "Polo", "Cidade", "Palestrante", "Sala", "Inscricoes", "ID", "Tenant"]

                edited_mc = st.data_editor(view_mc[cols_ordem], hide_index=True, width='stretch', disabled=["ID", "Tenant"])

                @st.dialog("⚠️ Confirmação de Edição")
                def confirm_edit_mc_dialog(df_to_save):
                    st.warning("Tem certeza que deseja salvar as alterações realizadas nos registros de Masterclass?")
                    if st.button("✅ Sim, realizar edição", type="primary", width='stretch'):
                        save_masterclass(df_to_save.drop(columns=["Excluir?"]))
                        audit("Edição em Lote Masterclass")
                        st.session_state["_ok_open"] = True
                        st.session_state["_ok_msg"] = "Masterclasses atualizadas com sucesso!"
                        st.rerun()

                @st.dialog("🚨 Confirmação de Exclusão")
                def confirm_delete_mc_dialog(df_to_delete):
                    st.error(f"Você marcou {len(df_to_delete)} masterclass(es) para exclusão.")
                    st.warning("Esta ação removerá esses registros de todos os relatórios financeiros.")
                    if st.button("🗑️ Sim, excluir permanentemente", type="primary", width='stretch'):
                        ids_to_delete = df_to_delete["ID"].tolist()
                        save_masterclass(df_mc[~df_mc["ID"].isin(ids_to_delete)].copy())
                        audit("Exclusão em Lote Masterclass", f"{len(ids_to_delete)} removidas")
                        st.session_state["_ok_open"] = True
                        st.session_state["_ok_msg"] = "Masterclasses excluídas!"
                        st.rerun()

                b1, b2, _ = st.columns([2, 2, 6], gap="small")
                with b1:
                    if st.button("💾 Salvar Alterações", type="primary", width='stretch', key="save_mc"):
                        confirm_edit_mc_dialog(edited_mc[edited_mc["Excluir?"] == False].copy())
                with b2:
                    df_mc_delete = edited_mc[edited_mc["Excluir?"] == True]
                    if st.button("🗑️ Excluir Selecionados", type="secondary", width='stretch', disabled=len(df_mc_delete) == 0, key="del_mc"):
                        confirm_delete_mc_dialog(df_mc_delete)
            else:
                render_html(render_empty_state("Nenhum histórico de masterclass para editar.", "📋"))


def module_metas(df_mt, cfg):
    section_header("Gestão de Metas Mensais", "Cadastro rápido, edição em lote e atualização por período")

    t_nova, t_lote, t_periodo = st.tabs(["➕ Nova Meta", "📋 Edição na Tabela", "🔄 Alterar por Período"])

    with t_nova:
        with st.container(border=True):
            with st.form("form_metas"):
                c1, c2, c3 = st.columns([1, 1, 1], gap="medium")
                with c1:
                    mes = st.selectbox("Mês", list(range(1, 13)))
                    ano = st.number_input("Ano", min_value=2020, value=dt.date.today().year)
                    polo = st.selectbox("Polo", ["Selecione..."] + cfg["polos"])
                with c2:
                    qtd = st.number_input("Qtd Palestras", min_value=0, step=1)
                    meta_por = st.number_input("Meta por Palestra", min_value=0, step=1)
                    st.info(f"Meta total gerada: **{fmt_br(qtd * meta_por)}**")
                with c3:
                    st.write("")
                    st.write("")
                    submit_meta = st.form_submit_button("Salvar Nova Meta", type="primary", width='stretch')

                if submit_meta:
                    if "Selecione" in polo:
                        st.error("Selecione o Polo.")
                    else:
                        nova_meta = pd.DataFrame([{"ID": str(uuid.uuid4()), "Tenant": TENANT_ID, "Mes": mes, "Ano": ano, "Polo": polo, "Quantidade_MC": qtd, "Meta_Vendas_Por_MC": meta_por, "Meta_Inscricoes": qtd * meta_por}])
                        if not df_mt.empty:
                            mask = (df_mt["Mes"] == mes) & (df_mt["Ano"] == ano) & (df_mt["Polo"] == polo)
                            if mask.any():
                                nova_meta["ID"] = df_mt.loc[mask, "ID"].values[0]
                                df_m_new = df_mt[~mask].copy()
                            else:
                                df_m_new = df_mt.copy()
                        else:
                            df_m_new = pd.DataFrame()

                        save_metas(pd.concat([df_m_new, nova_meta], ignore_index=True))
                        audit("Meta Salva", f"{polo} {mes}/{ano}")
                        ok_modal("Meta gravada com sucesso!")

    with t_lote:
        with st.container(border=True):
            if not df_mt.empty:
                st.caption("Edite direto na tabela ou marque 'Excluir?' para remover registros.")
                view_mt = df_mt.copy()
                view_mt.insert(0, "Excluir?", False)
                cols_ordem = ["Excluir?", "Mes", "Ano", "Polo", "Quantidade_MC", "Meta_Vendas_Por_MC", "Meta_Inscricoes", "ID", "Tenant"]

                edited = st.data_editor(view_mt[cols_ordem], hide_index=True, width='stretch', disabled=["Meta_Inscricoes", "ID", "Tenant"])

                @st.dialog("⚠️ Confirmação de Edição")
                def confirm_edit_dialog(df_to_save):
                    st.warning("Tem certeza que deseja salvar as alterações realizadas na tabela?")
                    if st.button("✅ Sim, realizar edição", type="primary", width='stretch'):
                        df_to_save["Meta_Inscricoes"] = df_to_save["Quantidade_MC"] * df_to_save["Meta_Vendas_Por_MC"]
                        save_metas(df_to_save.drop(columns=["Excluir?"]))
                        audit("Edição em Lote na Tabela de Metas")
                        st.session_state["_ok_open"] = True
                        st.session_state["_ok_msg"] = "Metas atualizadas com sucesso!"
                        st.rerun()

                @st.dialog("🚨 Confirmação de Exclusão")
                def confirm_delete_dialog(df_to_delete):
                    st.error(f"Você marcou {len(df_to_delete)} meta(s) para exclusão.")
                    st.warning("Esta ação removerá esses números de todos os relatórios.")
                    if st.button("🗑️ Sim, excluir permanentemente", type="primary", width='stretch'):
                        ids_to_delete = df_to_delete["ID"].tolist()
                        save_metas(df_mt[~df_mt["ID"].isin(ids_to_delete)].copy())
                        audit("Exclusão em Lote", f"{len(ids_to_delete)} metas removidas")
                        st.session_state["_ok_open"] = True
                        st.session_state["_ok_msg"] = "Metas excluídas!"
                        st.rerun()

                b1, b2, _ = st.columns([2, 2, 6], gap="small")
                with b1:
                    if st.button("💾 Salvar Tabela", type="primary", width='stretch'):
                        confirm_edit_dialog(edited[edited["Excluir?"] == False].copy())
                with b2:
                    df_to_delete = edited[edited["Excluir?"] == True]
                    if st.button("🗑️ Excluir Selecionados", type="secondary", width='stretch', disabled=len(df_to_delete) == 0):
                        confirm_delete_dialog(df_to_delete)
            else:
                render_html(render_empty_state("Nenhuma meta cadastrada.", "🎯"))

    with t_periodo:
        with st.container(border=True):
            st.markdown("#### Atualização em Lote por Período")
            st.caption("Altere a 'Meta por Palestra' de vários meses de uma só vez. Apenas registros já existentes serão atualizados.")
            with st.form("form_periodo"):
                c1, c2, c3 = st.columns([1, 1, 1.5], gap="medium")
                with c1:
                    st.markdown("**Período Inicial**")
                    mes_ini = st.selectbox("Mês", list(range(1, 13)), key="mi")
                    ano_ini = st.number_input("Ano", min_value=2020, value=dt.date.today().year, key="ai")
                with c2:
                    st.markdown("**Período Final**")
                    mes_fim = st.selectbox("Mês", list(range(1, 13)), key="mf", index=11)
                    ano_fim = st.number_input("Ano", min_value=2020, value=dt.date.today().year, key="af")
                with c3:
                    st.markdown("**Novos Parâmetros**")
                    polo_alvo = st.selectbox("Polo Alvo", ["TODOS OS POLOS"] + cfg["polos"], key="pa")
                    nova_meta = st.number_input("Nova Meta por Palestra", min_value=0, step=1, key="nm")
                
                st.write("")
                submit_periodo = st.form_submit_button("🔄 Aplicar Nova Meta ao Período", type="primary", width='stretch')

            if submit_periodo:
                start_ym = ano_ini * 100 + mes_ini
                end_ym = ano_fim * 100 + mes_fim
                
                if start_ym > end_ym:
                    st.error("⚠️ O período inicial não pode ser maior que o final.")
                elif df_mt.empty:
                    st.warning("⚠️ Nenhuma meta cadastrada no banco de dados para ser alterada.")
                else:
                    df_to_update = df_mt.copy()
                    df_to_update["ym"] = df_to_update["Ano"] * 100 + df_to_update["Mes"]
                    
                    mask = (df_to_update["ym"] >= start_ym) & (df_to_update["ym"] <= end_ym)
                    if polo_alvo != "TODOS OS POLOS":
                        mask = mask & (df_to_update["Polo"] == polo_alvo)
                        
                    registros_afetados = mask.sum()
                    
                    if registros_afetados == 0:
                        st.warning("⚠️ Nenhum registro encontrado para este período e polo.")
                    else:
                        df_to_update.loc[mask, "Meta_Vendas_Por_MC"] = nova_meta
                        df_to_update.loc[mask, "Meta_Inscricoes"] = df_to_update.loc[mask, "Quantidade_MC"] * nova_meta
                        
                        df_to_update = df_to_update.drop(columns=["ym"])
                        save_metas(df_to_update)
                        audit("Alteração de Meta por Período", f"{polo_alvo} ({mes_ini}/{ano_ini} a {mes_fim}/{ano_fim}) -> {nova_meta}")
                        ok_modal(f"Sucesso! {registros_afetados} registro(s) atualizado(s) para a meta de {nova_meta}.")

def module_comissoes(df_mc, cfg):
    section_header("Gestão de Comissões", "Cálculo automático de comissionamento de palestrantes e configuração de regras")

    t_resumo, t_regras = st.tabs(["💰 Resumo Mensal", "🗂️ Regras por Início"])

    with t_resumo:
        with st.container(border=True):
            render_html("""<div class="panel-head" style="margin-bottom: 8px;">
                <div><h4 class="panel-title">Filtrar Período de Cálculo</h4><p class="panel-sub">Selecione o mês para visualizar as comissões pagáveis</p></div>
            </div>""")
            
            c1, c2, c3, _ = st.columns([1, 1, 1, 4])
            with c1:
                mes_com = st.selectbox("Mês Referência", list(range(1, 13)), index=dt.date.today().month - 1)
            with c2:
                ano_com = st.number_input("Ano Referência", min_value=2020, value=dt.date.today().year)
            with c3:
                st.write("")
                st.write("")
                if st.button("Aplicar Filtro", width='stretch'):
                    pass # Só para re-renderizar

            df_com = df_mc.copy()
            inicio_sistema = obter_inicio_sistema(df_mc)
            if not df_com.empty:
                df_com = df_com[(df_com["Data_dt"].dt.year == ano_com) & (df_com["Data_dt"].dt.month == mes_com)].copy()
                df_com = enriquecer_df_com_regras(df_com, cfg, inicio_sistema)

            regra_default = regra_comissao_formatada(cfg["regras_comissao_default"], cfg["regras_comissao_default"])
            regras_hist = regras_historico_normalizadas(cfg)
            regras_ref = {"default": regra_default}
            if regras_hist:
                fim_padrao = regras_hist[0]["data_inicio"] - dt.timedelta(days=1)
                labels_ref = {"default": f"Padrão: {inicio_sistema.strftime('%d/%m/%Y')} a {fim_padrao.strftime('%d/%m/%Y')}"}
            else:
                labels_ref = {"default": "Padrão do sistema"}
            for rh in regras_hist:
                regras_ref[rh["id"]] = rh["regra"]
                fim_txt = rh["data_fim"].strftime("%d/%m/%Y") if rh["data_fim"] else "em vigor"
                labels_ref[rh["id"]] = f"{rh['data_inicio'].strftime('%d/%m/%Y')} a {fim_txt}"
            
            col_dash, col_legenda = st.columns([2.5, 1], gap="medium")
            
            with col_legenda:
                regras_no_periodo = []
                if not df_com.empty and "Regra_ID" in df_com.columns:
                    regras_no_periodo = list(dict.fromkeys(df_com["Regra_ID"].tolist()))
                if not regras_no_periodo:
                    regras_no_periodo = ["default"]

                blocos = []
                for rid in regras_no_periodo:
                    rr = regras_ref.get(rid, regra_default)
                    lbl = labels_ref.get(rid, "Padrão do sistema")
                    tag = "Padrão" if rid == "default" else "Regra histórica"
                    tag_bg = "#EEF2FF" if rid == "default" else "#ECFDF3"
                    tag_color = IAM_ROYAL if rid == "default" else IAM_SUCCESS
                    blocos.append(
                        f"""
                        <div class="mini-item" style="padding: 10px 12px; background: #ffffff; border:1px solid {IAM_G200}; border-radius:12px; margin-bottom:10px;">
                            <div style="display:flex; justify-content:space-between; align-items:center; gap:8px; margin-bottom:8px;">
                                <div style="font-size:11px; font-weight:800; color:{IAM_BLACK};">{lbl}</div>
                                <span style="font-size:9px; font-weight:800; color:{tag_color}; background:{tag_bg}; border-radius:999px; padding:3px 8px; white-space:nowrap;">{tag}</span>
                            </div>
                            <div style="display:grid; grid-template-columns: 1fr; gap:4px;">
                                <div style="display:flex; justify-content:space-between; font-size:10px;">
                                    <span style="color:{IAM_G700}; font-weight:700;">Faixa 1 (até {int(rr['f1_max'])})</span>
                                    <strong style="color:{IAM_G500};">{fmt_br_money(rr['f1_val'])}</strong>
                                </div>
                                <div style="display:flex; justify-content:space-between; font-size:10px;">
                                    <span style="color:{IAM_G700}; font-weight:700;">Faixa 2 (até {int(rr['f2_max'])})</span>
                                    <strong style="color:{IAM_G500};">{fmt_br_money(rr['f2_val'])}</strong>
                                </div>
                                <div style="display:flex; justify-content:space-between; font-size:10px;">
                                    <span style="color:{IAM_G700}; font-weight:700;">Faixa 3 (até {int(rr['f3_max'])})</span>
                                    <strong style="color:{IAM_G500};">{fmt_br_money(rr['f3_val'])}</strong>
                                </div>
                                <div style="display:flex; justify-content:space-between; font-size:10px;">
                                    <span style="color:{IAM_G700}; font-weight:700;">Faixa 4 (até {int(rr['f4_max'])})</span>
                                    <strong style="color:{IAM_G500};">{fmt_br_money(rr['f4_val'])}</strong>
                                </div>
                                <div style="display:flex; justify-content:space-between; font-size:10px;">
                                    <span style="color:{IAM_G700}; font-weight:700;">Faixa 5 (desde {int(rr['f5_min'])})</span>
                                    <strong style="color:{IAM_SUCCESS};">{fmt_br_money(rr['f5_val'])}</strong>
                                </div>
                            </div>
                        </div>
                        """
                    )

                html_legenda = f"""<div style="background: {IAM_G100}; border: 1px solid {IAM_G200}; border-radius: 16px; padding: 18px;">
                    <div style="font-weight: 850; font-size: 14px; color: {IAM_BLACK}; margin-bottom: 14px; display:flex; align-items:center; gap:8px;">
                        📌 Regras Aplicadas <span style="font-size:10px; font-weight:700; color:{IAM_ROYAL}; background:#E0F2FE; padding:3px 8px; border-radius:8px;">{mes_com:02d}/{ano_com}</span>
                    </div>
                    <div style="font-size:10px; color:{IAM_G500}; margin-bottom:10px;">Total de regras usadas no período: <strong style="color:{IAM_BLACK};">{len(regras_no_periodo)}</strong></div>
                    <div class="mini-list">
                        {''.join(blocos)}
                    </div>
                    <div style="font-size: 10px; color: {IAM_G500}; margin-top: 12px; line-height:1.4;">*Cada masterclass usa a regra vigente na data do treinamento.</div>
                </div>"""
                render_html(html_legenda)

            with col_dash:
                if df_com.empty:
                    render_html(render_empty_state("Nenhuma masterclass registrada neste mês.", "💰"))
                else:
                    comissoes_totais = df_com["Valor_Comissao"].sum()
                    
                    render_html(f"""
                        <div class="metric-slab real" style="padding: 16px; margin-bottom: 16px; border-color: {IAM_ROYAL}; border-left: 5px solid {IAM_ROYAL};">
                            <span style="font-size:13px; color:{IAM_G500}; font-weight:800; letter-spacing:0.4px;">TOTAL A PAGAR NO MÊS</span>
                            <strong style="color:{IAM_ROYAL}; font-size: 24px;">{fmt_br_money(comissoes_totais)}</strong>
                        </div>
                    """)
                    
                    st.markdown("#### Detalhamento por Palestrante")
                    
                    agrupado = df_com.groupby("Palestrante", as_index=False).agg(
                        Masterclasses=("ID", "count"),
                        Inscricoes=("Inscricoes", "sum"),
                        Comissao=("Valor_Comissao", "sum")
                    ).sort_values("Comissao", ascending=False)
                    
                    # Cabeçalho da Lista Interativa
                    render_html(f"""
                    <div style="display:flex; padding: 10px 14px; background: var(--iam-g100); border-radius: 12px 12px 0 0; border-bottom: 2px solid var(--iam-g200); font-weight: 800; font-size: 11px; color: var(--iam-g500); text-transform: uppercase;">
                        <div style="flex: 2.5;">Palestrante</div>
                        <div style="flex: 1; text-align: center;">Masterclasses</div>
                        <div style="flex: 1; text-align: center;">Inscrições</div>
                        <div style="flex: 1.5; text-align: right;">Comissão (R$)</div>
                        <div style="flex: 1; text-align: center; margin-left: 10px;">Ação</div>
                    </div>
                    """)
                    
                    # Lista Interativa com Botões Reais em cada linha (botão menor)
                    for _, row in agrupado.iterrows():
                        c1, c2, c3, c4, c5 = st.columns([2.5, 1, 1, 1.5, 1], gap="small")
                        c1.markdown(f"<div style='padding-top:10px; font-weight:750; font-size:14px; color:var(--iam-black);'>{row['Palestrante']}</div>", unsafe_allow_html=True)
                        c2.markdown(f"<div style='padding-top:10px; text-align:center; font-size:14px;'>{fmt_br(row['Masterclasses'])}</div>", unsafe_allow_html=True)
                        c3.markdown(f"<div style='padding-top:10px; text-align:center; font-size:14px;'>{fmt_br(row['Inscricoes'])}</div>", unsafe_allow_html=True)
                        c4.markdown(f"<div style='padding-top:10px; text-align:right; font-weight:800; font-size:14px; color:var(--iam-royal);'>{fmt_br_money(row['Comissao'])}</div>", unsafe_allow_html=True)
                        with c5:
                            # O Botão aciona o Diálogo detalhado do palestrante (sem width stretch para ficar pequeno)
                            if st.button("Detalhar", key=f"btn_det_{row['Palestrante']}_{mes_com}_{ano_com}"):
                                dialog_relatorio_comissao(row['Palestrante'], df_com, mes_com, ano_com)
                        st.markdown("<hr style='margin:0; border-color:var(--iam-g100);'>", unsafe_allow_html=True)

    with t_regras:
        with st.container(border=True):
            st.markdown("#### Criar Regra por Data de Início")
            st.caption("A regra nova começa na data informada. A regra anterior passa a valer até o dia anterior automaticamente.")
            
            with st.form("form_regras_comissao"):
                c1, c2, c3 = st.columns([1, 1, 2], gap="large")
                with c1:
                    data_ini = st.date_input("Data Início da Regra", dt.date.today(), format="DD/MM/YYYY", key="r_data_ini")
                with c2:
                    st.caption("Sem data fim manual: será definida automaticamente.")
                
                st.markdown("---")
                regra_edit = cfg["regras_comissao_default"]
                _f4_default = int(regra_edit.get("f4_max", 20))
                _f5_default = int(regra_edit.get("f5_min", _f4_default + 1))
                fa1, fa2, fa3, fa4, fa5 = st.columns(5, gap="small")
                with fa1:
                    st.markdown("##### Faixa 1")
                    f1_max = st.number_input(
                        "Limite de inscrições (inclusive)",
                        value=int(regra_edit.get("f1_max", 7)),
                        min_value=0,
                        step=1,
                        help="Teto desta faixa: com até este número de inscrições aplica-se o valor ao lado.",
                        key="rc_new_f1_max",
                    )
                    f1_val = st.number_input(
                        "Valor por inscrição (R$)",
                        value=float(regra_edit.get("f1_val", 0.0)),
                        min_value=0.0,
                        step=1.0,
                        key="rc_new_f1_val",
                    )
                    st.caption(f"Escopo: **0** a **{f1_max}** inscrições.")
                with fa2:
                    st.markdown("##### Faixa 2")
                    f2_max = st.number_input(
                        "Limite de inscrições (inclusive)",
                        value=int(regra_edit.get("f2_max", 12)),
                        min_value=0,
                        step=1,
                        key="rc_new_f2_max",
                    )
                    f2_val = st.number_input(
                        "Valor por inscrição (R$)",
                        value=float(regra_edit.get("f2_val", 60.0)),
                        min_value=0.0,
                        step=1.0,
                        key="rc_new_f2_val",
                    )
                    st.caption(f"Escopo: **{f1_max + 1}** a **{f2_max}** inscrições.")
                with fa3:
                    st.markdown("##### Faixa 3")
                    f3_max = st.number_input(
                        "Limite de inscrições (inclusive)",
                        value=int(regra_edit.get("f3_max", 16)),
                        min_value=0,
                        step=1,
                        key="rc_new_f3_max",
                    )
                    f3_val = st.number_input(
                        "Valor por inscrição (R$)",
                        value=float(regra_edit.get("f3_val", 70.0)),
                        min_value=0.0,
                        step=1.0,
                        key="rc_new_f3_val",
                    )
                    st.caption(f"Escopo: **{f2_max + 1}** a **{f3_max}** inscrições.")
                with fa4:
                    st.markdown("##### Faixa 4")
                    f4_max = st.number_input(
                        "Limite de inscrições (inclusive)",
                        value=_f4_default,
                        min_value=0,
                        step=1,
                        key="rc_new_f4_max",
                    )
                    f4_val = st.number_input(
                        "Valor por inscrição (R$)",
                        value=float(regra_edit.get("f4_val", 80.0)),
                        min_value=0.0,
                        step=1.0,
                        key="rc_new_f4_val",
                    )
                    st.caption(f"Escopo: **{f3_max + 1}** a **{f4_max}** inscrições.")
                with fa5:
                    st.markdown("##### Faixa 5")
                    f5_min = st.number_input(
                        "Primeira inscrição da faixa 5 (inclusive)",
                        value=_f5_default,
                        min_value=max(1, f4_max + 1),
                        step=1,
                        help="Deve ser o limite da faixa 4 mais 1, sem buracos entre as faixas.",
                        key="rc_new_f5_min",
                    )
                    f5_val = st.number_input(
                        "Valor por inscrição (R$)",
                        value=float(regra_edit.get("f5_val", regra_edit.get("f6_val", 100.0))),
                        min_value=0.0,
                        step=1.0,
                        key="rc_new_f5_val",
                    )
                    st.caption(f"Escopo: **{f5_min}** ou mais inscrições.")

                st.caption(
                    f"Conferência: faixa 4 = **{f3_max + 1}**–**{f4_max}** inscrições; faixa 5 a partir de **{f5_min}** "
                    f"(esperado **{f4_max + 1}** = início da faixa 5)."
                )

                st.write("")
                col_btn1, _ = st.columns([2, 6])
                with col_btn1:
                    submit_regra = st.form_submit_button("💾 Salvar Regra a Partir da Data", type="primary", width='stretch')
                
                if submit_regra:
                    lim_ok = f1_max < f2_max < f3_max < f4_max < f5_min
                    val_ok = f1_val < f2_val < f3_val < f4_val < f5_val
                    encosta = f5_min == f4_max + 1
                    if not lim_ok:
                        st.error(
                            "Limites de inscrições: cada faixa precisa ter limite **estritamente menor** que o da seguinte "
                            "(faixa 1 < faixa 2 < faixa 3 < faixa 4 < início da faixa 5)."
                        )
                    elif not val_ok:
                        st.error(
                            "Valores (R$): cada faixa precisa ter valor por inscrição **estritamente menor** que o da faixa seguinte."
                        )
                    elif not encosta:
                        st.error(
                            "A primeira inscrição da faixa 5 deve ser **exatamente** o limite da faixa 4 + 1 "
                            f"(neste caso, **{f4_max + 1}**)."
                        )
                    else:
                        novas_regras = cfg.setdefault("regras_comissao_historico", [])
                        novo_ini = data_ini
                        regras_norm = regras_historico_normalizadas(cfg)

                        # Se já existe regra com mesmo início, substitui.
                        mesmo_inicio = None
                        for rr in regras_norm:
                            if rr["data_inicio"] == novo_ini:
                                mesmo_inicio = rr["id"]
                                break
                        if mesmo_inicio:
                            novas_regras = [x for x in novas_regras if str(x.get("id")) != str(mesmo_inicio)]
                            cfg["regras_comissao_historico"] = novas_regras

                        nova_regra = {
                            "id": str(uuid.uuid4()),
                            "data_inicio": novo_ini.isoformat(),
                            "created_at": dt.datetime.now().isoformat(timespec="seconds"),
                            "f1_max": f1_max, "f1_val": f1_val,
                            "f2_max": f2_max, "f2_val": f2_val,
                            "f3_max": f3_max, "f3_val": f3_val,
                            "f4_max": f4_max, "f4_val": f4_val,
                            "f5_min": f5_min,
                            "f5_val": f5_val,
                        }
                        novas_regras.append(nova_regra)
                        novas_regras.sort(key=lambda x: str(x.get("data_inicio", "")))
                        save_config(cfg)
                        audit("Regra de Comissão por Início", f"Regra iniciada em {novo_ini.isoformat()}")
                        ok_modal(
                            f"Regra salva com início em {novo_ini.strftime('%d/%m/%Y')}. "
                            "A regra anterior passa a terminar no dia anterior automaticamente."
                        )

            st.markdown("---")
            st.markdown("#### Histórico de Regras")
            hist = regras_historico_normalizadas(cfg)
            if not hist:
                st.info("Nenhuma regra por data de início cadastrada. O sistema está usando apenas a regra padrão.")
            else:
                rows = []
                inicio_sis_hist = obter_inicio_sistema(df_mc)
                fim_padrao_hist = hist[0]["data_inicio"] - dt.timedelta(days=1)
                if inicio_sis_hist <= fim_padrao_hist:
                    rows.append({
                        "ID": "default",
                        "Período": f"{inicio_sis_hist.strftime('%d/%m/%Y')} a {fim_padrao_hist.strftime('%d/%m/%Y')}",
                        "F1": f"até {cfg['regras_comissao_default']['f1_max']} ({fmt_br_money(cfg['regras_comissao_default']['f1_val'])})",
                        "F2": f"até {cfg['regras_comissao_default']['f2_max']} ({fmt_br_money(cfg['regras_comissao_default']['f2_val'])})",
                        "F3": f"até {cfg['regras_comissao_default']['f3_max']} ({fmt_br_money(cfg['regras_comissao_default']['f3_val'])})",
                        "F4": f"até {cfg['regras_comissao_default']['f4_max']} ({fmt_br_money(cfg['regras_comissao_default']['f4_val'])})",
                        "F5": f"desde {cfg['regras_comissao_default']['f5_min']} ({fmt_br_money(cfg['regras_comissao_default']['f5_val'])})",
                    })
                for h in hist:
                    r = h["regra"]
                    fim_txt = h["data_fim"].strftime('%d/%m/%Y') if h["data_fim"] else "em vigor"
                    rows.append({
                        "ID": h["id"],
                        "Período": f"{h['data_inicio'].strftime('%d/%m/%Y')} a {fim_txt}",
                        "F1": f"até {r['f1_max']} ({fmt_br_money(r['f1_val'])})",
                        "F2": f"até {r['f2_max']} ({fmt_br_money(r['f2_val'])})",
                        "F3": f"até {r['f3_max']} ({fmt_br_money(r['f3_val'])})",
                        "F4": f"até {r['f4_max']} ({fmt_br_money(r['f4_val'])})",
                        "F5": f"desde {r['f5_min']} ({fmt_br_money(r['f5_val'])})",
                    })
                st.dataframe(pd.DataFrame(rows), hide_index=True, width='stretch')

                opcoes = {
                    f"{h['data_inicio'].strftime('%d/%m/%Y')} a {(h['data_fim'].strftime('%d/%m/%Y') if h['data_fim'] else 'em vigor')} | ID {h['id'][:8]}": h["id"]
                    for h in hist
                }
                cdel1, cdel2 = st.columns([3, 1], gap="small")
                with cdel1:
                    regra_del_lbl = st.selectbox("Selecione uma regra histórica para excluir", list(opcoes.keys()), key="regra_del_lbl")
                with cdel2:
                    st.write("")
                    if st.button("🗑️ Excluir Regra", type="secondary", width='stretch'):
                        regra_del_id = opcoes.get(regra_del_lbl)
                        antes = len(cfg.get("regras_comissao_historico", []))
                        cfg["regras_comissao_historico"] = [x for x in cfg.get("regras_comissao_historico", []) if str(x.get("id")) != str(regra_del_id)]
                        depois = len(cfg.get("regras_comissao_historico", []))
                        if depois < antes:
                            save_config(cfg)
                            audit("Exclusão de Regra de Comissão", f"Regra removida: {regra_del_id}")
                            ok_modal("Regra histórica excluída com sucesso.")
                        else:
                            st.warning("Não foi possível localizar a regra selecionada.")

def module_agenda(df_mc, cfg):
    section_header("Agenda de Masterclasses", "Visão cronológica dos eventos agendados e realizados")

    @st.dialog("➕ Nova Agenda Futura")
    def dialog_nova_agenda():
        st.caption("Adicione rapidamente um evento na agenda. O sistema descobrirá o Polo automaticamente pela Cidade.")
        ag_data = st.date_input("Data do Evento", dt.date.today(), key="ag_data")
        
        # Achatar e ordenar todas as cidades para seleção rápida
        todas_cidades = []
        mapa_cidades = {}
        for polo_nome, cids in cfg.get("cidades_rel", {}).items():
            for c in cids:
                if c not in mapa_cidades:
                    mapa_cidades[c] = polo_nome
                todas_cidades.append(c)
        todas_cidades = sorted(list(set(todas_cidades)))
        
        ag_cid = st.selectbox("Cidade", ["— Selecione —"] + todas_cidades, key="ag_cid")
        ag_pal = st.selectbox("Palestrante", ["— Selecione —"] + cfg["palestrantes"], key="ag_pal")
        
        st.write("")
        if st.button("💾 Salvar Agenda", type="primary", width='stretch'):
            if ag_cid == "— Selecione —" or ag_pal == "— Selecione —":
                st.error("Preencha Cidade e Palestrante.")
            else:
                polo_inferido = mapa_cidades[ag_cid]
                novo_id = str(uuid.uuid4())
                
                nova_linha = pd.DataFrame([{
                    "ID": novo_id, "Tenant": TENANT_ID, "Data": ag_data, 
                    "Polo": polo_inferido, "Cidade": ag_cid, 
                    "Sala": 0, "Inscricoes": 0, "Palestrante": ag_pal
                }])
                
                if not df_mc.empty:
                    mask = ((df_mc["Data"] == ag_data) & (df_mc["Cidade"] == ag_cid) & (df_mc["Palestrante"] == ag_pal))
                    if mask.any():
                        nova_linha["ID"] = df_mc.loc[mask, "ID"].values[0]
                        df_new = df_mc[~mask].copy()
                    else:
                        df_new = df_mc.copy()
                else:
                    df_new = pd.DataFrame()
                    
                df_final = pd.concat([df_new, nova_linha], ignore_index=True)
                save_masterclass(df_final)
                audit("Novo Agendamento Futuro", f"{ag_cid} - {ag_data}")
                st.session_state["_ok_open"] = True
                st.session_state["_ok_msg"] = "Agendamento criado com sucesso!"
                st.rerun()

    @st.dialog("🗑️ Excluir Agendamento")
    def dialog_excluir_agenda(m_sel, a_sel):
        df_mes = df_mc[(df_mc["Data_dt"].dt.year == a_sel) & (df_mc["Data_dt"].dt.month == m_sel)].copy()
        if df_mes.empty:
            st.info(f"Nenhum evento agendado para {m_sel}/{a_sel}.")
        else:
            df_mes["Label"] = df_mes.apply(lambda x: f"{x['Data'].strftime('%d/%m/%Y')} - {x['Cidade']} ({x['Polo']}) | 🎤 {x['Palestrante']}", axis=1)
            eventos_dict = dict(zip(df_mes["Label"], df_mes["ID"]))
            
            ev_sel = st.selectbox("Selecione o evento que deseja excluir:", ["— Selecione —"] + list(eventos_dict.keys()), key="del_agenda_sel_modal")
            
            st.write("")
            if st.button("🗑️ Confirmar Exclusão", type="primary", width='stretch'):
                if ev_sel != "— Selecione —":
                    id_to_delete = eventos_dict[ev_sel]
                    df_mc_new = df_mc[df_mc["ID"] != id_to_delete].copy()
                    save_masterclass(df_mc_new)
                    audit("Exclusão de Evento via Agenda", f"Evento ID apagado: {id_to_delete}")
                    st.session_state["_ok_open"] = True
                    st.session_state["_ok_msg"] = "Evento excluído da agenda com sucesso!"
                    st.rerun()
                else:
                    st.error("Por favor, selecione um evento válido.")

    with st.container(border=True):
        c1, c2, c3, c4, c5 = st.columns([1.5, 1.5, 4, 1.8, 1.8], gap="small")
        with c1:
            mes_sel = st.selectbox("Mês", list(range(1, 13)), index=dt.date.today().month - 1, key="agenda_mes")
        with c2:
            ano_sel = st.number_input("Ano", min_value=2020, value=dt.date.today().year, key="agenda_ano")
        with c3:
            st.write("")
        with c4:
            st.write("")
            st.markdown("<div style='margin-top: 2px;'></div>", unsafe_allow_html=True)
            if st.button("➕ Nova Agenda", type="primary", width='stretch'):
                dialog_nova_agenda()
        with c5:
            st.write("")
            st.markdown("<div style='margin-top: 2px;'></div>", unsafe_allow_html=True)
            if st.button("🗑️ Excluir Agenda", type="secondary", width='stretch'):
                dialog_excluir_agenda(mes_sel, ano_sel)

        st.write("") # Espaçador para o grid

        # 0 = Segunda-feira, 6 = Domingo
        cal = calendar.Calendar(firstweekday=0) 
        month_days = cal.monthdatescalendar(ano_sel, mes_sel)
        
        today = dt.date.today()
        
        html_cal = '<div class="calendar-container"><div class="calendar-header">'
        for day_name in ["Segunda", "Terça", "Quarta", "Quinta", "Sexta", "Sábado", "Domingo"]:
            html_cal += f'<div>{day_name}</div>'
        html_cal += '</div><div class="calendar-grid">'
        
        for week in month_days:
            for day in week:
                is_other_month = day.month != mes_sel
                class_other = " other-month" if is_other_month else ""
                class_today = " today" if day == today else ""
                
                day_events = pd.DataFrame()
                if not df_mc.empty:
                    day_events = df_mc[df_mc["Data_dt"].dt.date == day]
                
                events_html = ""
                for _, ev in day_events.iterrows():
                    if ev["Sala"] > 0:
                        # Estilo: Evento Realizado (Azul Escuro/Institucional)
                        border_color = "var(--iam-navy)"
                        bg_color = "#DBEAFE"
                        text_color = "var(--iam-navy)"
                    else:
                        # Estilo: Evento Futuro (Azul Claro/Suave)
                        border_color = "var(--iam-info)"
                        bg_color = "#F0F9FF"
                        text_color = "var(--iam-royal)"
                        
                    events_html += f'''
                        <div class="cal-event" style="border-left-color: {border_color}; background-color: {bg_color}; color: {text_color};" title="Sala: {ev['Sala']} | Insc: {ev['Inscricoes']}">
                            <span class="cal-event-title">{ev['Cidade']}</span>
                            <span class="cal-event-sub">🎤 {ev['Palestrante']}</span>
                        </div>
                    '''
                
                html_cal += f'''
                    <div class="calendar-day{class_other}">
                        <div class="day-number-wrap">
                            <span class="day-number{class_today}">{day.day}</span>
                        </div>
                        <div class="day-events-wrapper">
                            {events_html}
                        </div>
                    </div>
                '''
        html_cal += '</div></div>'
        
        render_html(html_cal)
        
        st.markdown("<br>", unsafe_allow_html=True)
        st.caption("💡 Dica: Eventos já realizados (com presença > 0) aparecem em **azul escuro**. Eventos futuros aparecem em **azul claro**. Você pode rolar (scroll) dentro dos dias caso haja muitos eventos.")

def module_cadastro(cfg):
    section_header("Parametrização do Sistema", "Gestão de polos, cidades e palestrantes com visual mais compacto")
    t1, t2, t3 = st.tabs(["📍 Polos", "🏙️ Cidades", "🎤 Palestrantes"])

    def render_cad_simples(key_dict, obj_name):
        with st.container(border=True):
            with st.form(f"add_{key_dict}"):
                c1, c2 = st.columns([3, 1], gap="small")
                with c1:
                    novo = st.text_input(f"Adicionar {obj_name}")
                with c2:
                    st.write("")
                    submit = st.form_submit_button("Salvar", width='stretch')

                if submit and novo.strip():
                    cfg[key_dict] = sorted(list(set(cfg[key_dict] + [novo.strip()])))
                    if key_dict == "polos" and novo.strip() not in cfg["cidades_rel"]:
                        cfg["cidades_rel"][novo.strip()] = []
                    save_config(cfg)
                    ok_modal("Adicionado")

            st.dataframe(pd.DataFrame({obj_name: cfg[key_dict]}), hide_index=True, width='stretch')

            del_item = st.selectbox(f"Excluir {obj_name}", ["—"] + cfg[key_dict], key=f"del_{key_dict}")
            if del_item != "—" and st.button(f"🗑️ Remover {del_item}", key=f"btn_{key_dict}", type="secondary"):
                cfg[key_dict].remove(del_item)
                if key_dict == "polos" and del_item in cfg["cidades_rel"]:
                    del cfg["cidades_rel"][del_item]
                save_config(cfg)
                ok_modal("Removido")

    with t1: render_cad_simples("polos", "Polo")
    with t3: render_cad_simples("palestrantes", "Palestrante")

    with t2:
        with st.container(border=True):
            sel_polo_cad = st.selectbox("Selecione o Polo para visualizar ou editar as cidades:", cfg["polos"], key="sel_polo_cad")
            if sel_polo_cad:
                with st.form(f"add_cid_{sel_polo_cad}"):
                    c1, c2 = st.columns([3, 1], gap="small")
                    with c1:
                        novo_cid = st.text_input(f"Adicionar Cidade ao polo {sel_polo_cad}")
                    with c2:
                        st.write("")
                        submit_cid = st.form_submit_button("Salvar Cidade", width='stretch')

                    if submit_cid and novo_cid.strip():
                        if sel_polo_cad not in cfg["cidades_rel"]: cfg["cidades_rel"][sel_polo_cad] = []
                        cfg["cidades_rel"][sel_polo_cad] = sorted(list(set(cfg["cidades_rel"][sel_polo_cad] + [novo_cid.strip().upper()])))
                        save_config(cfg)
                        ok_modal(f"Cidade adicionada ao polo {sel_polo_cad}!")

                cidades_do_polo = cfg["cidades_rel"].get(sel_polo_cad, [])
                st.dataframe(pd.DataFrame({"Cidades atreladas": cidades_do_polo}), hide_index=True, width='stretch')

                if cidades_do_polo:
                    del_cid = st.selectbox(f"Excluir Cidade do polo {sel_polo_cad}", ["—"] + cidades_do_polo, key=f"del_cid_{sel_polo_cad}")
                    if del_cid != "—" and st.button(f"🗑️ Remover {del_cid}", key=f"btn_del_cid_{sel_polo_cad}", type="secondary"):
                        cfg["cidades_rel"][sel_polo_cad].remove(del_cid)
                        save_config(cfg)
                        ok_modal("Cidade removida!")

# =========================================================
# 8. GESTOR DE ROTAS (FLUXO PRINCIPAL)
# =========================================================
if st.session_state["page"] == "📊 DASH":
    module_dashboard(df_mc, df_mt, cfg)
elif st.session_state["page"] == "📝 Masterclass":
    module_masterclass(df_mc, cfg)
elif st.session_state["page"] == "🎯 Metas":
    module_metas(df_mt, cfg)
elif st.session_state["page"] == "💰 Comissões":
    module_comissoes(df_mc, cfg)
elif st.session_state["page"] == "📅 Agenda":
    module_agenda(df_mc, cfg)
elif st.session_state["page"] == "🧩 Cadastro":
    module_cadastro(cfg)

render_ok_modal()
