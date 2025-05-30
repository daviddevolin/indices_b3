import streamlit as st
import pandas as pd
import yfinance as yf
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from datetime import datetime, timedelta
import os
import requests
from bs4 import BeautifulSoup
from tqdm import tqdm
import time
from zoneinfo import ZoneInfo

# Configuração da página
st.set_page_config(
    page_title="Dashboard Financeiro B3 - Tickers Ativos",
    page_icon="📈",
    layout="wide"
)

# Constantes
PASTA_DADOS = "data"
os.makedirs(PASTA_DADOS, exist_ok=True)
ARQUIVO_TICKERS = os.path.join(PASTA_DADOS, "top_15_tickers_validados.csv")

# Lista de fallback atualizada
TICKERS_FALLBACK = [
    "VALE3.SA", "PETR4.SA", "ITUB4.SA", "BBDC4.SA", "B3SA3.SA",
    "ABEV3.SA", "WEGE3.SA", "BBAS3.SA", "SUZB3.SA", "EQTL3.SA",
    "GGBR4.SA", "RENT3.SA", "LREN3.SA", "RAIL3.SA", "BPAC11.SA"
]

def verificar_ticker(ticker, dias_verificacao=5):
    try:
        acao = yf.Ticker(ticker)
        info = acao.info
        if not info.get('regularMarketPrice'):
            return False
        if info.get('currency', '').upper() != 'BRL':
            return False
        historico = acao.history(period=f"{dias_verificacao}d")
        if historico.empty or len(historico) < 3:
            return False
        if info.get('averageVolume', 0) < 100000:
            return False
        return True
    except Exception as e:
        print(f"Erro na verificação de {ticker}: {str(e)}")
        return False

def obter_tickers_do_yahoo():
    try:
        url = "https://finance.yahoo.com/quote/%5EBVSP/components/"
        headers = {'User-Agent': 'Mozilla/5.0'}
        response = requests.get(url, headers=headers, timeout=15)
        soup = BeautifulSoup(response.text, 'html.parser')
        tickers = []
        table = soup.find('table', {'class': 'W(100%)'})
        if table:
            for row in table.find_all('tr')[1:21]:
                cols = row.find_all('td')
                if cols and len(cols) > 1:
                    symbol = cols[0].text.strip()
                    if not symbol.endswith('.SA'):
                        symbol += '.SA'
                    tickers.append(symbol)
        return tickers if tickers else None
    except Exception as e:
        print(f"Erro ao obter tickers do Yahoo: {str(e)}")
        return None

def filtrar_tickers_ativos(tickers):
    ativos = []
    for ticker in tqdm(tickers, desc="Validando tickers"):
        if verificar_ticker(ticker):
            ativos.append(ticker)
        if len(ativos) >= 15:
            break
    return ativos

def obter_tickers_alternativos():
    return [
        "KLBN11.SA", "UGPA3.SA", "CCRO3.SA", "CYRE3.SA", "TOTS3.SA",
        "BRFS3.SA", "GOAU4.SA", "EMBR3.SA", "AZUL4.SA", "CSAN3.SA"
    ]

def obter_melhores_tickers():
    print("\n🔍 Iniciando busca por tickers válidos...")
    tickers = obter_tickers_do_yahoo()
    if tickers:
        validos = filtrar_tickers_ativos(tickers)
        if len(validos) >= 15:
            return validos[:15]
    print("⚠️ Usando lista fallback principal...")
    validos = filtrar_tickers_ativos(TICKERS_FALLBACK)
    if len(validos) >= 15:
        return validos[:15]
    print("⚠️ Completando com tickers alternativos...")
    extras = obter_tickers_alternativos()
    for t in extras:
        if len(validos) >= 15:
            break
        if t not in validos and verificar_ticker(t):
            validos.append(t)
    return validos[:15]

def salvar_tickers(tickers):
    dados = {
        'Ticker': tickers,
        'Data_Validacao': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        'Fonte': 'Yahoo Finance + Validação Direta'
    }
    df = pd.DataFrame(dados)
    df.to_csv(ARQUIVO_TICKERS, index=False)

@st.cache_data(ttl=86400)
def carregar_tickers_validados():
    if os.path.exists(ARQUIVO_TICKERS):
        modificado = os.path.getmtime(ARQUIVO_TICKERS)
        if (time.time() - modificado) < (7 * 86400):
            df = pd.read_csv(ARQUIVO_TICKERS)
            if not df.empty:
                return df['Ticker'].tolist()

    tickers = obter_melhores_tickers()
    if not tickers or len(tickers) == 0:
        st.warning("Nenhum ticker pôde ser validado. Usando fallback bruto.")
        tickers = TICKERS_FALLBACK

    salvar_tickers(tickers)
    return tickers

def formatar_moeda(valor):
    return f"R$ {valor:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")

def formatar_percentual(valor):
    return f"{valor:.2f}%"

@st.cache_data(ttl=3600)
def carregar_dados_historicos(ticker, periodo="1y"):
    try:
        # Mapeamento de períodos para o formato do yfinance
        periodo_map = {
            "1m": "1mo",
            "3m": "3mo",
            "6m": "6mo",
            "1y": "1y",
            "2y": "2y",
            "3y": "3y",
            "5y": "5y"
        }
        
        # Obter dados usando o parâmetro period que é mais confiável
        dados = yf.Ticker(ticker).history(
            period=periodo_map.get(periodo, "1y"),
            interval="1d",
            auto_adjust=False,
            prepost=False,
            actions=False
        )
        
        if dados.empty:
            st.warning(f"Nenhum dado encontrado para {ticker} no período selecionado")
            return None
        
        # Verificar dados essenciais
        colunas_essenciais = ['Open', 'High', 'Low', 'Close', 'Volume']
        for col in colunas_essenciais:
            if col not in dados.columns:
                st.error(f"Coluna {col} não encontrada nos dados")
                return None
        
        # Tratamento de fuso horário
        if dados.index.tz is None:
            dados.index = dados.index.tz_localize('UTC').tz_convert('America/Sao_Paulo')
        
        # Calcular indicadores técnicos (com verificação de dados suficientes)
        if len(dados) >= 20:
            dados['MM20'] = dados['Close'].rolling(window=20).mean()
        else:
            dados['MM20'] = None
            
        if len(dados) >= 50:
            dados['MM50'] = dados['Close'].rolling(window=50).mean()
        else:
            dados['MM50'] = None
            
        if len(dados) >= 200:
            dados['MM200'] = dados['Close'].rolling(window=200).mean()
        else:
            dados['MM200'] = None
            
        dados['Retorno_Diario'] = dados['Close'].pct_change() * 100
        dados['Volatilidade'] = dados['Retorno_Diario'].rolling(window=20).std()
        
        return dados.dropna(subset=['Open', 'High', 'Low', 'Close'])
        
    except Exception as e:
        st.error(f"Erro ao carregar dados para {ticker}: {str(e)}")
        return None

@st.cache_data(ttl=3600)
def obter_dados_fundamentalistas(ticker):
    try:
        acao = yf.Ticker(ticker)
        info = acao.info
        dados = {
            'ROE': info.get('returnOnEquity'),
            'Margem_Liquida': info.get('profitMargins'),
            'P/L': info.get('trailingPE'),
            'P/VP': info.get('priceToBook'),
            'Liquidez_Corrente': info.get('currentRatio'),
            'Dívida/Patrimônio': info.get('debtToEquity'),
            'Volume_Medio_3M': info.get('averageVolume')
        }
        for key in ['ROE', 'Margem_Liquida']:
            if dados[key] is not None and not isinstance(dados[key], str):
                dados[key] = dados[key] * 100
        return {k: v for k, v in dados.items() if v is not None}
    except Exception as e:
        st.warning(f"Não foi possível obter dados fundamentalistas para {ticker}")
        return {}

# Inicialização segura de sessão
if 'tickers_validados' not in st.session_state:
    try:
        st.session_state.tickers_validados = carregar_tickers_validados()
    except Exception as e:
        st.warning("Erro ao carregar tickers. Usando fallback.")
        st.session_state.tickers_validados = TICKERS_FALLBACK

if not st.session_state.tickers_validados:
    st.warning("Nenhum ticker pôde ser validado. Usando fallback.")
    st.session_state.tickers_validados = TICKERS_FALLBACK

if 'ticker_selecionado' not in st.session_state:
    if len(st.session_state.tickers_validados) > 0:
        st.session_state.ticker_selecionado = st.session_state.tickers_validados[0]
    else:
        st.error("Erro crítico: Nenhum ticker disponível.")
        st.stop()

# CSS personalizado
st.markdown("""
<style>
    .metric-card {
        background-color: var(--background-color);
        border-radius: 10px;
        padding: 15px;
        margin-bottom: 15px;
        box-shadow: 0 4px 6px rgba(0,0,0,0.1);
        border: 1px solid var(--border-color);
    }
    .positive { color: #28a745 !important; }
    .negative { color: #dc3545 !important; }
    [data-theme="dark"] .metric-card {
        --background-color: #2d2d2d;
        --border-color: #444;
        --text-color: #ffffff;
    }
    [data-theme="light"] .metric-card {
        --background-color: #f8f9fa;
        --border-color: #ddd;
        --text-color: #000000;
    }
    .metric-card h2, .metric-card h3 {
        color: var(--text-color) !important;
    }
    .stSelectbox div[data-baseweb="select"] {
        min-width: 250px;
    }
</style>
""", unsafe_allow_html=True)

def main():
    st.title("📊 Dashboard Financeiro - Top 15 Ações B3")

    # Inicialização do session_state
    if 'periodo_selecionado' not in st.session_state:
        st.session_state.periodo_selecionado = "6m"

    # Sidebar
    st.sidebar.header("Configurações")

    if st.sidebar.button("🔄 Atualizar Lista de Tickers"):
        st.cache_data.clear()
        st.session_state.tickers_validados = carregar_tickers_validados()
        st.rerun()

    # Widgets que atualizam o session_state
    novo_ticker = st.sidebar.selectbox(
        "Selecione o ativo:", 
        st.session_state.tickers_validados,
        index=st.session_state.tickers_validados.index(st.session_state.ticker_selecionado),
        key='select_ticker'
    )

    novo_periodo = st.sidebar.selectbox(
        "Período de análise:", 
        ["1m", "3m", "6m", "1y", "2y", "3y", "5y"],
        index=["1m", "3m", "6m", "1y", "2y", "3y", "5y"].index(st.session_state.periodo_selecionado),
        key='select_periodo'
    )

    # Verifica mudanças e atualiza
    if novo_ticker != st.session_state.ticker_selecionado or novo_periodo != st.session_state.periodo_selecionado:
        st.session_state.ticker_selecionado = novo_ticker
        st.session_state.periodo_selecionado = novo_periodo
        st.rerun()

    # Carregar dados com cache específico
    with st.spinner(f"Carregando dados para {st.session_state.ticker_selecionado}..."):
        dados = carregar_dados_historicos(st.session_state.ticker_selecionado, st.session_state.periodo_selecionado)
        dados_fundamentalistas = obter_dados_fundamentalistas(st.session_state.ticker_selecionado)

    if dados is None or dados.empty:
        st.error("Não foi possível carregar os dados para este ativo.")
        st.stop()

    # Debug (opcional)
    if st.sidebar.checkbox("Mostrar dados brutos (debug)"):
        st.write("Estatísticas dos dados:")
        st.write(dados.describe())
        st.write("Últimos 5 registros:")
        st.write(dados.tail())

    # Layout com abas
    tab1, tab2 = st.tabs(["📈 Análise Técnica", "💰 Análise Fundamentalista"])

    with tab1:
        col1, col2, col3, col4 = st.columns(4)
        
        variacao = ((dados['Close'].iloc[-1] / dados['Close'].iloc[0]) - 1) * 100
        variacao_class = "positive" if variacao >= 0 else "negative"
        
        with col1:
            st.markdown(f"""
            <div class="metric-card">
                <h3>Preço Atual</h3>
                <h2>{formatar_moeda(dados['Close'].iloc[-1])}</h2>
            </div>
            """, unsafe_allow_html=True)
        
        with col2:
            st.markdown(f"""
            <div class="metric-card">
                <h3>Variação Período</h3>
                <h2 class="{variacao_class}">{formatar_percentual(variacao)}</h2>
            </div>
            """, unsafe_allow_html=True)
        
        with col3:
            vol_medio = dados_fundamentalistas.get('Volume_Medio_3M', dados['Volume'].mean())
            st.markdown(f"""
            <div class="metric-card">
                <h3>Volume Médio (3M)</h3>
                <h2>{formatar_moeda(vol_medio)}</h2>
            </div>
            """, unsafe_allow_html=True)
        
        with col4:
            st.markdown(f"""
            <div class="metric-card">
                <h3>Volatilidade (20d)</h3>
                <h2>{formatar_percentual(dados['Volatilidade'].iloc[-1])}</h2>
            </div>
            """, unsafe_allow_html=True)
        
        # Gráfico de preços
        fig = make_subplots(rows=2, cols=1, shared_xaxes=True, 
                          vertical_spacing=0.05,
                          row_heights=[0.7, 0.3])
        
        fig.add_trace(
            go.Candlestick(
                x=dados.index,
                open=dados['Open'],
                high=dados['High'],
                low=dados['Low'],
                close=dados['Close'],
                name="Preço",
                increasing_line_color='#2ecc71',
                decreasing_line_color='#e74c3c'
            ),
            row=1, col=1
        )
        
        # Adicionar médias móveis apenas se existirem
        if 'MM20' in dados and not dados['MM20'].isnull().all():
            fig.add_trace(
                go.Scatter(
                    x=dados.index,
                    y=dados['MM20'],
                    name="MM20 (1 mês)",
                    line=dict(color='#f39c12', width=2)
                ),
                row=1, col=1
            )
        
        if 'MM50' in dados and not dados['MM50'].isnull().all():
            fig.add_trace(
                go.Scatter(
                    x=dados.index,
                    y=dados['MM50'],
                    name="MM50 (2.5 meses)",
                    line=dict(color='#3498db', width=2)
                ),
                row=1, col=1
            )
        
        if 'MM200' in dados and not dados['MM200'].isnull().all():
            fig.add_trace(
                go.Scatter(
                    x=dados.index,
                    y=dados['MM200'],
                    name="MM200 (10 meses)",
                    line=dict(color='#9b59b6', width=3)
                ),
                row=1, col=1
            )
        
        fig.add_trace(
            go.Bar(
                x=dados.index,
                y=dados['Volume'],
                name="Volume",
                marker_color='#7f8c8d',
                opacity=0.7
            ),
            row=2, col=1
        )
        
        fig.update_layout(
            title=f"{st.session_state.ticker_selecionado} - Análise Técnica | Período: {st.session_state.periodo_selecionado}",
            height=700,
            showlegend=True,
            hovermode="x unified",
            xaxis_rangeslider_visible=False,
            template="plotly_white",
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
        )
        
        fig.update_yaxes(title_text="Preço (R$)", row=1, col=1)
        fig.update_yaxes(title_text="Volume", row=2, col=1)
        
        st.plotly_chart(fig, use_container_width=True)

    with tab2:
        if not dados_fundamentalistas:
            st.warning("Dados fundamentalistas não disponíveis para este ativo.")
        else:
            st.header("📊 Indicadores Fundamentalistas")
            
            col1, col2 = st.columns(2)
            
            with col1:
                st.markdown("### 📈 Rentabilidade")
                st.metric("ROE (Retorno sobre Patrimônio)", 
                        formatar_percentual(dados_fundamentalistas.get('ROE', 0)),
                        help="Mede a eficiência na geração de lucros com o patrimônio líquido")
                
                st.metric("Margem Líquida", 
                        formatar_percentual(dados_fundamentalistas.get('Margem_Liquida', 0)),
                        help="Percentual de lucro líquido em relação à receita total")
            
            with col2:
                st.markdown("### 💰 Valuation")
                st.metric("P/L (Preço/Lucro)", 
                        f"{dados_fundamentalistas.get('P/L', 0):.2f}",
                        help="Razão entre preço da ação e lucro por ação")
                
            
            st.markdown("---")
            
            col3, col4 = st.columns(2)
            
            with col3:
                st.markdown("### 🏦 Endividamento")
                divida_patrimonio = dados_fundamentalistas.get('Dívida/Patrimônio', 0)
                st.metric("Dívida/Patrimônio", 
                        formatar_percentual(divida_patrimonio),
                        help="Razão entre dívida líquida e patrimônio líquido (em %)")
                    
            with col4:
                st.markdown("### 💧 Liquidez")
                st.metric("Liquidez Corrente", 
                        f"{dados_fundamentalistas.get('Liquidez_Corrente', 0):.2f}",
                        help="Capacidade de pagar obrigações de curto prazo")

    # Rodapé
    hora_atual = datetime.now(ZoneInfo('America/Sao_Paulo'))
    st.sidebar.markdown("---")
    st.sidebar.markdown(f"📅 Última atualização: {hora_atual.strftime('%d/%m/%Y %H:%M')}")
    st.sidebar.markdown("📊 Fonte: Yahoo Finance + Validação Direta")

if __name__ == "__main__":
    main()