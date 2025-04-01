import pandas as pd
import yfinance as yf
from datetime import datetime, timedelta
from tqdm import tqdm
import os
import requests
from bs4 import BeautifulSoup

# Configurações
PASTA_DADOS = "data"
ARQUIVO_CONSOLIDADO = os.path.join(PASTA_DADOS, "dados_consolidados_top15.csv")
ARQUIVO_TICKERS = os.path.join(PASTA_DADOS, "top_15_tickers.csv")

# Criar pasta de dados se não existir
os.makedirs(PASTA_DADOS, exist_ok=True)

def obter_top_15_tickers():
    """Obtém os 15 tickers mais líquidos do mercado brasileiro"""
    try:
        # Tenta obter do Yahoo Finance
        url = "https://finance.yahoo.com/quote/%5EBVSP/components/"
        headers = {'User-Agent': 'Mozilla/5.0'}
        response = requests.get(url, headers=headers, timeout=10)
        soup = BeautifulSoup(response.text, 'html.parser')
        
        tickers = []
        table = soup.find('table')
        if table:
            for row in table.find_all('tr')[1:16]:  # Pega as 15 primeiras ações
                cols = row.find_all('td')
                if cols:
                    symbol = cols[0].text.strip()
                    if '.SA' not in symbol:
                        symbol += '.SA'
                    tickers.append(symbol)
        
        # Fallback se não conseguir do Yahoo
        if not tickers:
            tickers = [
                "VALE3.SA", "PETR4.SA", "ITUB4.SA", "BBDC4.SA", "B3SA3.SA",
                "ABEV3.SA", "WEGE3.SA", "BBAS3.SA", "PETR3.SA", "SUZB3.SA",
                "EQTL3.SA", "GGBR4.SA", "JBSS3.SA", "RENT3.SA", "HAPV3.SA"
            ]
        
        # Verifica quais tickers estão realmente ativos
        tickers_ativos = []
        for ticker in tqdm(tickers, desc="Verificando tickers"):
            try:
                if yf.Ticker(ticker).history(period="1d").shape[0] > 0:
                    tickers_ativos.append(ticker)
                    if len(tickers_ativos) >= 15:
                        break
            except:
                continue
        
        # Salva a lista de tickers para referência futura
        pd.DataFrame(tickers_ativos, columns=["Ticker"]).to_csv(ARQUIVO_TICKERS, index=False)
        return tickers_ativos
    
    except Exception as e:
        print(f"Erro ao obter top 15 tickers: {e}")
        # Se tudo falhar, usa uma lista de fallback verificada
        return [
            "VALE3.SA", "PETR4.SA", "ITUB4.SA", "BBDC4.SA", "B3SA3.SA",
            "ABEV3.SA", "WEGE3.SA", "BBAS3.SA", "SUZB3.SA", "EQTL3.SA",
            "GGBR4.SA", "RENT3.SA", "LREN3.SA", "RAIL3.SA", "BPAC11.SA"
        ]

def coletar_dados_historicos(ticker):
    try:
        acao = yf.Ticker(ticker)
        
        # Datas para os últimos 3 anos (mais robusto)
        fim = datetime.today()
        inicio = fim - timedelta(days=3*365)
        
        # Coleta de dados históricos com tratamento de falhas
        historico = acao.history(
            start=inicio,
            end=fim,
            interval="1d",
            auto_adjust=False,
            actions=False
        )
        
        if historico.empty:
            return pd.DataFrame()
        
        # Selecionar e formatar colunas
        historico = historico[["Open", "High", "Low", "Close", "Volume"]].copy()
        historico.reset_index(inplace=True)
        historico["Date"] = pd.to_datetime(historico["Date"]).dt.date
        
        # Arredondar e formatar valores
        for col in ["Open", "High", "Low", "Close"]:
            historico[col] = historico[col].round(2).astype(str).str.replace(".", ",")
        
        historico["Volume"] = historico["Volume"].round().astype(int).astype(str)
        historico["Ticker"] = ticker
        
        return historico

    except Exception as e:
        print(f"Erro ao coletar dados históricos para {ticker}: {e}")
        return pd.DataFrame()

def coletar_dados_fundamentalistas(ticker):
    try:
        acao = yf.Ticker(ticker)
        info = acao.info
        
        # Mapeamento de indicadores com tratamento robusto
        indicadores = {
            "Liquidez_Corrente": info.get("currentRatio"),
            "Divida_Patrimonio": info.get("debtToEquity"),
            "Lucro_por_Acao": info.get("trailingEps"),
            "ROE": info.get("returnOnEquity"),
            "Margem_Liquida": info.get("profitMargins"),
            "P/L": info.get("trailingPE"),
            "Dividend_Yield": info.get("dividendYield"),
            "Fonte_Dados": "Yahoo Finance"
        }
        
        # Formatação dos valores
        for key, value in indicadores.items():
            if value is None:
                continue
            if isinstance(value, (int, float)):
                if key == "Dividend_Yield" and value < 1:  # Assume que está em decimal
                    value *= 100
                indicadores[key] = str(round(value, 2)).replace(".", ",")
        
        indicadores["Ticker"] = ticker
        return indicadores

    except Exception as e:
        print(f"Erro ao coletar dados fundamentalistas para {ticker}: {e}")
        return None

def coletar_dados():
    # Obtém os top 15 tickers ativos
    tickers = obter_top_15_tickers()
    print(f"\n🔎 Coletando dados para {len(tickers)} tickers:")
    print(", ".join(tickers))
    
    dados_consolidados = []
    
    for ticker in tqdm(tickers, desc="Processando tickers"):
        historico = coletar_dados_historicos(ticker)
        
        if not historico.empty:
            fund = coletar_dados_fundamentalistas(ticker)
            
            if fund:
                # Adiciona indicadores fundamentalistas
                for col, val in fund.items():
                    if col != "Ticker":
                        historico[col] = val
                
                dados_consolidados.append(historico)
    
    if dados_consolidados:
        df_consolidado = pd.concat(dados_consolidados, ignore_index=True)
        
        # Ordenação e seleção de colunas
        colunas_prioritarias = [
            "Date", "Ticker", "Open", "High", "Low", "Close", "Volume",
            "P/L", "Dividend_Yield", "ROE", "Margem_Liquida",
            "Liquidez_Corrente", "Divida_Patrimonio", "Fonte_Dados"
        ]
        
        # Mantém apenas colunas existentes
        colunas = [col for col in colunas_prioritarias if col in df_consolidado.columns]
        df_consolidado = df_consolidado[colunas]
        
        # Salva os dados
        df_consolidado.to_csv(ARQUIVO_CONSOLIDADO, index=False, sep="|", encoding="utf-8")
        print(f"\n✅ Dados consolidados salvos em {ARQUIVO_CONSOLIDADO}")
        print(f"📊 Total de registros: {len(df_consolidado)}")
        
        return df_consolidado
    else:
        print("\n⚠️ Nenhum dado válido foi coletado")
        return pd.DataFrame()

# Execução principal
if __name__ == "__main__":
    dados = coletar_dados()