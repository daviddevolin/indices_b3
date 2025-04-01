import yfinance as yf
import pandas as pd
from tqdm import tqdm
import requests
from bs4 import BeautifulSoup
import os
from datetime import datetime, timedelta

# Configurações
PASTA_DADOS = "data"
os.makedirs(PASTA_DADOS, exist_ok=True)
ARQUIVO_TICKERS = os.path.join(PASTA_DADOS, "top_15_tickers.csv")

# Lista de fallback atualizada com tickers mais estáveis
TICKERS_FALLBACK = [
    "VALE3.SA", "PETR4.SA", "ITUB4.SA", "BBDC4.SA", "B3SA3.SA",
    "ABEV3.SA", "WEGE3.SA", "BBAS3.SA", "SUZB3.SA", "EQTL3.SA",
    "GGBR4.SA", "RENT3.SA", "LREN3.SA", "RAIL3.SA", "BPAC11.SA"
]

def verificar_ticker(ticker, dias_verificacao=30):
    """Verifica robustamente se um ticker está ativo"""
    try:
        acao = yf.Ticker(ticker)
        
        # Verificação em 3 camadas
        info = acao.info
        if not info.get('regularMarketPrice'):
            return False
            
        # Verifica histórico recente
        historico = acao.history(period=f"{dias_verificacao}d")
        if historico.empty:
            return False
            
        # Verifica volume médio e moeda
        if (info.get('averageVolume', 0) < 100000 or 
            info.get('currency') != 'BRL'):
            return False
            
        return True
        
    except Exception as e:
        print(f"Erro na verificação de {ticker}: {str(e)}")
        return False

def obter_tickers_do_yahoo():
    """Obtém tickers brasileiros do Yahoo Finance com tratamento robusto"""
    try:
        url = "https://finance.yahoo.com/quote/%5EBVSP/components/"
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        
        response = requests.get(url, headers=headers, timeout=15)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.text, 'html.parser')
        tabela = soup.find('table', {'class': 'W(100%)'})
        
        tickers = []
        if tabela:
            for row in tabela.find_all('tr')[1:16]:  # Pega as 15 primeiras
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
    """Filtra apenas tickers ativos com validação rigorosa"""
    ativos = []
    for ticker in tqdm(tickers, desc="Validando tickers"):
        if verificar_ticker(ticker):
            ativos.append(ticker)
            if len(ativos) >= 15:
                break
    return ativos

def obter_tickers_alternativos():
    """Lista secundária de tickers para completar caso necessário"""
    return [
        "KLBN11.SA", "UGPA3.SA", "CCRO3.SA", "CYRE3.SA", "TOTS3.SA",
        "BRFS3.SA", "GOAU4.SA", "EMBR3.SA", "AZUL4.SA", "CSAN3.SA"
    ]

def obter_melhores_tickers():
    """Obtém os 15 melhores tickers com validação em tempo real"""
    print("\n🔍 Iniciando busca por tickers válidos...")
    
    # 1. Tentativa - Yahoo Finance
    tickers = obter_tickers_do_yahoo()
    if tickers:
        print("✅ Tickers obtidos do Yahoo Finance")
        validos = filtrar_tickers_ativos(tickers)
        if len(validos) >= 15:
            return validos[:15]
    
    # 2. Tentativa - Lista Fallback
    print("⚠️ Usando lista fallback principal...")
    validos = filtrar_tickers_ativos(TICKERS_FALLBACK)
    if len(validos) >= 15:
        return validos[:15]
    
    # 3. Tentativa - Lista Alternativa
    print("⚠️ Completando com tickers alternativos...")
    extras = obter_tickers_alternativos()
    for t in extras:
        if len(validos) >= 15:
            break
        if t not in validos and verificar_ticker(t):
            validos.append(t)
    
    return validos[:15]

def salvar_tickers(tickers):
    """Salva os tickers com metadados de validação"""
    dados = {
        'Ticker': tickers,
        'Data_Validacao': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        'Fonte': 'Yahoo Finance + Validação Direta'
    }
    df = pd.DataFrame(dados)
    df.to_csv(ARQUIVO_TICKERS, index=False)
    print(f"\n✅ {len(df)} tickers válidos salvos em {ARQUIVO_TICKERS}")

def carregar_tickers_validados():
    """Carrega tickers validados ou gera nova lista se necessário"""
    if os.path.exists(ARQUIVO_TICKERS):
        # Verifica se o arquivo tem menos de 3 dias
        modificado = os.path.getmtime(ARQUIVO_TICKERS)
        if (time.time() - modificado) < (3 * 86400):
            df = pd.read_csv(ARQUIVO_TICKERS)
            return df['Ticker'].tolist()
    
    # Se o arquivo não existe ou está desatualizado
    tickers = obter_melhores_tickers()
    salvar_tickers(tickers)
    return tickers

if __name__ == "__main__":
    import time
    
    # Obtém os tickers (carrega existente ou gera novos)
    tickers = carregar_tickers_validados()
    
    # Exibe resultado
    print("\n🎯 Top 15 Tickers Brasileiros Validados:")
    for i, ticker in enumerate(tickers, 1):
        print(f"{i:2d}. {ticker}")
    
    print("\n📌 Arquivo salvo:", ARQUIVO_TICKERS)