import yfinance as yf
import pandas as pd
from tqdm import tqdm
import investpy  # Ou outra biblioteca para obter a lista de tickers da B3

def obter_tickers_b3():
    """Obtém a lista de tickers da B3."""
    acoes_br = investpy.get_stocks(country="brazil")
    tickers_b3 = acoes_br["symbol"].tolist()
    tickers_b3 = [ticker + ".SA" for ticker in tickers_b3]  # Adiciona sufixo .SA
    return tickers_b3

def verificar_tickers_ativos(tickers):
    """Verifica quais tickers estão ativos no Yahoo Finance."""
    tickers_ativos = []
    for ticker in tqdm(tickers, desc="Verificando tickers"):
        try:
            acao = yf.Ticker(ticker)
            if acao.info.get("regularMarketPrice") is not None:
                tickers_ativos.append(ticker)
        except Exception as e:
            print(f"Erro ao verificar {ticker}: {e}")
    return tickers_ativos

def salvar_tickers(tickers, arquivo):
    """Salva a lista de tickers em um arquivo CSV com uma coluna 'Ticker'."""
    df = pd.DataFrame(tickers, columns=["Ticker"])  # Cria um DataFrame com a coluna "Ticker"
    df.to_csv(arquivo, index=False)  # Salva o DataFrame em um arquivo CSV
    print(f"✅ {len(tickers)} tickers salvos em '{arquivo}'.")

if __name__ == "__main__":
    # Passo 1: Obter tickers da B3
    tickers_b3 = obter_tickers_b3()
    salvar_tickers(tickers_b3, "data/tickers_b3.csv")

    # Passo 2: Verificar tickers ativos no Yahoo Finance
    tickers_ativos = verificar_tickers_ativos(tickers_b3)
    salvar_tickers(tickers_ativos, "data/tickers_ativos_yahoo.csv")