import pandas as pd
import yfinance as yf
from datetime import datetime
from tqdm import tqdm

# Carregar a lista de ações
acoes = pd.read_csv("data/tickers_ativos_yahoo.csv")
tickers = acoes['Ticker'].tolist()  # Nome da coluna no CSV

# Função para coletar dados históricos
def coletar_dados(ticker):
    try:
        acao = yf.Ticker(ticker)
        
        # Datas para os últimos 3 anos
        fim = datetime.today().strftime("%Y-%m-%d")
        inicio = (datetime.today().replace(year=datetime.today().year - 3)).strftime("%Y-%m-%d")
        
        # Coleta de dados históricos
        historico = acao.history(start=inicio, end=fim, interval="1d")
        
        # Selecionar apenas as colunas desejadas
        historico = historico[["Open", "High", "Low", "Close", "Volume"]]
        historico.reset_index(inplace=True)  # Resetar o índice para incluir a coluna "Date"
        
        # Formatar a coluna "Date" para remover a parte do horário
        historico["Date"] = pd.to_datetime(historico["Date"]).dt.date
        
        # Arredondar os valores para 2 casas decimais
        historico = historico.round({"Open": 2, "High": 2, "Low": 2, "Close": 2, "Volume": 2})
        
        # Substituir pontos por vírgulas nos valores decimais
        for col in ["Open", "High", "Low", "Close", "Volume"]:
            historico[col] = historico[col].astype(str).str.replace(".", ",")
        
        historico["Ticker"] = ticker  # Adicionar coluna com o ticker
        
        return historico

    except Exception as e:
        print(f"Erro ao coletar dados para {ticker}: {e}")
        return pd.DataFrame()

# Loop para coletar dados de todas as ações
resultado = []
for ticker in tqdm(tickers, desc="Coletando dados"):
    dados = coletar_dados(ticker)
    if not dados.empty:
        resultado.append(dados)

# Concatenar todos os resultados
if resultado:
    df_final = pd.concat(resultado)
    
    # Salvar o DataFrame em um arquivo CSV
    # Usar barra (|) como separador de colunas
    df_final.to_csv("data/dados_historicos.csv", index=False, sep="|", encoding="utf-8")
    print("✅ Dados coletados e salvos em 'data/dados_historicos.csv'")
else:
    print("❌ Nenhum dado foi coletado.")