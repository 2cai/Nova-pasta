from data_handler_update import DataHandler
from strategy import Strategy
from execution import Execution
from portfolio import Portfolio
import time

def main():
    print("Iniciando o sistema de trading...")
    
    # Inicializa os módulos
    data_handler = DataHandler()
    portfolio = Portfolio()
    execution = Execution()
    
    while True:
        # Atualiza os dados de mercado
        data_handler.update_data()
        
        # Calcula sinais de trading
        strategy = Strategy(data_handler)
        df_signals = strategy.run_strategy()
        
        # Executa ordens com base nos sinais
        execution.manage_positions(df_signals)
        
        # Atualiza o portfólio
        portfolio.update_portfolio()
        
        # Aguarda 60 segundos para a próxima iteração
        time.sleep(60)

if __name__ == "__main__":
    main()
