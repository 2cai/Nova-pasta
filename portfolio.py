import pandas as pd
from ib_insync import IB, Position
import os

class Portfolio:
    def __init__(self, ib_host="127.0.0.1", ib_port=7497, client_id=3):
        self.ib = IB()
        self.ib.connect(ib_host, ib_port, client_id)
        self.file_path = "track_portfolio.csv"
        self.positions = self.load_portfolio()
    
    def load_portfolio(self):
        """Carrega o portfólio salvo ou inicializa um novo."""
        if os.path.exists(self.file_path):
            return pd.read_csv(self.file_path, index_col="symbol")
        else:
            return pd.DataFrame(columns=["symbol", "position", "avgPrice"]).set_index("symbol")
    
    def update_portfolio(self):
        """Atualiza o portfólio com os dados da IB."""
        ib_positions = self.ib.positions()
        portfolio_data = []
        
        for pos in ib_positions:
            portfolio_data.append({
                "symbol": pos.contract.symbol,
                "position": pos.position,
                "avgPrice": pos.avgCost
            })
        
        self.positions = pd.DataFrame(portfolio_data).set_index("symbol")
        self.positions.to_csv(self.file_path)
        print("Portfólio atualizado:")
        print(self.positions)
    
    def get_position(self, symbol):
        """Obtém a posição atual de um ativo."""
        if symbol in self.positions.index:
            return self.positions.loc[symbol]
        return None

if __name__ == "__main__":
    portfolio = Portfolio()
    portfolio.update_portfolio()
