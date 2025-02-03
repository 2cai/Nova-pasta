from ib_insync import IB, MarketOrder
import time

class Execution:
    def __init__(self, ib_host="127.0.0.1", ib_port=7497, client_id=2):
        self.ib = IB()
        self.ib.connect(ib_host, ib_port, client_id)
        self.positions = {}

    def place_order(self, contract, action, quantity):
        """Envia uma ordem de mercado para a IB."""
        order = MarketOrder(action, quantity)
        trade = self.ib.placeOrder(contract, order)
        self.ib.sleep(1)  # Aguarda confirmação
        print(f"Ordem enviada: {action} {quantity} {contract.symbol}")
        return trade

    def manage_positions(self, df_signals):
        """Verifica os sinais de trading e executa as ordens."""
        for index, row in df_signals.iterrows():
            signal = row['Signal']
            contract = None
            quantity = 1  # Ajuste o tamanho do lote conforme necessário

            if signal == 1:  # Compra
                contract = self.create_contract("CL")  # Exemplo para Crude Oil
                self.place_order(contract, "BUY", quantity)
                self.positions["CL"] = quantity

            elif signal == -1:  # Venda
                contract = self.create_contract("CL")
                self.place_order(contract, "SELL", quantity)
                self.positions["CL"] = -quantity

            elif signal == 0 and "CL" in self.positions:  # Fechar posição
                contract = self.create_contract("CL")
                action = "SELL" if self.positions["CL"] > 0 else "BUY"
                self.place_order(contract, action, abs(self.positions["CL"]))
                del self.positions["CL"]

    def create_contract(self, symbol):
        """Cria um contrato para negociação no Interactive Brokers."""
        return self.ib.qualifyContracts(
            Stock(symbol, "NYMEX", "USD")
        )[0]

if __name__ == "__main__":
    from strategy import Strategy
    from data_handler_update import DataHandler
    
    data_handler = DataHandler()
    strategy = Strategy(data_handler)
    df_signals = strategy.run_strategy()
    
    execution = Execution()
    execution.manage_positions(df_signals)
