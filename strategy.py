import pandas as pd
import numpy as np

class Strategy:
    def __init__(self, data_handler, window=30, z_threshold=2.0):
        self.data_handler = data_handler
        self.window = window  # Janela para cálculo da média móvel e desvio padrão
        self.z_threshold = z_threshold  # Limite para entrada/saída de posições
        self.df = self.data_handler.data  # Carregar os dados históricos

    def calculate_crack_spread(self):
        """Calcula o Crack Spread com base nos preços atuais."""
        self.df['CrackSpread'] = (3 * self.df['CrudeOil'] -
                                  2 * self.df['Gasoline'] -
                                  1 * self.df['HeatingOil']) / 3

    def calculate_z_score(self):
        """Calcula o Z-score baseado na janela de retornos."""
        self.df['Spread_Returns'] = self.df['CrackSpread'].diff()
        rolling_mean = self.df['Spread_Returns'].rolling(window=self.window).mean()
        rolling_std = self.df['Spread_Returns'].rolling(window=self.window).std()
        self.df['ZScore'] = (self.df['Spread_Returns'] - rolling_mean) / rolling_std

    def generate_signals(self):
        """Define os pontos de entrada e saída com base no Z-score."""
        self.df['Signal'] = 0
        self.df.loc[self.df['ZScore'] > self.z_threshold, 'Signal'] = -1  # Venda
        self.df.loc[self.df['ZScore'] < -self.z_threshold, 'Signal'] = 1  # Compra
        self.df.loc[(self.df['ZScore'].abs() < 0.5), 'Signal'] = 0  # Fechar posição

    def run_strategy(self):
        """Executa toda a estratégia de cálculo e sinais."""
        self.calculate_crack_spread()
        self.calculate_z_score()
        self.generate_signals()
        return self.df[['CrackSpread', 'ZScore', 'Signal']]

if __name__ == "__main__":
    from data_handler_update import DataHandler
    
    data_handler = DataHandler()
    strategy = Strategy(data_handler)
    df_signals = strategy.run_strategy()
    print(df_signals.tail())
