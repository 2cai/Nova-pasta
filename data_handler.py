import pandas as pd
import numpy as np
from ib_insync import IB, Stock, util
import time
import os


class DataHandler:
    def __init__(self, ib_host="127.0.0.1", ib_port=7497, client_id=1):
        self.ib = IB()
        self.ib.connect(ib_host, ib_port, client_id)
        self.file_path = "df_historico.csv"
        self.symbols = ["CL", "RB", "HO"]  # Crude Oil, Gasoline, Heating Oil
        self.data = None
        self.window_days = 30  # 30 dias de histórico

        # Carrega ou baixa histórico
        self.load_or_fetch_data()

    def load_or_fetch_data(self):
        """Carrega o histórico do CSV ou baixa os últimos 30 dias da IB API."""
        if os.path.exists(self.file_path):
            print("Carregando histórico local...")
            self.data = pd.read_csv(self.file_path, index_col="datetime", parse_dates=True)
            
            # Verifica a última data disponível e baixa os dados faltantes
            last_date = self.data.index.max()
            if last_date < pd.Timestamp.now().normalize() - pd.Timedelta(days=1):
                print("Atualizando dados faltantes...")
                missing_data = self.fetch_historical_data(start_date=last_date + pd.Timedelta(minutes=1))
                self.data = pd.concat([self.data, missing_data])
                self.data.to_csv(self.file_path)
        else:
            print("Baixando histórico de 30 dias...")
            self.data = self.fetch_historical_data()
            self.data.to_csv(self.file_path)

    def fetch_historical_data(self, start_date=None):
        """Solicita dados históricos minuto a minuto da IB API."""
        historical_data = []
        duration_str = "30 D" if start_date is None else "{} D".format((pd.Timestamp.now() - start_date).days)
        
        for symbol in self.symbols:
            contract = Stock(symbol, "NYMEX", "USD")  # Ajuste conforme necessário
            bars = self.ib.reqHistoricalData(
                contract,
                endDateTime="",
                durationStr=duration_str,
                barSizeSetting="1 min",
                whatToShow="TRADES",
                useRTH=False,
                formatDate=1
            )
            df = util.df(bars)
            df["symbol"] = symbol
            historical_data.append(df)

        df_all = pd.concat(historical_data).pivot(index="date", columns="symbol", values="close")
        df_all.index = pd.to_datetime(df_all.index)
        df_all.columns = ["CrudeOil", "Gasoline", "HeatingOil"]
        return df_all

    def update_data(self):
        """Atualiza os preços a cada minuto e salva no CSV."""
        new_data = {}

        for symbol in self.symbols:
            contract = Stock(symbol, "NYMEX", "USD")
            bars = self.ib.reqHistoricalData(
                contract,
                endDateTime="",
                durationStr="1 min",
                barSizeSetting="1 min",
                whatToShow="TRADES",
                useRTH=False,
                formatDate=1
            )
            if bars:
                new_data[symbol] = bars[-1].close

        if new_data:
            new_entry = pd.DataFrame([new_data], index=[pd.Timestamp.now()])
            self.data = pd.concat([self.data, new_entry])
            
            cutoff = pd.Timestamp.now() - pd.Timedelta(days=self.window_days)
            self.data = self.data[self.data.index > cutoff]
            
            self.data.to_csv(self.file_path)
            print("Dados atualizados:", new_entry)

    def run_live_updates(self):
        """Loop para atualizar os preços a cada 60 segundos."""
        while True:
            self.update_data()
            time.sleep(60)

if __name__ == "__main__":
    dh = DataHandler()
    dh.run_live_updates()
