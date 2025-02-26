"""
Create a script that loads csv file by given filename. The file has two columns "Date" and "Price". Price is in EUR/kWh. Date granularity is 1 hour and is in ISO8601 format (for example "2023-01-01T00:00:00Z").

We have a battery that can collect energy in kWh. A battery has some parameters:
- capacity: maximum amount of energy (in kWh) that can be stored;
- speed: how many kWh can be stored in one hour;
- efficiency: how many kWh can be returned to the grid from 1 kWh of energy (0 to 1 float).

We have a grid that can buy and sell energy. The grid "knows" prices only for today and at evening it also can "to know" prices for tomorrow. The grid has some parameters:
- day_ahead_time: the hour (0-23) when the grid "get knowledge" about prices for tomorrow.
- profit_min: the minimum profit (in EUR) that we want to get from one kWh. Be aware that battery has efficiency parameter.
- tax_fixed_returnable: fixed tax (in EUR) that we have to pay for every kWh that we consume from the grid. When we sell energy to the grid, we get this tax back (proportionally efficiency).

Script gets the data from the file and calculates the best strategy for the battery to buy and sell energy. The strategy is a list of actions (buy/sell) with the amount of energy (in kWh) and the price (in EUR/kWh).
battery starts with 0 energy.
Scripts runs through the data and calculates the best strategy for the battery. Script has to analise day by day and decide when battery buys energy from grid and when sells energy to grid. The goal is to maximize the profit.
Take into account that strategy can read prices for a next day at "day_ahead_time". It means if it had a plan to buy/sell energy the current day, it can change it after "day_ahead_time" if it is more profitable.
battery can buy energy or sell energy in one hour (not both in one time). Take into account that battery has a speed parameter. It means if for example its capacity is 10 kwhs and speed is 2 kwhs/hour, it can sell or buy 2 kwhs in the hour, not all 10. But, potentially, it can buy 5 hours in a row and then sell 5 hours in a row or combine buying and selling in any other way.

As a result script saves the result in a dataframe with the next columns:
- date: date of the action (including hour);
- action: "buy" or "sell" (buy means that battery buys energy from the grid and charges, sell means that battery sells energy to the grid, discharge);
- amount_kwh: amount of energy (in kWh) that battery buys or sells;
- amount_eur: amount of money (in EUR) that battery buys or sells;
- price: price of energy (in EUR/kWh);
- balance: current balance of the battery (in kWh).
- cycles: number of cycles (buy/sell) that battery has done. It means that if the action is buying energy, cycles is increased by 1. If the action is selling energy, cycles does not change.
- capacity_cycles: number of cycles that battery has used from its capacity. For example, capacity is 10 kWh, when we buy 3 kWh, capacity_cycles is 0.3. When we sell, capacity_cycles is not changing. Other words it shows how much time we have charged our battery with a full capacity.
- profit: profit of the action (in EUR) when selling energy to the grid. If the action is buying energy, profit is 0. It means battery has to remember buying price of every transaction and calculate profit when selling energy.
- month_profit: profit of the battery (in EUR) for the month. It means that if the action is buying energy, month profit is not changed. If the action is selling energy, month profit is increased by the profit of the action.
- total_profit: total profit of the battery (in EUR) after the action. It means that if the action is buying energy, total profit is not changed. If the action is selling energy, total profit is increased by the profit of the action.

Then dataframe has to be saved to the csv file, and its name must include all parameters of battery and grid.
"""

import pandas as pd

class BatteryTrading:
    def __init__(self, filename, capacity, speed, efficiency, day_ahead_time, profit_min, tax_fixed_returnable):
        """
        Initialize the battery trading strategy.
        :param filename: CSV file containing energy prices.
        :param capacity: Maximum energy storage capacity in kWh.
        :param speed: Max charge/discharge speed in kWh per hour.
        :param efficiency: Energy efficiency when selling (0-1).
        :param day_ahead_time: Hour when next day's prices become available.
        :param profit_min: Minimum profit for trading 1 kWh.
        :param tax_fixed_returnable: Fixed tax for buying and selling energy.
        """
        self.filename = filename
        self.capacity = capacity
        self.speed = speed
        self.efficiency = efficiency
        self.day_ahead_time = day_ahead_time
        self.profit_min = profit_min
        self.tax_fixed_returnable = tax_fixed_returnable

        # Load data from CSV
        self.data = pd.read_csv(filename)
        self.data['Date'] = pd.to_datetime(self.data['Date'])
        self.data.set_index('Date', inplace=True)
        # Remove duplicate dates
        self.data = self.data.drop_duplicates()

        # Initialize battery state and tracking variables
        self.balance = 0
        self.cycles = 0
        self.capacity_cycles = 0
        self.total_profit = 0
        self.monthly_profit = 0
        self.transactions = []  # List to store transaction history

        self.month_current = 0

    def run(self):
        """
        Main function to process daily energy trading.
        """
        buy_log = []  # Keeps track of previous purchases

        last_sell_time = None
        for date, day_data in self.data.resample('D'):
            # Find best buy/sell opportunities for the current day
            best_trades = self.find_best_trade(day_data)
            for buy_time, sell_time, profit_kwh in best_trades:
                if last_sell_time is None or buy_time > last_sell_time:
                    self.execute_trade(buy_time, sell_time, profit_kwh, buy_log)
                last_sell_time = sell_time

            # If the current hour is when next day's prices are known, plan for tomorrow
            # if date.hour == self.day_ahead_time:
            #     next_day_data = self.data.loc[date + pd.Timedelta(days=1):date + pd.Timedelta(days=1, hours=23)]
            #     best_trades_next = self.find_best_trade(next_day_data)
            #     for buy_time, sell_time, amount in best_trades_next:
            #         self.execute_trade(buy_time, sell_time, amount, buy_log)

        self.save_results()

    def find_best_trade(self, day_data):
        """
        Identifies the best buy and sell points for the given day's data.
        :param day_data: Data for a single day.
        :return: List of (buy_time, sell_time, profit_kwh) tuples.
        """
        best_trades = []

        for buy_timestamp, buy_row in day_data.iterrows():
            buy_price = buy_row['Price']
            for sell_timestamp, sell_row in day_data.iterrows():
                if sell_timestamp > buy_timestamp:
                    sell_price = sell_row['Price']
                    profit_kwh = (sell_price + self.tax_fixed_returnable) * self.efficiency - (buy_price + self.tax_fixed_returnable)
                    if profit_kwh >= self.profit_min:
                        best_trades.append((buy_timestamp, sell_timestamp, profit_kwh))

        # Sort trades by profit.
        best_trades.sort(key=lambda x: x[2], reverse=True)

        # Remove used timestamps.
        grouped_best_trades = []
        used_timestamps = []
        for buy_timestamp, sell_timestamp, profit in best_trades:
            if buy_timestamp not in used_timestamps and sell_timestamp not in used_timestamps:
                grouped_best_trades.append((buy_timestamp, sell_timestamp, profit))
                used_timestamps.append(buy_timestamp)
                used_timestamps.append(sell_timestamp)

        return grouped_best_trades

    def execute_trade(self, buy_time, sell_time, amount, buy_log):
        """
        Executes a trade by first buying then selling energy.
        :param buy_time: Timestamp of the buy action.
        :param sell_time: Timestamp of the sell action.
        :param amount: Amount of energy in kWh.
        :param buy_log: Log of past buy transactions.
        """
        amount_kwh = min(self.speed, self.capacity - self.balance)
        buy_price = self.data.loc[buy_time, 'Price']
        sell_price = self.data.loc[sell_time, 'Price']

        # Perform the buy and sell actions
        self.buy_energy(buy_time, amount_kwh, buy_price, buy_log)
        self.sell_energy(sell_time, sell_price, buy_log)

    def buy_energy(self, timestamp, amount_kwh, price, buy_log):
        """
        Processes a buy transaction.
        :param timestamp: Buy timestamp.
        :param amount_kwh: Amount bought in kWh.
        :param price: Price per kWh.
        :param buy_log: Log of past buy transactions.
        """
        self.balance += amount_kwh
        self.cycles += 1
        self.capacity_cycles += amount_kwh / self.capacity
        buy_log.append((timestamp, amount_kwh, price))  # Store buy transaction

        self.transactions.append([timestamp, 'buy', amount_kwh, amount_kwh * price, price, self.balance, self.cycles, self.capacity_cycles, 0, self.monthly_profit, self.total_profit])

    def sell_energy(self, timestamp, price, buy_log):
        """
        Processes a sell transaction by matching it with previous buys.
        :param timestamp: Sell timestamp.
        :param price: Selling price per kWh.
        :param buy_log: Log of past buy transactions.
        """
        for buy_time, buy_amount, buy_price in buy_log:
            if self.balance > 0:
                amount_kwh = min(self.speed, self.balance, buy_amount)

                profit = (amount_kwh * (price + self.tax_fixed_returnable) * self.efficiency) - (amount_kwh * (buy_price + self.tax_fixed_returnable))

                if self.month_current != timestamp.month:
                    self.month_current = timestamp.month
                    self.monthly_profit = 0

                self.total_profit += profit
                self.monthly_profit += profit
                self.balance -= amount_kwh

                buy_log.remove((buy_time, buy_amount, buy_price))  # Remove used buy transaction

                self.transactions.append([timestamp, 'sell', amount_kwh, amount_kwh * price * self.efficiency, price, self.balance, self.cycles, self.capacity_cycles, profit, self.monthly_profit, self.total_profit])
                break

    def save_results(self):
        """
        Saves the transaction history to a CSV file.
        """
        output_filename = f"strategy_capacity{self.capacity}_speed{self.speed}_eff{self.efficiency}_profit_min{self.profit_min}_dayahead{self.day_ahead_time}_tax_fixed_returnable{self.tax_fixed_returnable}.csv"
        df = pd.DataFrame(self.transactions, columns=['date', 'action', 'amount_kwh', 'amount_eur', 'price', 'balance', 'cycles', 'capacity_cycles', 'profit', 'month_profit', 'total_profit'])
        df.to_csv(output_filename, index=False)

if __name__ == "__main__":
    strategy = BatteryTrading("energy_prices_EPEX_NL_nobtw_2023_2024.csv", capacity=5, speed=5, efficiency=0.95, day_ahead_time=16, profit_min=0.03, tax_fixed_returnable=0.17)
    strategy.run()
