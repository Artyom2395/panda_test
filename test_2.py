import pandas as pd
import json


class DataProcessor:
    def __init__(self, filename):
        self.filename = filename
        self.data = self.load_data()
        self.df = pd.json_normalize(self.data, record_path='products', meta=["order_id", "warehouse_name", "highway_cost"])

    def load_data(self):
        with open(self.filename, 'r', encoding='utf-8') as f:
            data = json.load(f)
        return data
    
    #Считаем тариф
    def calculate_tariffs(self):
        highway_cost_sum = self.df.drop_duplicates(subset=["order_id"]).groupby(["warehouse_name"])["highway_cost"].sum()
        grouped_data = self.df.groupby('warehouse_name')['quantity'].sum()
        result = abs(highway_cost_sum / grouped_data)
        result_df = pd.DataFrame({"warehouse_name": result.index, "tarif": result.values})
        self.df = self.df.merge(result_df, on="warehouse_name", how="left")
    
    #Заносим данные доход, расход, прибыль
    def calculate_profit(self):
        self.df['income'] = self.df['price'] * self.df['quantity']
        self.df['expenses'] = self.df['tarif'] * self.df['quantity']
        self.df['profit'] = self.df['income'] - self.df['expenses']
    
    #Суммарные данные для каждого товара
    def calculate_product_summary(self):
        self.result_pd2 = self.df.groupby('product').agg(total_income=('income', 'sum'),
                                                         total_quantity=('quantity', 'sum'),
                                                         total_expenses=('expenses', 'sum'),
                                                         total_profit=('profit', 'sum'))
    
    #Прибыль полученная с заказа, и средняя
    def calculate_order_summary(self):
        grouped_df = self.df.groupby("order_id").agg({"profit": "sum"}).reset_index()
        grouped_df["avg_profit_per_order"] = self.df.groupby("order_id")["profit"].mean().reset_index()["profit"]
        self.order_summary_df = grouped_df
    
    #Процент прибыли продукта заказанного из определенного склада к прибыли этого склада, накопленный процент, категории
    def calculate_percent_profit_product_of_warehouse(self):
        grouped_profit = self.df.groupby(["warehouse_name", "product"])[["profit", "quantity"]].sum().reset_index()
        grouped_profit["percent_profit_product_of_warehouse"] = grouped_profit.groupby("warehouse_name")["profit"].transform(lambda x: (x / x.sum()) * 100)
        grouped_profit["percent_profit_product_of_warehouse"] = pd.to_numeric(grouped_profit["percent_profit_product_of_warehouse"], errors='coerce')
        
        sorted_df = grouped_profit.sort_values(by="percent_profit_product_of_warehouse", ascending=False)
        sorted_df["accumulated_percent_profit_product_of_warehouse"] = sorted_df.groupby("warehouse_name")["percent_profit_product_of_warehouse"].cumsum()

        def categorize_percent(percent):
            if percent <= 70:
                return 'A'
            elif 70 < percent <= 90:
                return 'B'
            else:
                return 'C'

        sorted_df["category"] = sorted_df["accumulated_percent_profit_product_of_warehouse"].apply(categorize_percent)
        self.percent_profit_product_of_warehouse_df = sorted_df

    def process_data(self):
        self.calculate_tariffs()
        self.calculate_profit()
        self.calculate_product_summary()
        self.calculate_order_summary()
        self.calculate_percent_profit_product_of_warehouse()

if __name__ == "__main__":
    filename = 'trial_task.json'
    data_processor = DataProcessor(filename)
    data_processor.process_data()
    #для просмотра таблиц в консоли
    print(data_processor.df)