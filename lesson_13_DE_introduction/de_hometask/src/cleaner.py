import pandas as pd
import re
import logging
import os
from datetime import datetime

current_script_path = os.path.abspath(__file__)
src_dir = os.path.dirname(current_script_path)
base_dir = os.path.dirname(src_dir)

data_dir = os.path.join(base_dir, 'data')
logs_dir = os.path.join(base_dir, 'logs')
output_dir = os.path.join(base_dir, 'output')

for folder in [logs_dir, output_dir]:
    os.makedirs(folder, exist_ok=True)

log_filename = f"pipeline_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
log_file = os.path.join(logs_dir, log_filename)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file, mode='w', encoding='utf-8'),
        logging.StreamHandler()
    ]
)

class PipelineStats:
    def __init__(self):
        self.report = []

    def add_stat(self, table, issue, count):
        if count > 0:
            self.report.append({
                'Table': table,
                'Issue': issue,
                'Removed_Rows': count
            })

stats = PipelineStats()


def clean_customers(df):
    table = "Customers"
    initial_len = len(df)
    
    dup_ids = df[df['customer_id'].duplicated() | df['customer_id'].isna()]
    stats.add_stat(table, "Duplicate/Null IDs", len(dup_ids))
    df = df.drop_duplicates(subset=['customer_id']).dropna(subset=['customer_id'])
    
    email_pattern = r'^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$'
    invalid_emails = df[~df['email'].astype(str).str.contains(email_pattern, na=False)]
    stats.add_stat(table, "Invalid Emails", len(invalid_emails))
    df = df[df['email'].astype(str).str.contains(email_pattern, na=False)]
    
    dates = pd.to_datetime(df['created_at'], errors='coerce')
    stats.add_stat(table, "Invalid Timestamps", dates.isna().sum())
    df['created_at'] = dates
    df = df.dropna(subset=['created_at'])
    
    logging.info(f"Таблиця {table} очищена. Залишилось {len(df)} з {initial_len} рядків.")
    return df

def clean_products(df):
    table = "Products"
    initial_len = len(df)
    
    bad_ids = df[df['product_id'].duplicated() | df['product_id'].isna()]
    stats.add_stat(table, "Duplicate/Null IDs", len(bad_ids))
    df = df.drop_duplicates(subset=['product_id']).dropna(subset=['product_id'])
    
    bad_prices = df[df['price'] <= 0]
    stats.add_stat(table, "Zero/Negative Prices", len(bad_prices))
    df = df[df['price'] > 0]
    
    logging.info(f"Таблиця {table} очищена. Залишилось {len(df)} з {initial_len} рядків.")
    return df

def clean_orders(df):
    table = "Orders"
    initial_len = len(df)
    
    bad_ids = df[df['order_id'].duplicated() | df['order_id'].isna()]
    stats.add_stat(table, "Duplicate/Null IDs", len(bad_ids))
    df = df.drop_duplicates(subset=['order_id']).dropna(subset=['order_id'])
    
    df['order_status'] = df['order_status'].astype(str).str.lower().str.strip()
    valid_statuses = ['pending', 'completed', 'cancelled']
    invalid_status_rows = df[~df['order_status'].isin(valid_statuses)]
    stats.add_stat(table, "Unknown Statuses", len(invalid_status_rows))
    df = df[df['order_status'].isin(valid_statuses)]
    
    dates = pd.to_datetime(df['created_at'], errors='coerce')
    stats.add_stat(table, "Invalid Timestamps", dates.isna().sum())
    df['created_at'] = dates
    df = df.dropna(subset=['created_at'])
    
    logging.info(f"Таблиця {table} очищена. Залишилось {len(df)} з {initial_len} рядків.")
    return df

def clean_order_items(df):
    table = "Order_Items"
    initial_len = len(df)
    
    bad_ids = df[df['order_item_id'].duplicated() | df['order_item_id'].isna()]
    stats.add_stat(table, "Duplicate/Null IDs", len(bad_ids))
    df = df.drop_duplicates(subset=['order_item_id']).dropna(subset=['order_item_id'])
    
    bad_qty = df[df['quantity'] <= 0]
    stats.add_stat(table, "Negative/Zero Quantity", len(bad_qty))
    df = df[df['quantity'] > 0]
    
    missing_refs = df[df['order_id'].isna() | df['product_id'].isna()]
    stats.add_stat(table, "Missing Foreign Keys (IDs)", len(missing_refs))
    df = df.dropna(subset=['order_id', 'product_id'])
    
    logging.info(f"Таблиця {table} очищена. Залишилось {len(df)} з {initial_len} рядків.")
    return df


def run_pipeline():
    try:
        logging.info("--- ЗАПУСК ПАЙПЛАЙНУ ---")
        logging.info(f"Шукаю файли у папці: {data_dir}")

        customers = pd.read_csv(os.path.join(data_dir, 'customers.csv'))
        products = pd.read_csv(os.path.join(data_dir, 'products.csv'))
        orders = pd.read_csv(os.path.join(data_dir, 'orders.csv'))
        items = pd.read_csv(os.path.join(data_dir, 'order_items.csv'))

        c_clean = clean_customers(customers)
        p_clean = clean_products(products)
        o_clean = clean_orders(orders)
        i_clean = clean_order_items(items)

        logging.info("Перевірка цілісності зв'язків...")
        
        orphan_orders = o_clean[~o_clean['customer_id'].isin(c_clean['customer_id'])]
        stats.add_stat("Orders", "Orphan (No Valid Customer)", len(orphan_orders))
        o_clean = o_clean[o_clean['customer_id'].isin(c_clean['customer_id'])]
        
        orphan_items = i_clean[~i_clean['order_id'].isin(o_clean['order_id']) | ~i_clean['product_id'].isin(p_clean['product_id'])]
        stats.add_stat("Order_Items", "Orphan (No Valid Order/Product)", len(orphan_items))
        i_clean = i_clean[i_clean['order_id'].isin(o_clean['order_id'])]
        i_clean = i_clean[i_clean['product_id'].isin(p_clean['product_id'])]

        c_clean.to_csv(os.path.join(output_dir, 'customers_cleaned.csv'), index=False)
        p_clean.to_csv(os.path.join(output_dir, 'products_cleaned.csv'), index=False)
        o_clean.to_csv(os.path.join(output_dir, 'orders_cleaned.csv'), index=False)
        i_clean.to_csv(os.path.join(output_dir, 'order_items_cleaned.csv'), index=False)

        print("\n" + "="*50)
        print("          ЗВІТ ПРО ЯКІСТЬ ДАНИХ (STATS)")
        print("="*50)
        report_df = pd.DataFrame(stats.report)
        if not report_df.empty:
            print(report_df.to_string(index=False))
        else:
            print("Дані були ідеально чистими!")
        print("="*50 + "\n")

        logging.info(f"Очищені файли збережено в: {output_dir}")
        logging.info("--- ПАЙПЛАЙН УСПІШНО ЗАВЕРШЕНО ---")
        return c_clean, p_clean, o_clean, i_clean

    except Exception as e:
        logging.error(f"Помилка під час виконання пайплайну: {e}")
        return None