import sys
import os   

from src.cleaner import run_pipeline
from src.database import save_to_db

def main():
    print(" Початок роботи ETL-пайплайну...")
    
    result = run_pipeline()
    
    if result:
        customers_df, products_df, orders_df, items_df = result
        
        print(" Завантаження даних у базу та створення аналітики...")
        save_to_db(customers_df, products_df, orders_df, items_df)
        
        print("\n ПАЙПЛАЙН ПОВНІСТЮ ВИКОНАНО!")
        print(" Очищені CSV та база даних знаходяться у папці /output")
        print(" Логи виконання у папці /logs")
    else:
        print("\n Виникла помилка під час очищення. Процес зупинено.")

if __name__ == "__main__":
    main()