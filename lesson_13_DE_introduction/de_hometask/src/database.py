import os
import logging
from sqlalchemy import create_engine, text

def save_to_db(customers, products, orders, items):
    """Збереження даних та створення розширеної аналітики"""
    try:
        current_script_path = os.path.abspath(__file__)
        src_dir = os.path.dirname(current_script_path)
        base_dir = os.path.dirname(src_dir)
        output_dir = os.path.join(base_dir, 'output')

        os.makedirs(output_dir, exist_ok=True)
        db_path = os.path.join(output_dir, 'ecommerce.db')
        engine = create_engine('postgresql://user:password@db:5432/ecommerce')
        
        logging.info(f"Підключення до БД: {db_path}")

        customers.to_sql('customers', engine, if_exists='replace', index=False)
        products.to_sql('products', engine, if_exists='replace', index=False)
        orders.to_sql('orders', engine, if_exists='replace', index=False)
        items.to_sql('order_items', engine, if_exists='replace', index=False)

        with engine.begin() as conn:
            conn.execute(text("DROP TABLE IF EXISTS analytics_category_revenue"))
            conn.execute(text("""
                CREATE TABLE analytics_category_revenue AS
                SELECT p.category, SUM(i.quantity * p.price) as total_revenue, COUNT(DISTINCT i.order_id) as total_orders
                FROM order_items i JOIN products p ON i.product_id = p.product_id
                GROUP BY 1 ORDER BY 2 DESC;
            """))

            conn.execute(text("DROP TABLE IF EXISTS analytics_customer_value"))
            conn.execute(text("""
                CREATE TABLE analytics_customer_value AS
                SELECT c.customer_id, c.email, COUNT(o.order_id) as orders_count, SUM(i.quantity * p.price) as total_spent
                FROM customers c
                JOIN orders o ON c.customer_id = o.customer_id
                JOIN order_items i ON o.order_id = i.order_id
                JOIN products p ON i.product_id = p.product_id
                GROUP BY 1, 2 ORDER BY 4 DESC;
            """))

            conn.execute(text("DROP TABLE IF EXISTS analytics_sales_by_country"))
            conn.execute(text("""
                CREATE TABLE analytics_sales_by_country AS
                SELECT c.country, COUNT(o.order_id) as orders_count, SUM(i.quantity * p.price) as total_revenue
                FROM customers c
                JOIN orders o ON c.customer_id = o.customer_id
                JOIN order_items i ON o.order_id = i.order_id
                JOIN products p ON i.product_id = p.product_id
                GROUP BY 1 ORDER BY 3 DESC;
            """))

            conn.execute(text("DROP TABLE IF EXISTS analytics_top_products"))
            conn.execute(text("""
                CREATE TABLE analytics_top_products AS
                SELECT p.name, p.category, SUM(i.quantity) as units_sold
                FROM order_items i
                JOIN products p ON i.product_id = p.product_id
                GROUP BY 1, 2 ORDER BY 3 DESC;
            """))

            conn.execute(text("DROP TABLE IF EXISTS analytics_monthly_trends"))
            conn.execute(text("""
                CREATE TABLE analytics_monthly_trends AS
                SELECT TO_CHAR(created_at, 'YYYY-MM') as month, COUNT(order_id) as orders_count, order_status
                FROM orders
                GROUP BY 1, 3 ORDER BY 1 DESC;
            """))

            conn.execute(text("DROP TABLE IF EXISTS analytics_order_metrics"))
            conn.execute(text("""
                CREATE TABLE analytics_order_metrics AS
                SELECT o.order_id, COUNT(i.order_item_id) as items_in_order, SUM(i.quantity * p.price) as order_total
                FROM orders o
                JOIN order_items i ON o.order_id = i.order_id
                JOIN products p ON i.product_id = p.product_id
                GROUP BY 1;
            """))

        logging.info("Всі аналітичні вітрини успішно оновлені.")
        print(f" База даних готова! Створено 6 аналітичних таблиць у {db_path}")

    except Exception as e:
        logging.error(f"Помилка БД: {e}")