import mysql.connector
from mysql.connector import Error
from dotenv import load_dotenv
from faker import Faker
import os
import time

# Завантаження змінних середовища
load_dotenv()

# Налаштування підключення
HOST = os.getenv('host')
USER = os.getenv('user')
PASSWORD = os.getenv('password')
DATABASE = os.getenv('database')


def create_connection():
    try:
        connection = mysql.connector.connect(
            host=HOST,
            user=USER,
            password=PASSWORD,
            database=DATABASE
        )
        if connection.is_connected():
            return connection
    except Error as e:
        print(f"Error: {e}")
    return None


def insert_fake_data():
    fake = Faker()
    connection = create_connection()
    if connection is None:
        print("Connection failed!")
        return

    cursor = connection.cursor()

    try:
        # Вставка даних у таблицю customers
        for _ in range(10000):
            cursor.execute("INSERT INTO customers (name, email) VALUES (%s, %s)",
                           (fake.name(), fake.email()))

        connection.commit()

        # Вставка даних у таблицю orders
        for _ in range(10000):
            cursor.execute("INSERT INTO orders (customer_id, order_date, amount) VALUES (%s, %s, %s)",
                           (fake.random_int(min=1, max=10000), fake.date_this_decade(), fake.random_number(digits=5)))

        connection.commit()

        # Вставка даних у таблицю order_items
        for _ in range(10000):
            cursor.execute("INSERT INTO order_items (order_id, product_name, quantity, price) VALUES (%s, %s, %s, %s)",
                           (fake.random_int(min=1, max=10000), fake.word(), fake.random_int(min=1, max=10),
                            fake.random_number(digits=5)))

        connection.commit()

    except Error as e:
        print(f"Error: {e}")

    finally:
        if cursor:
            cursor.close()
        if connection and connection.is_connected():
            connection.close()


def create_indexes():
    connection = create_connection()
    if connection is None:
        print("Connection failed!")
        return

    cursor = connection.cursor()

    try:
        cursor.execute("CREATE INDEX idx_customer_id ON orders (customer_id)")
        cursor.execute("CREATE INDEX idx_order_id ON order_items (order_id)")
        connection.commit()
    except Error as e:
        print(f"Error: {e}")
    finally:
        if cursor:
            cursor.close()
        if connection and connection.is_connected():
            connection.close()


def measure_query_time(query):
    connection = create_connection()
    if connection is None:
        print("Connection failed!")
        return

    cursor = connection.cursor()
    start_time = time.time()

    try:
        cursor.execute(query)
        result = cursor.fetchall()
        connection.commit()
        return time.time() - start_time, result
    except Error as e:
        print(f"Error: {e}")
        return None, []
    finally:
        if cursor:
            cursor.close()
        if connection and connection.is_connected():
            connection.close()


if __name__ == "__main__":
    print("\nInserting fake data")
    #insert_fake_data()

    print("\nCreating indexes")
    create_indexes()

    query1 = '''
    SELECT cust.name, cust.email, ordr.order_date, ordr.amount, ordr_itm.product_name, ordr_itm.quantity, ordr_itm.price
    FROM customers cust
    JOIN orders ordr ON cust.id = ordr.customer_id
    JOIN order_items ordr_itm ON ordr.id = ordr_itm.order_id
    WHERE ordr.amount BETWEEN 1111 AND 1122
    '''

    query2 = '''
    WITH customerOrders AS (
        SELECT ordr.id, ordr.customer_id, ordr.order_date, ordr.amount
        FROM orders ordr
        WHERE ordr.amount BETWEEN 1111 AND 1122
    )
    SELECT cust.name, cust.email, cst_ordr.order_date, cst_ordr.amount, ordr_itm.product_name, ordr_itm.quantity, ordr_itm.price
    FROM customers cust
    JOIN customerOrders cst_ordr ON cust.id = cst_ordr.customer_id
    JOIN order_items ordr_itm ON cst_ordr.id = ordr_itm.order_id
    '''

    print("\nMeasuring query execution time")

    time1, _ = measure_query_time(query1)
    time2, _ = measure_query_time(query2)

    print(f"Час виконання першого запиту: {time1} секунд")
    print(f"Час виконання другого запиту: {time2} секунд")

print("done!")
