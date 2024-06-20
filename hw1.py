import mysql.connector
from mysql.connector import Error
from dotenv import load_dotenv
from datetime import datetime
import os

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

def read_uncommited_demo():
    """
    Показує, як працює рівень ізоляції READ UNCOMMITTED.
    Демонструє брудне читання.
    :return: void
    """
    connection1 = create_connection()
    connection2 = create_connection()
    print("\n read uncommitted")
    try:
        cursor1 = connection1.cursor()
        cursor2 = connection2.cursor()

        # Транзакція 1: Читання непідтверджених
        print(f"Transaction 1 started: {datetime.now()} ")
        connection1.start_transaction(isolation_level='READ UNCOMMITTED')
        cursor1.execute("UPDATE accounts SET balance = 9999 WHERE name = 'Alice'")

        # Транзакція 2: Читання непідтверджених
        print(f"Transaction 2 started: {datetime.now()} ")
        connection2.start_transaction(isolation_level='READ UNCOMMITTED')
        cursor2.execute("SELECT balance FROM accounts WHERE name = 'Alice'")
        balance_dirty_read = cursor2.fetchone()[0]

        print(f"      Dirty Read (READ UNCOMMITTED): Alice's balance = {balance_dirty_read}")

        print(f"Transaction 1 rollback(): {datetime.now()}")
        connection1.rollback()

        print(f"Transaction 2 commit(): {datetime.now()}")
        connection2.commit()

    except Error as e:
        print(f"Error: {e}")
    finally:
        if cursor1:
            cursor1.close()
        if connection1 and connection1.is_connected():
            connection1.close()
        if cursor2:
            cursor2.close()
        if connection2 and connection2.is_connected():
            connection2.close()

def read_committed_demo():
    """
    Показує, як працює рівень ізоляції READ COMMITTED.
    Демонструє відсутність брудного читання.
    :return: void
    """
    connection1 = create_connection()
    connection2 = create_connection()
    print("\n read committed")

    try:
        cursor1 = connection1.cursor()
        cursor2 = connection2.cursor()

        # Транзакція 1: READ COMMITTED
        print(f"Transaction 1 started: {datetime.now()}")
        connection1.start_transaction(isolation_level='READ COMMITTED')
        cursor1.execute("UPDATE accounts SET balance = 9999 WHERE name = 'Alice'")

        # Транзакція 2: READ COMMITTED
        print(f"Transaction 2 started: {datetime.now()}")
        connection2.start_transaction(isolation_level='READ COMMITTED')
        cursor2.execute("UPDATE accounts SET balance = 666 WHERE name = 'Alice'")
        cursor2.execute("SELECT balance FROM accounts WHERE name = 'Alice'")
        balance_read_committed = cursor2.fetchone()[0]

        print(f"       Read Committed: Alice's balance = {balance_read_committed}")

        print(f"Transaction 1 rollback(): {datetime.now()}")
        connection1.rollback()

        print(f"Transaction 2 commit(): {datetime.now()}")
        connection2.commit()

    except Error as e:
        print(f"Error: {e}")
    finally:
        if cursor1:
            cursor1.close()
        if connection1 and connection1.is_connected():
            connection1.close()
        if cursor2:
            cursor2.close()
        if connection2 and connection2.is_connected():
            connection2.close()

def read_repeatable_demo():
    """
    Показує, як працює рівень ізоляції REPEATABLE READ.
    Демонструє неповторювані читання.
    :return: void
    """
    connection1 = create_connection()
    connection2 = create_connection()
    print("\n read repeatable")

    try:
        cursor1 = connection1.cursor()
        cursor2 = connection2.cursor()

        # Транзакція 1: REPEATABLE READ
        print(f"Transaction 1 started: {datetime.now()}")
        connection1.start_transaction(isolation_level='REPEATABLE READ')
        cursor1.execute("UPDATE accounts SET balance = 9999 WHERE name = 'Alice'")

        # Транзакція 2: REPEATABLE READ
        print(f"Transaction 2 started: {datetime.now()}")
        connection2.start_transaction(isolation_level='REPEATABLE READ')
        cursor2.execute("UPDATE accounts SET balance = 666 WHERE name = 'Bob'")
        cursor2.execute("SELECT balance FROM accounts WHERE name = 'Alice'")
        balance_repeatable_read = cursor2.fetchone()[0]

        print(f"     Repeatable Read: Alice's balance = {balance_repeatable_read}")

        print(f"Transaction 1 rollback(): {datetime.now()}")
        connection1.rollback()

        print(f"Transaction 2 commit(): {datetime.now()}")
        connection2.commit()

    except Error as e:
        print(f"Error: {e}")
    finally:
        if cursor1:
            cursor1.close()
        if connection1 and connection1.is_connected():
            connection1.close()
        if cursor2:
            cursor2.close()
        if connection2 and connection2.is_connected():
            connection2.close()

def serializable_demo():
    """
    Показує, як працює рівень ізоляції SERIALIZABLE.
    Демонструє серіалізовані транзакції.
    """
    print("\n read serializable")
    connection1 = create_connection()
    connection2 = create_connection()

    try:
        cursor1 = connection1.cursor()
        cursor2 = connection2.cursor()

        # Транзакція 1: SERIALIZABLE
        print(f"Transaction 1 started: {datetime.now()}")
        connection1.start_transaction(isolation_level='SERIALIZABLE')
        cursor1.execute("SELECT balance FROM accounts WHERE name = 'Alice'")
        balance_serializable_1 = cursor1.fetchone()[0]
        print(f"     Initial Read (SERIALIZABLE): Alice's balance = {balance_serializable_1}")

        # Транзакція 2: SERIALIZABLE
        print(f"Transaction 2 started: {datetime.now()}")
        connection2.start_transaction(isolation_level='SERIALIZABLE')
        cursor2.execute("UPDATE accounts SET balance = 666 WHERE name = 'Bob'")
        cursor2.execute("UPDATE accounts SET balance = 9999 WHERE name = 'Alice'")

        try:
            connection2.commit()
        except Error as e:
            print(f"Error committing transaction 2: {e}")

        # Транзакція 1: Повторне читання
        cursor1.execute("SELECT balance FROM accounts WHERE name = 'Alice'")
        balance_serializable_2 = cursor1.fetchone()[0]

        print(f"     Serializable: Alice's balance after update = {balance_serializable_2}")

        connection1.commit()

    except Error as e:
        print(f"Error: {e}")
    finally:
        if cursor1:
            cursor1.close()
        if connection1 and connection1.is_connected():
            connection1.close()
        if cursor2:
            cursor2.close()
        if connection2 and connection2.is_connected():
            connection2.close()

if __name__ == "__main__":
    serializable_demo()
    read_repeatable_demo()
    read_committed_demo()
    read_uncommited_demo()
