import mysql.connector
import os
from dotenv import load_dotenv

load_dotenv()

mysql_config = {
    'user': os.getenv("USER"),
    'password': os.getenv("PASSWORD"),
    'host': os.getenv("HOST"),
    'database': os.getenv("DATABASE")
}

def connect_to_mysql():
    '''
    Function: To connect with MySQL database
    Parameters: None
    Return: Connection object
    '''
    try:
        connection = mysql.connector.connect(**mysql_config)
        if connection:
            print('Connected to MySQL database')
            return connection
    except mysql.connector.Error as e:
        print(f"Error: {e}")

def show_tables(cursor):
    '''
    Function: Show tables in the database
    Parameters: cursor: connection cursor
    Returns: None
    '''
    cursor.execute("SHOW TABLES")
    tables = cursor.fetchall()
    for table in tables:
        print(table)

def show_registry(cursor):
    '''
    Function: Show data from the registry table
    Parameters: cursor: connection cursor
    Returns: None
    '''
    cursor.execute("SELECT * FROM REGISTRY")
    rows = cursor.fetchall()
    for row in rows:
        print(row)

def insert_record(cursor):
    '''
    Function: Insert a record into the registry table
    Parameters: cursor: connection cursor
    Returns: None
    '''
    full_name = input("Enter full name: ")
    city = input("Enter city: ")
    state = input("Enter state: ")
    phone_number = int(input("Enter phone number: "))
    pincode = int(input("Enter pincode: "))
    email = input("Enter email: ")
    cursor.execute("INSERT INTO registry (full_name, city, state, phone_number, pincode, email) VALUES (%s, %s, %s, %s, %s, %s)", (full_name, city, state, phone_number, pincode, email))

def delete_name(cursor):
    '''
    Function: Delete the record with full_name  from the registry table
    Parameters: cursor: connection cursor
    Returns: None
    '''
    name=input("Enter the full_name to delete data")
    cursor.execute("DELETE FROM registry WHERE full_name = %s", (name,))

def main():
    '''
    Function: Main function to handle user commands
    Parameters: None
    Returns: None
    '''
    connection = connect_to_mysql()
    if connection:
        cursor = connection.cursor()
        with connection:
            while True:
                command = int(input("Enter command (1: show_tables, 2: show_registry, 3: insert_record, 4: delete_full_name 5:Exit ): "))
                match command:
                    case 1:
                        show_tables(cursor)
                    case 2:
                        show_registry(cursor)
                    case 3:
                        insert_record(cursor)
                        connection.commit()
                    case 4:
                        delete_name(cursor)
                        connection.commit()
                    case 5:
                        cursor.close()
                        break


if __name__ == "__main__":
    main()
