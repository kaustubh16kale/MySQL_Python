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
        if connection.is_connected():
            print('Connected to MySQL database')
            return connection
    except mysql.connector.Error as e:
        print(f"Error: {e}")
        return None

def show_database(cursor):
    '''
    Function: show_database: print all the databases
    Parameters: cursor
    Return: print all database
    '''
    cursor.execute("SHOW DATABASES")
    dbs = cursor.fetchall()
    for db in dbs:
        print(db[0])

    current_db = input("Enter database name: ")
    try:
        cursor.execute(f"USE `{current_db}`")
        print(f"Using {current_db}")
    except mysql.connector.Error as e:
        print(f"Error: {e}")

def print_data(cursor):
    '''
    Function: print_data: To print the data store in the cursor (called from all the functions)
    Parameters: cursor
    Return: None
    '''
    datas = cursor.fetchall()
    for data in datas:
        print(" ".join(map(str, data)))

def show_tables(cursor):
    '''
    Function: Show tables in the database
    Parameters: cursor: connection cursor
    Returns: None
    '''
    cursor.execute("SHOW TABLES")
    print_data(cursor)

def show_data(cursor, table=None):
    '''
    Function: show_data: To print the data from selected table
    Parameters: cursor:
                table: if passed then user input table name
                       else: None
    Returns: print_data()
    '''
    if table is None:
        table = input("Enter the name of the table to get data from: ")
    try:
        cursor.execute(f"SELECT * FROM `{table}`")
        print_data(cursor)
    except mysql.connector.Error as e:
        print(f"Error: {e}")

def show_columns(cursor, table):
    '''
    Function: show_columns: To show all the columns from the selected table
    Parameters: cursor:
                table: user input table name
    Return: print_data()
    '''
    try:
        cursor.execute(f"SHOW COLUMNS FROM `{table}`")
        print_data(cursor)
    except mysql.connector.Error as e:
        print(f"Error: {e}")

def left_join(cursor):
    '''
    Functions: left_join: To perform left join on the database
    Parameters: cursor:
    Return: print_data()  --> new data after performing left join
    '''
    show_tables(cursor)
    first_table = input("Enter first table: ")
    show_columns(cursor, first_table)
    
    second_table = input("Enter the second table: ")
    show_columns(cursor, second_table)

    column=input("Enter the column name to perform join on: ")
    
    try:
        cursor.execute(f"SELECT * FROM `{first_table}` AS F LEFT JOIN `{second_table}` AS S ON F.`{column}` = S.`{column}`")        
        print_data(cursor)
    except mysql.connector.Error as e:
        print(f"Error: {e}")

def right_join(cursor):
    '''
    Functions: right_join: To perform right join on the database
    Parameters: cursor:
    Return: print_data()  --> new data after performing right join
    '''
    show_tables(cursor)
    first_table=input("Enter first_table: ")
    show_columns(cursor,first_table)

    second_table=input("Enter the second_table: ")
    show_columns(cursor,second_table)

    column=input("Enter the column name to perform join on: ")
    try:
        cursor.execute(f"SELECT * FROM `{first_table}` AS F right JOIN `{second_table}` AS S ON F.`{column}` = S.`{column}`")        
        print_data(cursor)
    except mysql.connector.Error as e:
        print(f"Error: {e}")

def cross_join(cursor):
    '''
    Functions: cross_join: To perform cross join on the database
    Parameters: cursor:
    Return: print_data()  --> new data after performing cross join
    '''
    show_tables(cursor)
    first_table = input("Enter first table: ")
    show_columns(cursor, first_table)

    second_table = input("Enter the second table: ")
    show_columns(cursor, second_table)
    column=input("Enter the column name to perform join on: ")
    try:
        cursor.execute(f"SELECT * FROM `{first_table}` AS F CROSS JOIN `{second_table}` AS S ON F.`{column}` = S.`{column}`")
        print_data(cursor)
    except mysql.connector.Error as e:
        print(f"Error: {e}")

def full_outer_join(cursor):
    '''
    Functions: full_outer_join: To perform left join on the database
    Parameters: cursor:
    Return: print_data()  --> new data after performing full_outer_join
    '''
    try:
        show_tables(cursor)
        first_table = input("Enter first table: ")
        show_columns(cursor, first_table)

        second_table = input("Enter the second table: ")
        show_columns(cursor, second_table)

        column = input("Enter the column name to perform join on: ")

        # Perform left join
        cursor.execute(f"SELECT * FROM `{first_table}` AS F LEFT JOIN `{second_table}` AS S ON F.`{column}` = S.`{column}`")
        left_result = cursor.fetchall()

        # Perform right join
        cursor.execute(f"SELECT * FROM `{first_table}` AS F RIGHT JOIN `{second_table}` AS S ON F.`{column}` = S.`{column}`")
        right_result = cursor.fetchall()

        full_outer_result = left_result + right_result

        for data in full_outer_result:
            print(" ".join(map(str, data)))
    except mysql.connector.Error as e:
        print(f"Error: {e}")


def main():
    '''
    Function: main()
    Parameters: None
    Return: None
    '''
    connection = connect_to_mysql()
    if connection:
        cursor = connection.cursor()
        with connection:
            while True:
                command = int(input("1) To show database 2) To show tables 3) Data from table 4) To perform join 5) Quit"))
                match command:
                    case 1:
                        show_database(cursor)
                    case 2:
                        show_tables(cursor)
                    case 3:
                        show_data(cursor)
                    case 4:
                        join_command = int(input("1) To perform Left join 2) To perform Right join 3) To Perform Cross join 4) To perform full outer join  "))
                        match join_command:
                            case 1:
                                left_join(cursor)
                            case 2:
                                right_join(cursor)
                            case 3:
                                cross_join(cursor)
                            case 4:
                                full_outer_join(cursor)

                    case 5:
                        break
                    case _:
                        print("Invalid command")


if __name__ == "__main__":
    main()
