import pyodbc
import datetime
import time
import re
import tkinter as tk
from threading import Thread

# SQL Server Connection Configuration
server = 'DESKTOP-OGN2CU6\SQLEXPRESS'
database = 'hcp'
username = 'sa'
password = 'admin@123'
table = 'hcp1'


# Global variables
is_running = True
last_fetched_time = ''
file_path = 'C:/Users/ARVINDH/Desktop/Apps/toss'  # Set the output text file location here

# Create a new text file and rename it to the current date
def create_new_file(file_path):
    current_date = datetime.datetime.now().strftime('WIL%Y%m%d')
    file_name = f'{current_date}.txt'
    file_loc = file_path + '/' + file_name
    with open(file_loc, 'w') as file:
        pass  # Create an empty file

    return file_loc

# Pull data from SQL table and write it to the text file
def pull_data_and_write_to_file(file_path):
    global is_running
    global last_fetched_time

    conn_str = f"DRIVER={{SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}"
    connection = None
    cursor = None

    try:
        connection = pyodbc.connect(conn_str)
        cursor = connection.cursor()
        file_loc = create_new_file(file_path)
        # SQL query to fetch data from the table (selecting specific columns)
        query = f'SELECT devicename, accesstime, accessdate, empid, indexno FROM {table}'

        # Set to store unique values based on column 7
        unique_values = set()

        while is_running:
            try:
                cursor.execute(query)
                result = cursor.fetchall()

                # Write data to the text file
                with open(file_loc, 'a') as file:
                    for row in result:
                        line = ''.join(str(col) for col in row[:4])  # Exclude column 7
                        line = re.sub(r'[^a-zA-Z0-9]', '', line)  # Remove special characters
                        column7_value = row[4]  # Assuming column 7 is the fifth column (zero-based index)
                        if column7_value not in unique_values:
                            file.write(line + '\n')
                            unique_values.add(column7_value)

                last_fetched_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                prev_date = datetime.datetime.now().strftime('WIL%Y%m%d')
                time.sleep(5)  # Wait for 5 seconds before pulling data again
                current_date = datetime.datetime.now().strftime('WIL%Y%m%d')
                if prev_date != current_date:
                    file_loc = create_new_file(file_path)

            except Exception as e:
                print(f'Error occurred while fetching data: {e}')
                time.sleep(5)  # Wait for 5 seconds before retrying

    except pyodbc.Error as ex:
        print(f"Database connection error: {ex}")

    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()

# Stop the program
def stop_program():
    global is_running
    is_running = False

# GUI setup
def create_gui():
    window = tk.Tk()
    window.title("Data Pull Program")
    window.geometry("400x200")

    status_label = tk.Label(window, text="Program Status: Running", fg="green")
    status_label.pack(pady=10)

    last_fetched_label = tk.Label(window, text=f"Last Fetched Time: {last_fetched_time}")
    last_fetched_label.pack(pady=10)

    stop_button = tk.Button(window, text="Stop Program", command=stop_program)
    stop_button.pack(pady=10)

    def update_status():
        if is_running:
            status_label.config(text="Program Status: Running", fg="green")
        else:
            status_label.config(text="Program Status: Not Running", fg="red")

        last_fetched_label.config(text=f"Last Fetched Time: {last_fetched_time}")
        window.after(1000, update_status)

    window.after(1000, update_status)
    window.mainloop()

# Main program
if __name__ == '__main__':
    t = Thread(target=pull_data_and_write_to_file, args=(file_path,))
    t.start()

    create_gui()
