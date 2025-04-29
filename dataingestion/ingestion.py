import psycopg2
import pandas as pd
from sqlalchemy import create_engine

class PostgresDB:
    def __init__(self, db_name, user, password, host="localhost", port="5432"):
        self.db_name = db_name
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        self.connection = None
        self.cursor = None
        self.engine = None
        
    def connect(self):
        """Establish connection to PostgreSQL database."""
        try:
            self.connection = psycopg2.connect(
                dbname=self.db_name,
                user=self.user,
                password=self.password,
                host=self.host,
                port=self.port
            )
            self.cursor = self.connection.cursor()
            print("Connection established!")
        except Exception as e:
            print(f"Error while connecting to PostgreSQL: {e}")
            
    def create_tables(self):
        """Create tables in PostgreSQL database."""
        try:
            # Create pizza_types table
            self.cursor.execute("""
                CREATE TABLE IF NOT EXISTS pizza_types (
                    pizza_type_id VARCHAR PRIMARY KEY,
                    name VARCHAR,
                    category VARCHAR,
                    ingredients TEXT
                )
            """)
            
            # Create pizzas table
            self.cursor.execute("""
                CREATE TABLE IF NOT EXISTS pizzas (
                    pizza_id VARCHAR PRIMARY KEY,
                    pizza_type_id VARCHAR,
                    pizza_size VARCHAR,
                    price DECIMAL,
                    FOREIGN KEY (pizza_type_id) REFERENCES pizza_types (pizza_type_id)
                )
            """)

            # Create orders table
            self.cursor.execute("""
                CREATE TABLE IF NOT EXISTS orders_2023 (
                    order_id SERIAL PRIMARY KEY,
                    order_date DATE,
                    order_time TIME
                )
            """)

            # Create order_details table
            self.cursor.execute("""
                CREATE TABLE IF NOT EXISTS order_details_2023 (
                    order_details_id SERIAL PRIMARY KEY,
                    order_id INT,
                    pizza_id VARCHAR,
                    quantity INT,
                    FOREIGN KEY (order_id) REFERENCES orders (order_id),
                    FOREIGN KEY (pizza_id) REFERENCES pizzas (pizza_id)
                )
            """)

            # Create orders table
            self.cursor.execute("""
                CREATE TABLE IF NOT EXISTS orders_2024 (
                    order_id SERIAL PRIMARY KEY,
                    order_date DATE,
                    order_time TIME
                )
            """)

            # Create order_details table
            self.cursor.execute("""
                CREATE TABLE IF NOT EXISTS order_details_2024 (
                    order_details_id SERIAL PRIMARY KEY,
                    order_id INT,
                    pizza_id VARCHAR,
                    quantity INT,
                    FOREIGN KEY (order_id) REFERENCES orders (order_id),
                    FOREIGN KEY (pizza_id) REFERENCES pizzas (pizza_id)
                )
            """)
            
            # Create orders table
            self.cursor.execute("""
                CREATE TABLE IF NOT EXISTS orders_2025 (
                    order_id SERIAL PRIMARY KEY,
                    order_date DATE,
                    order_time TIME
                )
            """)

            # Create order_details table
            self.cursor.execute("""
                CREATE TABLE IF NOT EXISTS order_details_2025 (
                    order_details_id SERIAL PRIMARY KEY,
                    order_id INT,
                    pizza_id VARCHAR,
                    quantity INT,
                    FOREIGN KEY (order_id) REFERENCES orders (order_id),
                    FOREIGN KEY (pizza_id) REFERENCES pizzas (pizza_id)
                )
            """)

            self.connection.commit()
            print("Tables created successfully!")
        except Exception as e:
            print(f"Error while creating tables: {e}")
           ## self.connection.rollback()
    
    def load_csv_to_db(self, csv_file, table_name):
        """Load CSV data into PostgreSQL table."""
        try:
            # Load CSV using pandas
            data = pd.read_csv(csv_file)
            
            # Use SQLAlchemy engine to connect to PostgreSQL for easy DataFrame insertion
            self.engine = create_engine(f'postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.db_name}')
            data.to_sql(table_name, self.engine, if_exists='replace', index=False) 
            print(f"Data from {csv_file} has been successfully loaded into {table_name}!")
        except Exception as e:
            print(f"Error while loading CSV into {table_name}: {e}")
    
    def close_connection(self):
        """Close the database connection."""
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()
        print("Connection closed!")

# Example usage
if __name__ == "__main__":
    # Replace with your PostgreSQL credentials
    db = PostgresDB(db_name="Pizza", user="postgres", password="Mary 2525.")
    
    # Step 1: Connect to the database
    db.connect()
    
    # Step 2: Create necessary tables
    db.create_tables()


    # Step 3: Load CSV files into the database
    db.load_csv_to_db(r"C:\Users\AYOBAMI\Desktop\AnalyticalEngineering\data/pizza_type.csv", "pizza_types")
    db.load_csv_to_db(r"C:\Users\AYOBAMI\Desktop\AnalyticalEngineering\data/pizzas.csv", "pizzas")
    db.load_csv_to_db(r"C:\Users\AYOBAMI\Desktop\AnalyticalEngineering\data/orders_2023.csv", "orders_2023")
    db.load_csv_to_db(r"C:\Users\AYOBAMI\Desktop\AnalyticalEngineering\data/order_details_2023.csv", "order_details_2023")
    db.load_csv_to_db(r"C:\Users\AYOBAMI\Desktop\AnalyticalEngineering\data/orders_2024.csv", "orders_2024")
    db.load_csv_to_db(r"C:\Users\AYOBAMI\Desktop\AnalyticalEngineering\data/order_details_2024.csv", "order_details_2024")
    db.load_csv_to_db(r"C:\Users\AYOBAMI\Desktop\AnalyticalEngineering\data/orders_2025.csv", "orders_2025")
    db.load_csv_to_db(r"C:\Users\AYOBAMI\Desktop\AnalyticalEngineering\data/order_details_2025.csv", "order_details_2025")
    
    # Step 4: Close the connection
    db.close_connection()