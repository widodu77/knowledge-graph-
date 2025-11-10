import psycopg2
from neo4j import GraphDatabase
import os
import time
import pandas as pd
from pathlib import Path

# --- Connect to Postgres ---
def get_postgres_connection():
    conn = psycopg2.connect(
        host="postgres",
        database="shop",
        user="app",
        password="app"
    )
    return conn

# --- Connect to Neo4j ---
def get_neo4j_driver():
    uri = os.getenv("NEO4J_URI", "bolt://neo4j:7687")
    user = os.getenv("NEO4J_USER", "neo4j")
    password = os.getenv("NEO4J_PASSWORD", "password")
    return GraphDatabase.driver(uri, auth=(user, password))

# --- Helper: Wait for Postgres ---
def wait_for_postgres(max_retries=30, delay=2):
    """Wait for PostgreSQL to be ready"""
    print("⏳ Waiting for Postgres...")
    for i in range(max_retries):
        try:
            conn = get_postgres_connection()
            conn.close()
            print("Postgres is ready")
            return True
        except psycopg2.OperationalError:
            print(f"   Attempt {i+1}/{max_retries} - Postgres not ready yet...")
            time.sleep(delay)
    raise Exception("Postgres did not become ready in time")

# --- Helper: Wait for Neo4j ---
def wait_for_neo4j(max_retries=30, delay=2):
    """Wait for Neo4j to be ready"""
    print("Waiting for Neo4j...")
    for i in range(max_retries):
        try:
            driver = get_neo4j_driver()
            with driver.session() as session:
                session.run("RETURN 1")
            driver.close()
            print("Neo4j is ready")
            return True
        except Exception as e:
            print(f"   Attempt {i+1}/{max_retries} - Neo4j not ready yet...")
            time.sleep(delay)
    raise Exception("Neo4j did not become ready in time")

# --- Helper: Run single Cypher query ---
def run_cypher(session, query, params=None):
    """Execute a single Cypher query"""
    try:
        result = session.run(query, params or {})
        return result
    except Exception as e:
        print(f"Error running query: {e}")
        raise

# --- Helper: Run Cypher file ---
def run_cypher_file(session, filepath):
    """Execute multiple Cypher statements from a file"""
    print(f"Running Cypher file: {filepath}")
    with open(filepath, 'r') as f:
        content = f.read()
    
    # Split by semicolon and filter empty statements
    statements = [s.strip() for s in content.split(';') if s.strip()]
    
    for statement in statements:
        try:
            session.run(statement)
            print(f"   ✓ Executed statement")
        except Exception as e:
            print(f"    Warning: {e}")

# --- Helper: Chunk dataframes for batch processing ---
def chunk(df, size=1000):
    """Split DataFrame into chunks for batch processing"""
    for start in range(0, len(df), size):
        yield df.iloc[start:start + size]

# --- Main ETL Function ---
def etl():
    """
    Main ETL function that migrates data from PostgreSQL to Neo4j.
    This function performs the complete Extract, Transform, Load process:
    1. Waits for both databases to be ready
    2. Sets up Neo4j schema using queries.cypher file
    3. Extracts data from PostgreSQL tables
    4. Transforms relational data into graph format
    5. Loads data into Neo4j with appropriate relationships
    
    The process creates the following graph structure:
    - Category nodes with name properties
    - Product nodes linked to categories via IN_CATEGORY relationships
    - Customer nodes with name and join_date properties
    - Order nodes linked to customers via PLACED relationships
    - Order-Product relationships via CONTAINS with quantity properties
    - Dynamic event relationships between customers and products
    """
    print("Starting ETL process...")
    
    # Ensure dependencies are ready (useful when running in docker-compose)
    wait_for_postgres()
    wait_for_neo4j()
    
    # Get path to your Cypher schema file
    queries_path = Path(__file__).with_name("queries.cypher")
    
    # Connect to databases
    conn = get_postgres_connection()
    driver = get_neo4j_driver()
    
    try:
        with driver.session() as session:
            # Step 1: Clear existing data (optional - for development)
            print("\n🧹 Clearing existing Neo4j data...")
            session.run("MATCH (n) DETACH DELETE n")
            
            # Step 2: Set up Neo4j schema (constraints & indexes)
            if queries_path.exists():
                print("\n📋 Setting up Neo4j schema...")
                run_cypher_file(session, queries_path)
            else:
                print(f"Warning: {queries_path} not found, skipping schema setup")
            
            # Step 3: Extract and Load Categories
            print("\nLoading Categories...")
            df_categories = pd.read_sql("SELECT id, name FROM categories", conn)
            for _, row in df_categories.iterrows():
                session.run("""
                    MERGE (c:Category {id: $id})
                    SET c.name = $name
                """, id=row['id'], name=row['name'])
            print(f"   ✓ Loaded {len(df_categories)} categories")
            
            # Step 4: Extract and Load Products
            print("\nLoading Products...")
            df_products = pd.read_sql("""
                SELECT id, name, price, category_id 
                FROM products
            """, conn)
            for _, row in df_products.iterrows():
                session.run("""
                    MERGE (p:Product {id: $id})
                    SET p.name = $name, p.price = $price
                    WITH p
                    MATCH (c:Category {id: $category_id})
                    MERGE (p)-[:IN_CATEGORY]->(c)
                """, id=row['id'], name=row['name'], 
                     price=float(row['price']), category_id=row['category_id'])
            print(f"   ✓ Loaded {len(df_products)} products")
            
            # Step 5: Extract and Load Customers
            print("\nLoading Customers...")
            df_customers = pd.read_sql("""
                SELECT id, name, join_date 
                FROM customers
            """, conn)
            for _, row in df_customers.iterrows():
                session.run("""
                    MERGE (c:Customer {id: $id})
                    SET c.name = $name, c.join_date = date($join_date)
                """, id=row['id'], name=row['name'], 
                     join_date=row['join_date'].isoformat())
            print(f"   ✓ Loaded {len(df_customers)} customers")
            
            # Step 6: Extract and Load Orders
            print("\nLoading Orders...")
            df_orders = pd.read_sql("""
                SELECT id, customer_id, ts 
                FROM orders
            """, conn)
            for _, row in df_orders.iterrows():
                session.run("""
                    MERGE (o:Order {id: $id})
                    SET o.timestamp = datetime($ts)
                    WITH o
                    MATCH (c:Customer {id: $customer_id})
                    MERGE (c)-[:PLACED]->(o)
                """, id=row['id'], customer_id=row['customer_id'],
                     ts=row['ts'].isoformat())
            print(f"   ✓ Loaded {len(df_orders)} orders")
            
            # Step 7: Extract and Load Order Items
            print("\nLoading Order Items...")
            df_order_items = pd.read_sql("""
                SELECT order_id, product_id, quantity 
                FROM order_items
            """, conn)
            for _, row in df_order_items.iterrows():
                session.run("""
                    MATCH (o:Order {id: $order_id})
                    MATCH (p:Product {id: $product_id})
                    MERGE (o)-[:CONTAINS {quantity: $quantity}]->(p)
                """, order_id=row['order_id'], product_id=row['product_id'],
                     quantity=int(row['quantity']))
            print(f"   ✓ Loaded {len(df_order_items)} order items")
            
            # Step 8: Extract and Load Events (Behavioral Tracking)
            print("\nLoading Events (behavioral tracking)...")
            df_events = pd.read_sql("""
                SELECT id, customer_id, product_id, event_type, ts 
                FROM events
            """, conn)
            
            event_type_map = {
                'view': 'VIEWED',
                'click': 'CLICKED',
                'add_to_cart': 'ADDED_TO_CART'
            }
            
            for _, row in df_events.iterrows():
                rel_type = event_type_map.get(row['event_type'], 'INTERACTED')
                session.run(f"""
                    MATCH (c:Customer {{id: $customer_id}})
                    MATCH (p:Product {{id: $product_id}})
                    CREATE (c)-[r:{rel_type} {{
                        event_id: $event_id,
                        timestamp: datetime($ts)
                    }}]->(p)
                """, customer_id=row['customer_id'], product_id=row['product_id'],
                     event_id=row['id'], ts=row['ts'].isoformat())
            print(f"   ✓ Loaded {len(df_events)} events")
            
            # Step 9: Verify data load
            print("\nVerifying data load...")
            counts = {
                'Categories': session.run("MATCH (c:Category) RETURN count(c) as cnt").single()['cnt'],
                'Products': session.run("MATCH (p:Product) RETURN count(p) as cnt").single()['cnt'],
                'Customers': session.run("MATCH (c:Customer) RETURN count(c) as cnt").single()['cnt'],
                'Orders': session.run("MATCH (o:Order) RETURN count(o) as cnt").single()['cnt'],
                'Relationships': session.run("MATCH ()-[r]->() RETURN count(r) as cnt").single()['cnt']
            }
            
            for entity, count in counts.items():
                print(f"   {entity}: {count}")
    
    finally:
        conn.close()
        driver.close()
    
    print("\nETL done.")

# --- Run ETL if executed directly ---
if __name__ == "__main__":
    etl()
