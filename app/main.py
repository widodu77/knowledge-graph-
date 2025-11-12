from fastapi import FastAPI, HTTPException
from etl import etl
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(
    title="E-Commerce Recommendation API",
    description="Graph-based recommendation system using Neo4j and PostgreSQL",
    version="1.0.0"
)

@app.get("/health")
def health():
    """Health check endpoint"""
    return {"ok": True, "status": "healthy"}

@app.post("/etl/run")
def trigger_etl():
    """
    Trigger the ETL process to load data from PostgreSQL into Neo4j.
    
    This endpoint:
    - Clears existing Neo4j data
    - Sets up schema (constraints & indexes)
    - Loads all tables from Postgres
    - Creates graph relationships
    - Processes behavioral events
    
    Returns:
        dict: Status message with ETL completion confirmation
    """
    try:
        logger.info("ETL triggered via API endpoint")
        etl()
        return {
            "ok": True,
            "message": "ETL completed successfully",
            "description": "Data has been loaded from PostgreSQL to Neo4j"
        }
    except Exception as e:
        logger.error(f"ETL failed: {str(e)}")
        raise HTTPException(status_code=500, detail=f"ETL failed: {str(e)}")

@app.get("/")
def root():
    """Root endpoint with API information"""
    return {
        "message": "E-Commerce Recommendation Engine API",
        "endpoints": {
            "/health": "Health check",
            "/etl/run": "Trigger ETL process (POST)",
            "/docs": "Interactive API documentation"
        }
    }


@app.get("/recs/collaborative/{customer_id}")
def get_collaborative_recommendations(customer_id: str):
    """
    Get product recommendations based on what similar customers bought
    """
    try:
        from neo4j import GraphDatabase
        import os
        
        # Connect to Neo4j
        uri = os.getenv("NEO4J_URI", "bolt://neo4j:7687")
        user = os.getenv("NEO4J_USER", "neo4j")
        password = os.getenv("NEO4J_PASSWORD", "password")
        
        driver = GraphDatabase.driver(uri, auth=(user, password))
        
        with driver.session() as session:
            # Find products bought by similar customers
            query = """
            MATCH (c:Customer {id: $customer_id})-[:PLACED]->(:Order)-[:CONTAINS]->(p:Product)
            WITH c, COLLECT(DISTINCT p.id) AS purchased_products
            
            MATCH (c)-[:PLACED]->(:Order)-[:CONTAINS]->(common:Product)<-[:CONTAINS]-(:Order)<-[:PLACED]-(other:Customer)
            WHERE c <> other
            WITH c, purchased_products, other, COUNT(DISTINCT common) AS similarity
            ORDER BY similarity DESC
            LIMIT 5
            
            MATCH (other)-[:PLACED]->(:Order)-[:CONTAINS]->(rec:Product)
            WHERE NOT rec.id IN purchased_products
            
            RETURN rec.id AS product_id, 
                   rec.name AS product_name, 
                   rec.price AS price,
                   COUNT(DISTINCT other) AS recommended_by,
                   AVG(similarity) AS avg_similarity
            ORDER BY recommended_by DESC, avg_similarity DESC
            LIMIT 10
            """
            
            result = session.run(query, customer_id=customer_id)
            recommendations = []
            
            for record in result:
                recommendations.append({
                    "product_id": record["product_id"],
                    "product_name": record["product_name"],
                    "price": float(record["price"]) if record["price"] else 0,
                    "recommended_by": record["recommended_by"],
                    "avg_similarity": float(record["avg_similarity"])
                })
            
            driver.close()
            
            return {
                "customer_id": customer_id,
                "recommendations": recommendations,
                "total": len(recommendations)
            }
            
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))