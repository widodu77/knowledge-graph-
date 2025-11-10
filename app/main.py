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