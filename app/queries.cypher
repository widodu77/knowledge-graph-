// Neo4j Schema Setup - Constraints and Indexes
// This file sets up the graph database schema with constraints and indexes
// for optimal query performance and data integrity

// ============================================
// CONSTRAINTS (Uniqueness)
// ============================================

// Ensure Customer IDs are unique
CREATE CONSTRAINT customer_id_unique IF NOT EXISTS
FOR (c:Customer) REQUIRE c.id IS UNIQUE;

// Ensure Product IDs are unique
CREATE CONSTRAINT product_id_unique IF NOT EXISTS
FOR (p:Product) REQUIRE p.id IS UNIQUE;

// Ensure Order IDs are unique
CREATE CONSTRAINT order_id_unique IF NOT EXISTS
FOR (o:Order) REQUIRE o.id IS UNIQUE;

// Ensure Category IDs are unique
CREATE CONSTRAINT category_id_unique IF NOT EXISTS
FOR (c:Category) REQUIRE c.id IS UNIQUE;

// ============================================
// INDEXES (Performance)
// ============================================

// Index on Customer name for faster lookups
CREATE INDEX customer_name_index IF NOT EXISTS
FOR (c:Customer) ON (c.name);

// Index on Product name for search
CREATE INDEX product_name_index IF NOT EXISTS
FOR (p:Product) ON (p.name);

// Index on Product price for range queries
CREATE INDEX product_price_index IF NOT EXISTS
FOR (p:Product) ON (p.price);

// Index on Category name
CREATE INDEX category_name_index IF NOT EXISTS
FOR (c:Category) ON (c.name);

// Index on Order timestamp for temporal queries
CREATE INDEX order_timestamp_index IF NOT EXISTS
FOR (o:Order) ON (o.timestamp);