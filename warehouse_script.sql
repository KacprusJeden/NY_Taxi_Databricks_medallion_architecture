-- ============================================
-- 1. Catalogs
-- ============================================

CREATE CATALOG IF NOT EXISTS bronze;
CREATE CATALOG IF NOT EXISTS silver;
CREATE CATALOG IF NOT EXISTS gold;


-- ============================================
-- 2. Schemas dbo
-- ============================================

CREATE SCHEMA IF NOT EXISTS bronze.dbo;
CREATE SCHEMA IF NOT EXISTS silver.dbo;
CREATE SCHEMA IF NOT EXISTS gold.dbo;


-- ============================================
-- 3. Volumes in bronze.dbo
-- ============================================

CREATE VOLUME IF NOT EXISTS bronze.dbo.ny_taxi;