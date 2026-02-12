#!/bin/bash
# =====================================================
# Superset Initialization Script
# =====================================================
# This script initializes Apache Superset with:
# 1. Database connection to MariaDB (data_quality)
# 2. Data Quality KPI datasets
# 3. Pre-built dashboard charts
#
# Usage: docker exec -it dq_superset bash /app/superset_init.sh
# =====================================================

set -e

echo "============================================"
echo "  SUPERSET DQ DASHBOARD INITIALIZATION"
echo "============================================"

# Step 1: Add MariaDB as a database connection
echo "[1/3] Adding MariaDB database connection..."
superset set-database-uri \
  --database-name "MariaDB - Data Quality" \
  --uri "mysql+pymysql://dq_user:dq_password@mariadb:3306/data_quality" \
  2>/dev/null || echo "  Database connection may already exist."

# Step 2: Import dashboard configuration (if exists)
echo "[2/3] Importing dashboard configuration..."
if [ -f /app/superset_dashboard_export.zip ]; then
    superset import-dashboards --path /app/superset_dashboard_export.zip
    echo "  Dashboard imported successfully."
else
    echo "  No dashboard export found. Manual configuration needed."
    echo "  Connect to http://localhost:8088 (admin/admin) to create dashboards."
fi

# Step 3: Summary
echo "[3/3] Initialization complete."
echo ""
echo "============================================"
echo "  SUPERSET READY"
echo "============================================"
echo "  URL:      http://localhost:8088"
echo "  Login:    admin"
echo "  Password: admin"
echo ""
echo "  MariaDB connection configured:"
echo "    Host: mariadb:3306"
echo "    Database: data_quality"
echo "    Tables: retail_raw, retail_cleaned"
echo "============================================"
