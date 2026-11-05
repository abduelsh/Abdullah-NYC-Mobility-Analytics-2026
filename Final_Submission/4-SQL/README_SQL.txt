NYC MOBILITY ANALYTICS PLATFORM — SQL FILES

Database: qeema_nyc
Schema: public
Main fact table: fact_trips

CONTENTS
01_database_inventory.sql
  Lists public database tables and checks the fact table and ETL audit table.

02_monthly_trip_analysis.sql
  Queries the monthly trip summary and dashboard trend view.

03_pickup_location_analysis.sql
  Queries the existing top-10 pickup location materialized view and dashboard view.

04_performance_optimization.sql
  Contains EXPLAIN ANALYZE tests, the pickup-date index definition, ANALYZE,
  and an optional materialized-view refresh command.

05_dashboard_analysis_checks.sql
  Queries the existing dashboard analysis views.

HOW TO RUN
1. Open pgAdmin.
2. Connect to the qeema_nyc database.
3. Open Tools > Query Tool.
4. Open a .sql file and execute the statements.

IMPORTANT LIMITATION
These scripts document and query objects that were already created in the project.
They do not recreate the complete staging/fact/dimension table definitions or the
Python ETL process. For a fully reproducible database schema, export the actual
schema from your running qeema_nyc database using pgAdmin:
  Right-click qeema_nyc > Backup...
  Choose Format: Plain
  In Dump Options, select schema-only (do not include data)
  Save as qeema_nyc_schema.sql

Do not run CREATE INDEX again unnecessarily; IF NOT EXISTS is included.
Do not run REFRESH MATERIALIZED VIEW unless the source data has changed.
