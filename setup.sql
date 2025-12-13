-- Configuration
SET SF_DATABASE = 'DOCS';
SET SF_SCHEMA = 'PUBLIC';
SET SF_STAGE = 'STG';

-- Enable Cortex cross-region support
ALTER ACCOUNT SET CORTEX_ENABLED_CROSS_REGION = 'ANY_REGION';

-- Create database and schema
CREATE DATABASE IF NOT EXISTS IDENTIFIER($SF_DATABASE);
USE DATABASE IDENTIFIER($SF_DATABASE);

CREATE SCHEMA IF NOT EXISTS IDENTIFIER($SF_SCHEMA);
USE SCHEMA IDENTIFIER($SF_SCHEMA);

-- Create stage with directory enabled (for querying file metadata)
CREATE STAGE IF NOT EXISTS IDENTIFIER($SF_STAGE)
    DIRECTORY = (ENABLE = TRUE)
    ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE')
    COMMENT = 'Stage for document files';

-- =============================================================================
-- UPLOAD FILES TO STAGE
-- =============================================================================
-- After creating the stage, use the upload script to load PDF files:
--
--   ./upload_to_snowflake.sh
--
-- The script will:
--   1. Scan the ./data directory for all PDF files
--   2. Upload files to the stage preserving directory structure
--   3. Refresh the stage metadata
--   4. Display the upload summary
--
-- Make sure Snow CLI is installed and configured before running.
-- =============================================================================

-- Refresh stage metadata after upload
ALTER STAGE IDENTIFIER($SF_STAGE) REFRESH;

-- Query uploaded files
SELECT COUNT(*) AS file_count FROM DIRECTORY(@STG);

-- List all files with paths
SELECT RELATIVE_PATH, SIZE, LAST_MODIFIED FROM DIRECTORY(@STG) ORDER BY RELATIVE_PATH;
