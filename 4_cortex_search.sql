-- Configuration
SET SF_DATABASE = 'DOCS';
SET SF_SCHEMA = 'PUBLIC';
SET SF_STAGE = '@DOCS.PUBLIC.STG';

-- Set context
USE DATABASE IDENTIFIER($SF_DATABASE);
USE SCHEMA IDENTIFIER($SF_SCHEMA);

-- =============================================================================
-- STEP 1: Create chunked documents table
-- =============================================================================

-- Create table with document chunks for better semantic search
-- Uses SPLIT_TEXT_MARKDOWN_HEADER to split content into chunks with headers
CREATE OR REPLACE TABLE doc_chunks AS
SELECT
    pd.filepath,
    pd.filename,
    pd.page_index,
    pd.page_count,
    pd.title,
    pd.print_date,
    pd.language,
    pd.summary,
    BUILD_SCOPED_FILE_URL($SF_STAGE, pd.filepath) AS file_url,
    (
        pd.filepath || ' - Page ' || pd.page_index || ':\n'
        || COALESCE('Title: ' || pd.title || '\n', '')
        || COALESCE('Header 1: ' || c.value['headers']['header_1'] || '\n', '')
        || COALESCE('Header 2: ' || c.value['headers']['header_2'] || '\n', '')
        || c.value['chunk']
    ) AS chunk,
    c.value['headers']['header_1']::VARCHAR AS header_1,
    c.value['headers']['header_2']::VARCHAR AS header_2,
    c.index AS chunk_index
FROM parsed_document pd,
LATERAL FLATTEN(
    SNOWFLAKE.CORTEX.SPLIT_TEXT_MARKDOWN_HEADER(
        pd.page_content,
        OBJECT_CONSTRUCT('#', 'header_1', '##', 'header_2'),
        2000,  -- chunks of 2000 characters
        300    -- 300 character overlap
    )
) c;

-- Verify chunked data
SELECT 
    COUNT(*) AS total_chunks,
    COUNT(DISTINCT filepath) AS total_documents,
    AVG(LENGTH(chunk)) AS avg_chunk_length
FROM doc_chunks;

-- Sample chunks
SELECT 
    filename,
    page_index,
    chunk_index,
    header_1,
    header_2,
    LEFT(chunk, 200) AS chunk_preview
FROM doc_chunks
LIMIT 10;

-- =============================================================================
-- STEP 2: Create Cortex Search Service on chunked documents
-- =============================================================================

-- Create Cortex Search service for semantic search over document chunks
CREATE OR REPLACE CORTEX SEARCH SERVICE document_search
ON chunk
ATTRIBUTES title, filename, filepath, language, print_date, summary, header_1, header_2, page_index
WAREHOUSE = COMPUTE_WH
TARGET_LAG = '1 hour'
AS (
    SELECT 
        filepath,
        filename,
        file_url,
        page_index,
        page_count,
        chunk_index,
        chunk,
        header_1,
        header_2,
        title,
        print_date,
        language,
        summary
    FROM doc_chunks
);

-- =============================================================================
-- Example search queries using CORTEX SEARCH
-- =============================================================================

-- Basic semantic search
SELECT PARSE_JSON(
    SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
        'document_search',
        '{
            "query": "clinical trial protocol",
            "columns": ["filepath", "filename", "page_index", "title", "chunk"],
            "limit": 10
        }'
    )
):results AS search_results;

-- Search with filter by language
SELECT PARSE_JSON(
    SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
        'document_search',
        '{
            "query": "manufacturing process",
            "columns": ["filepath", "filename", "title", "chunk"],
            "filter": {"@eq": {"language": "English"}},
            "limit": 10
        }'
    )
):results AS search_results;

-- Search with filter by filepath pattern
SELECT PARSE_JSON(
    SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
        'document_search',
        '{
            "query": "adverse events safety",
            "columns": ["filepath", "filename", "title", "chunk"],
            "filter": {"@contains": {"filepath": "Clinical"}},
            "limit": 10
        }'
    )
):results AS search_results;

-- =============================================================================
-- Check Cortex Search service status
-- =============================================================================

SHOW CORTEX SEARCH SERVICES;

-- Describe the search service
DESCRIBE CORTEX SEARCH SERVICE document_search;
