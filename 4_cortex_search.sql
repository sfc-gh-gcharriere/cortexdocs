-- Configuration
SET SF_DATABASE = 'DOCS';
SET SF_SCHEMA = 'PUBLIC';
SET SF_STAGE = '@DOCS.PUBLIC.STG';

-- Set context
USE DATABASE IDENTIFIER($SF_DATABASE);
USE SCHEMA IDENTIFIER($SF_SCHEMA);

-- =============================================================================
-- STEP 1: Create helper table for signature text
-- =============================================================================

CREATE OR REPLACE TEMPORARY TABLE temp_signature_text AS
SELECT 
    filepath,
    filename,
    LISTAGG('Signature: ' || TRIM(SPLIT_PART(s.value::VARCHAR, '|', 1)) 
            || ' | Title: ' || TRIM(SPLIT_PART(s.value::VARCHAR, '|', 2))
            || ' | Date: ' || TRIM(SPLIT_PART(s.value::VARCHAR, '|', 3)), '\n') AS signature_text
FROM parsed_document pd,
LATERAL FLATTEN(input => pd.hand_signatures) s
WHERE pd.page_index = 0
  AND pd.hand_signatures IS NOT NULL
  AND ARRAY_SIZE(pd.hand_signatures) > 0
GROUP BY filepath, filename;

-- =============================================================================
-- STEP 2: Create chunked documents table with embedded signatures
-- =============================================================================

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
    pd.hand_signatures,
    BUILD_SCOPED_FILE_URL($SF_STAGE, pd.filepath) AS file_url,
    (
        pd.filepath || ' - Page ' || pd.page_index || ':\n'
        || COALESCE('Title: ' || pd.title || '\n', '')
        || COALESCE('Header 1: ' || c.value['headers']['header_1'] || '\n', '')
        || COALESCE('Header 2: ' || c.value['headers']['header_2'] || '\n', '')
        || c.value['chunk']
        || CASE 
            WHEN pd.page_index = 0 AND sig.signature_text IS NOT NULL AND sig.signature_text != '' 
            THEN '\n\n--- Handwritten Signatures ---\n' || sig.signature_text 
            ELSE '' 
           END
    ) AS chunk,
    c.value['headers']['header_1']::VARCHAR AS header_1,
    c.value['headers']['header_2']::VARCHAR AS header_2,
    c.index AS chunk_index
FROM parsed_document pd
LEFT JOIN temp_signature_text sig 
    ON pd.filepath = sig.filepath AND pd.filename = sig.filename,
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
    SUM(CASE WHEN chunk LIKE '%Handwritten Signatures%' THEN 1 ELSE 0 END) AS chunks_with_signatures,
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
-- STEP 3: Create Cortex Search Service on chunked documents
-- =============================================================================

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
