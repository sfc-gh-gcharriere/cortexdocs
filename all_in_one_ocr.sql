-- =============================================================================
-- ALL-IN-ONE: Parse (OCR), Extract, Signatures, and Create Cortex Search
-- =============================================================================
-- This script processes all documents in a single workflow:
--   1. Parse documents using OCR mode
--   2. Extract metadata (title, print_date, language, summary)
--   3. Extract handwritten signatures (Name | Title | Date format)
--   4. Create document chunks with embedded signature info
--   5. Create Cortex Search service
--
-- MODE SELECTION GUIDE:
--   LAYOUT mode: Preferred for most use cases, especially complex documents.
--                Optimized for extracting text AND layout elements like tables.
--                Best for: knowledge bases, retrieval systems, AI applications.
--
--   OCR mode:    Recommended for quick, high-quality text extraction.
--                Best for: manuals, agreements, contracts, product detail pages,
--                insurance policies, claims, SharePoint documents.
-- =============================================================================

-- Configuration
SET SF_DATABASE = 'GLD';
SET SF_SCHEMA = 'PUBLIC';
SET SF_STAGE = '@GLD.PUBLIC.STG';
SET SF_WAREHOUSE = 'COMPUTE_WH';

-- Folder filter: set to folder path to process specific folder, or '%' for all documents
-- Examples: 'Clinical/%', 'PROJ-17483.pdf', '%' (all)
SET SF_FOLDER = '%';

-- Set context
USE DATABASE IDENTIFIER($SF_DATABASE);
USE SCHEMA IDENTIFIER($SF_SCHEMA);

-- =============================================================================
-- STEP 1: Parse Documents using OCR Mode
-- =============================================================================

-- Create table for parsed documents (if not exists)
CREATE TABLE IF NOT EXISTS parsed_document_ocr (
    filepath VARCHAR,           -- Full relative path (e.g., 'Clinical/Studies/subfolder/file.pdf')
    filename VARCHAR,           -- Just the filename (e.g., 'file.pdf')
    page_count INT,             -- Total pages in document
    page_index INT,             -- Current page index (0-based)
    page_content VARCHAR,       -- Page content text (extracted from parsed_doc:content)
    title VARCHAR,              -- Document title
    print_date VARCHAR,         -- Document date
    language VARCHAR,           -- Document language
    summary VARCHAR,            -- Document summary
    hand_signatures VARIANT,    -- Handwritten signatures (Name | Title | Date format)
    parsed_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Parse and insert documents using OCR mode
-- OCR mode: quick, high-quality text extraction (manuals, contracts, policies, etc.)
-- Change to 'LAYOUT' for complex documents with tables or for knowledge base/RAG use cases
INSERT INTO parsed_document_ocr (filepath, filename, page_count, page_index, page_content)
WITH files AS (
    SELECT RELATIVE_PATH AS filepath
    FROM DIRECTORY(@STG) 
    WHERE RELATIVE_PATH LIKE $SF_FOLDER
      AND RELATIVE_PATH NOT IN (SELECT DISTINCT filepath FROM parsed_document_ocr)  -- Skip already parsed
),
parsed AS (
    SELECT 
        f.filepath,
        SPLIT_PART(f.filepath, '/', -1) AS filename,
        AI_PARSE_DOCUMENT(
            TO_FILE($SF_STAGE, f.filepath),
            { 'mode': 'OCR', 'page_split': true }  -- Using OCR mode instead of LAYOUT
        ) AS parsed_doc
    FROM files f
)
SELECT 
    p.filepath,
    p.filename,
    p.parsed_doc:metadata:pageCount::INT AS page_count,
    pg.value:index::INT AS page_index,
    pg.value:content::VARCHAR AS page_content
FROM parsed p,
LATERAL FLATTEN(input => p.parsed_doc:pages) pg;

-- Verify parsed documents
SELECT 
    filename, 
    filepath,
    page_count, 
    COUNT(*) AS pages_inserted 
FROM parsed_document_ocr 
WHERE filepath LIKE $SF_FOLDER
GROUP BY filename, filepath, page_count 
ORDER BY filepath;

-- =============================================================================
-- STEP 2: Extract Metadata (title, print_date, language)
-- =============================================================================

-- Step 2a: Process documents with <= 125 pages using TO_FILE (better extraction)
UPDATE parsed_document_ocr p
SET 
    title = extracted.title,
    print_date = extracted.print_date,
    language = extracted.language
FROM (
    SELECT 
        filepath,
        filename,
        AI_EXTRACT(
            TO_FILE($SF_STAGE, filepath),
            OBJECT_CONSTRUCT(
                'title', 'The main title or subject of this document, not the company name. Look for text after headers like Update, Report, Protocol, or similar descriptive titles.',
                'print_date', 'Any date found in the document: print date, effective date, revision date, approval date, document date, or date in header/footer',
                'language', 'The primary language the document content is written in'
            )
        ):response AS extracted_data
    FROM parsed_document_ocr
    WHERE page_index = 0
      AND title IS NULL
      AND page_count <= 125  -- TO_FILE limit
) src,
LATERAL (
    SELECT 
        src.extracted_data:title::VARCHAR AS title,
        src.extracted_data:print_date::VARCHAR AS print_date,
        src.extracted_data:language::VARCHAR AS language
) extracted
WHERE p.filepath = src.filepath 
  AND p.filename = src.filename 
  AND p.page_index = 0
  AND p.title IS NULL;

-- Step 2b: Process documents with > 125 pages using parsed content (fallback)
UPDATE parsed_document_ocr p
SET 
    title = extracted.title,
    print_date = extracted.print_date,
    language = extracted.language
FROM (
    SELECT 
        filepath,
        filename,
        AI_EXTRACT(
            first_10_pages_content,
            OBJECT_CONSTRUCT(
                'title', 'The main title or subject of this document, not the company name. Look for text after headers like Update, Report, Protocol, or similar descriptive titles.',
                'print_date', 'Any date found in the document: print date, effective date, revision date, approval date, document date, or date in header/footer',
                'language', 'The primary language the document content is written in'
            )
        ):response AS extracted_data
    FROM (
        SELECT 
            filepath,
            filename,
            LISTAGG(page_content, '\n\n') 
                WITHIN GROUP (ORDER BY page_index) AS first_10_pages_content
        FROM parsed_document_ocr
        WHERE page_index < 10
        GROUP BY filepath, filename
        HAVING MAX(CASE WHEN page_index = 0 THEN title END) IS NULL  -- Only unprocessed docs
           AND MAX(page_count) > 125  -- Large documents only
    )
) src,
LATERAL (
    SELECT 
        src.extracted_data:title::VARCHAR AS title,
        src.extracted_data:print_date::VARCHAR AS print_date,
        src.extracted_data:language::VARCHAR AS language
) extracted
WHERE p.filepath = src.filepath 
  AND p.filename = src.filename 
  AND p.page_index = 0
  AND p.title IS NULL;

-- =============================================================================
-- STEP 3: Generate Summary using AI_COMPLETE
-- =============================================================================

UPDATE parsed_document_ocr p
SET summary = src.new_summary
FROM (
    SELECT 
        filepath,
        filename,
        AI_COMPLETE(
            'claude-3-5-sonnet',
            CONCAT(
                'Provide a brief 2-3 sentence summary of the following document. Be concise and focus on the main topic:\n\n',
                LEFT(combined_content, 8000)
            )
        ) AS new_summary
    FROM (
        SELECT 
            filepath,
            filename,
            LISTAGG(page_content, '\n\n') 
                WITHIN GROUP (ORDER BY page_index) AS combined_content
        FROM parsed_document_ocr
        WHERE page_index < 10
        GROUP BY filepath, filename
        HAVING MAX(CASE WHEN page_index = 0 THEN summary END) IS NULL  -- Only docs without summary
    )
) src
WHERE p.filepath = src.filepath
  AND p.filename = src.filename
  AND p.page_index = 0
  AND p.summary IS NULL;

-- =============================================================================
-- STEP 4: Extract Handwritten Signatures
-- =============================================================================

-- Extract handwritten signatures only (excluding Signatures Pages)
UPDATE parsed_document_ocr p
SET hand_signatures = src.extracted_hand_signatures
FROM (
    SELECT 
        filepath,
        filename,
        AI_EXTRACT(
            file => TO_FILE($SF_STAGE, filepath), 
            responseFormat => PARSE_JSON('{
                "schema": {
                    "type": "object",
                    "properties": {
                        "hand_signatures": {
                            "description": "List handwritten signatures on all pages except the dedicated Signatures Pages. For each, Return: [Name] | [Short Title] | [Date].",
                            "type": "array",
                            "items": {"type": "string"}
                        }
                    }
                }
            }')
        ):response:hand_signatures AS extracted_hand_signatures
    FROM parsed_document_ocr
    WHERE page_index = 0
      AND hand_signatures IS NULL
      AND page_count <= 125  -- TO_FILE limit
      AND filepath LIKE $SF_FOLDER
) src
WHERE p.filepath = src.filepath 
  AND p.filename = src.filename 
  AND p.page_index = 0
  AND p.hand_signatures IS NULL;

-- Verify hand_signatures extraction: check for None values (before cleanup)
SELECT 
    'Documents with hand_signatures (before cleanup)' AS category,
    COUNT(DISTINCT filename) AS count
FROM parsed_document_ocr
WHERE page_index = 0 AND hand_signatures IS NOT NULL;

-- Clean up hand_signatures: keep only entries with real name AND real date
UPDATE parsed_document_ocr p
SET hand_signatures = src.valid_signatures
FROM (
    SELECT 
        pd.filepath,
        pd.filename,
        ARRAY_AGG(s.value) AS valid_signatures
    FROM parsed_document_ocr pd,
    LATERAL FLATTEN(input => pd.hand_signatures) s
    WHERE pd.page_index = 0
      AND pd.hand_signatures IS NOT NULL
      -- Must have a real name (not None or empty)
      AND TRIM(SPLIT_PART(s.value::VARCHAR, '|', 1)) != 'None'
      AND TRIM(SPLIT_PART(s.value::VARCHAR, '|', 1)) != ''
      -- Must have a real date (not None or empty)
      AND TRIM(SPLIT_PART(s.value::VARCHAR, '|', 3)) != 'None'
      AND TRIM(SPLIT_PART(s.value::VARCHAR, '|', 3)) != ''
    GROUP BY pd.filepath, pd.filename
) src
WHERE p.filepath = src.filepath
  AND p.filename = src.filename
  AND p.page_index = 0;

-- Set hand_signatures to NULL for documents with no valid signatures
UPDATE parsed_document_ocr p
SET hand_signatures = NULL
WHERE p.page_index = 0
  AND p.hand_signatures IS NOT NULL
  AND p.filepath NOT IN (
      SELECT DISTINCT pd.filepath
      FROM parsed_document_ocr pd,
      LATERAL FLATTEN(input => pd.hand_signatures) s
      WHERE pd.page_index = 0
        AND pd.hand_signatures IS NOT NULL
        AND TRIM(SPLIT_PART(s.value::VARCHAR, '|', 1)) != 'None'
        AND TRIM(SPLIT_PART(s.value::VARCHAR, '|', 1)) != ''
        AND TRIM(SPLIT_PART(s.value::VARCHAR, '|', 3)) != 'None'
        AND TRIM(SPLIT_PART(s.value::VARCHAR, '|', 3)) != ''
  );

-- Verify hand_signatures after cleanup
SELECT 
    'Documents with valid hand_signatures (after cleanup)' AS category,
    COUNT(DISTINCT filename) AS count
FROM parsed_document_ocr
WHERE page_index = 0 
  AND hand_signatures IS NOT NULL
  AND ARRAY_SIZE(hand_signatures) > 0;

-- =============================================================================
-- STEP 5: Propagate Metadata to All Pages
-- =============================================================================

UPDATE parsed_document_ocr p
SET 
    title = src.title,
    print_date = src.print_date,
    language = src.language,
    summary = src.summary,
    hand_signatures = src.hand_signatures
FROM (
    SELECT filepath, filename, title, print_date, language, summary, hand_signatures
    FROM parsed_document_ocr
    WHERE page_index = 0
      AND title IS NOT NULL
) src
WHERE p.filepath = src.filepath
  AND p.filename = src.filename
  AND p.page_index > 0
  AND p.title IS NULL;

-- Verify extracted metadata
SELECT 
    filename,
    title,
    print_date,
    language,
    LEFT(summary, 200) AS summary_preview
FROM parsed_document_ocr 
WHERE page_index = 0
ORDER BY filepath 
LIMIT 20;

-- =============================================================================
-- STEP 6: Create Document Chunks with Embedded Signatures
-- =============================================================================

-- Create a helper table with signature text per document
-- Data is already cleaned (only valid signatures with real name AND date remain)
CREATE OR REPLACE TEMPORARY TABLE temp_signature_text AS
SELECT 
    filepath,
    filename,
    LISTAGG('Signature: ' || TRIM(SPLIT_PART(s.value::VARCHAR, '|', 1)) 
            || ' | Title: ' || TRIM(SPLIT_PART(s.value::VARCHAR, '|', 2))
            || ' | Date: ' || TRIM(SPLIT_PART(s.value::VARCHAR, '|', 3)), '\n') AS signature_text
FROM parsed_document_ocr pd,
LATERAL FLATTEN(input => pd.hand_signatures) s
WHERE pd.page_index = 0
  AND pd.hand_signatures IS NOT NULL
  AND ARRAY_SIZE(pd.hand_signatures) > 0
GROUP BY filepath, filename;

-- Create doc_chunks_ocr_signature table with ALL pages
-- Handwritten signatures are only embedded in page_index = 0 chunks
CREATE OR REPLACE TABLE doc_chunks_ocr_signature AS
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
    GET_PRESIGNED_URL($SF_STAGE, pd.filepath, 604800) AS presigned_url,
    (
        pd.filepath || ' - Page ' || pd.page_index || ':\n'
        || COALESCE('Title: ' || pd.title || '\n', '')
        || c.value['chunk']
        || CASE 
            WHEN pd.page_index = 0 AND sig.signature_text IS NOT NULL AND sig.signature_text != '' 
            THEN '\n\n--- Handwritten Signatures ---\n' || sig.signature_text 
            ELSE '' 
           END
    ) AS chunk,
    c.index AS chunk_index
FROM parsed_document_ocr pd
LEFT JOIN temp_signature_text sig 
    ON pd.filepath = sig.filepath AND pd.filename = sig.filename,
LATERAL FLATTEN(
    SNOWFLAKE.CORTEX.SPLIT_TEXT_MARKDOWN_HEADER(
        pd.page_content,
        OBJECT_CONSTRUCT('#', 'header_1', '##', 'header_2'),
        2000,
        300
    )
) c;

-- Verify chunked data
SELECT 
    COUNT(*) AS total_chunks,
    COUNT(DISTINCT filepath) AS total_documents,
    SUM(CASE WHEN chunk LIKE '%Handwritten Signatures%' THEN 1 ELSE 0 END) AS chunks_with_signatures,
    AVG(LENGTH(chunk)) AS avg_chunk_length
FROM doc_chunks_ocr_signature;

-- Sample chunks with signatures
SELECT 
    filename,
    RIGHT(chunk, 300) AS chunk_end_preview
FROM doc_chunks_ocr_signature
WHERE chunk LIKE '%Handwritten Signatures%'
LIMIT 5;

-- =============================================================================
-- STEP 7: Create Handwritten Signature View
-- =============================================================================

-- View for handwritten signatures (data already cleaned - only valid signatures remain)
CREATE OR REPLACE VIEW v_hand_signatures AS
SELECT 
    p.filepath,
    p.filename,
    p.title,
    p.print_date,
    s.value::VARCHAR AS signature_raw,
    TRIM(SPLIT_PART(s.value::VARCHAR, '|', 1)) AS signer_name,
    TRIM(SPLIT_PART(s.value::VARCHAR, '|', 2)) AS signer_title,
    TRIM(SPLIT_PART(s.value::VARCHAR, '|', 3)) AS signature_date
FROM parsed_document_ocr p,
LATERAL FLATTEN(input => p.hand_signatures) s
WHERE p.page_index = 0
  AND p.hand_signatures IS NOT NULL
  AND ARRAY_SIZE(p.hand_signatures) > 0;

-- =============================================================================
-- STEP 8: Create Cortex Search Service
-- =============================================================================

-- Create Cortex Search service for semantic search over document chunks with signatures
CREATE OR REPLACE CORTEX SEARCH SERVICE document_search_ocr_signature
ON chunk
ATTRIBUTES title, filename, filepath, language, print_date, summary, page_index
WAREHOUSE = COMPUTE_WH
TARGET_LAG = '1 hour'
AS (
    SELECT 
        filepath,
        filename,
        presigned_url,
        page_index,
        page_count,
        chunk_index,
        chunk,
        title,
        print_date,
        language,
        summary
    FROM doc_chunks_ocr_signature
);

-- Check Cortex Search service status
SHOW CORTEX SEARCH SERVICES;

-- Describe the search service
DESCRIBE CORTEX SEARCH SERVICE document_search_ocr_signature;

-- =============================================================================
-- VERIFICATION & SUMMARY
-- =============================================================================

-- Summary: total documents, pages, chunks, and signatures
SELECT 
    COUNT(DISTINCT filepath) AS total_documents,
    COUNT(*) AS total_pages,
    (SELECT COUNT(*) FROM doc_chunks_ocr_signature) AS total_chunks,
    (SELECT COUNT(*) FROM v_hand_signatures) AS total_hand_signatures
FROM parsed_document_ocr;

-- Handwritten signature statistics
SELECT 
    COUNT(DISTINCT filename) AS documents_with_signatures,
    COUNT(*) AS total_signatures
FROM v_hand_signatures;

-- Summary statistics by language
SELECT 
    language,
    COUNT(DISTINCT filename) AS document_count
FROM parsed_document_ocr
WHERE page_index = 0
GROUP BY language
ORDER BY document_count DESC;

-- Documents with missing metadata
SELECT 
    filename,
    CASE WHEN title IS NULL OR title = '' THEN 'Missing' ELSE 'OK' END AS title_status,
    CASE WHEN print_date IS NULL OR print_date = '' THEN 'Missing' ELSE 'OK' END AS date_status,
    CASE WHEN language IS NULL OR language = '' THEN 'Missing' ELSE 'OK' END AS language_status,
    CASE WHEN summary IS NULL OR summary = '' THEN 'Missing' ELSE 'OK' END AS summary_status
FROM parsed_document_ocr
WHERE page_index = 0
  AND (title IS NULL OR title = '' 
       OR print_date IS NULL OR print_date = ''
       OR language IS NULL OR language = ''
       OR summary IS NULL OR summary = '');

-- =============================================================================
-- EXAMPLE SEARCH QUERIES
-- =============================================================================

-- Basic semantic search
SELECT PARSE_JSON(
    SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
        'document_search_ocr_signature',
        '{
            "query": "clinical trial protocol",
            "columns": ["filepath", "filename", "page_index", "title", "chunk"],
            "limit": 10
        }'
    )
):results AS search_results;

-- Search for documents signed by a specific person
SELECT PARSE_JSON(
    SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
        'document_search_ocr_signature',
        '{
            "query": "signed by Anna Nilsson",
            "columns": ["filepath", "filename", "title", "chunk"],
            "limit": 10
        }'
    )
):results AS search_results;

-- Search with filter by language
SELECT PARSE_JSON(
    SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
        'document_search_ocr_signature',
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
        'document_search_ocr_signature',
        '{
            "query": "adverse events safety",
            "columns": ["filepath", "filename", "title", "chunk"],
            "filter": {"@contains": {"filepath": "Clinical"}},
            "limit": 10
        }'
    )
):results AS search_results;

-- =============================================================================
-- EXAMPLE SIGNATURE QUERIES
-- =============================================================================

-- Find all handwritten signatures by a specific person
-- SELECT * FROM v_hand_signatures WHERE signer_name ILIKE '%Pickett%';

-- Find documents signed in a specific year
-- SELECT * FROM v_hand_signatures WHERE signature_date LIKE '%2023%';

-- List all documents with handwritten signatures
-- SELECT DISTINCT filename, title FROM v_hand_signatures;
