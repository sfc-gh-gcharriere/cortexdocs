-- =============================================================================
-- Extract Handwritten Signatures from Documents
-- =============================================================================
-- This script extracts handwritten signatures (Name | Title | Date format)
-- from all pages except the dedicated Signatures Pages.
--
-- Note: This script is included in all_in_one_ocr.sql as Steps 4, 6, and 7.
--       Run this separately only if you need to re-extract signatures.
-- =============================================================================

-- Configuration
SET SF_DATABASE = 'GLD';
SET SF_SCHEMA = 'PUBLIC';
SET SF_STAGE = '@GLD.PUBLIC.STG';

-- Folder filter: set to specific file or folder, or '%' for all
SET SF_FOLDER = '%';

-- Set context
USE DATABASE IDENTIFIER($SF_DATABASE);
USE SCHEMA IDENTIFIER($SF_SCHEMA);

-- =============================================================================
-- STEP 1: Add hand_signatures column to parsed_document_ocr table
-- =============================================================================

ALTER TABLE parsed_document_ocr
ADD COLUMN IF NOT EXISTS hand_signatures VARIANT;

-- =============================================================================
-- STEP 2: Extract handwritten signatures from documents
-- =============================================================================
-- Uses AI_EXTRACT with responseFormat to extract handwritten signatures
-- Excludes dedicated Signatures Pages

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
-- STEP 3: Propagate signatures from page 0 to all other pages
-- =============================================================================

UPDATE parsed_document_ocr p
SET hand_signatures = src.hand_signatures
FROM (
    SELECT filepath, filename, hand_signatures
    FROM parsed_document_ocr
    WHERE page_index = 0
      AND hand_signatures IS NOT NULL
) src
WHERE p.filepath = src.filepath
  AND p.filename = src.filename
  AND p.page_index > 0
  AND p.hand_signatures IS NULL;

-- =============================================================================
-- STEP 4: Create view for handwritten signatures
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
-- STEP 5: Create doc_chunks_ocr_signature table with embedded signatures
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

-- =============================================================================
-- Verification queries
-- =============================================================================

-- View handwritten signatures (None entries excluded)
SELECT * FROM v_hand_signatures LIMIT 20;

-- Summary statistics
SELECT 
    COUNT(DISTINCT filename) AS documents_with_signatures,
    COUNT(*) AS total_signatures
FROM v_hand_signatures;

-- Chunks statistics
SELECT 
    COUNT(*) AS total_chunks,
    SUM(CASE WHEN chunk LIKE '%Handwritten Signatures%' THEN 1 ELSE 0 END) AS chunks_with_signatures,
    SUM(CASE WHEN page_index = 0 THEN 1 ELSE 0 END) AS page_0_chunks,
    SUM(CASE WHEN page_index > 0 THEN 1 ELSE 0 END) AS other_page_chunks
FROM doc_chunks_ocr_signature;

-- Sample chunks with signatures
SELECT 
    filename,
    RIGHT(chunk, 300) AS chunk_end_preview
FROM doc_chunks_ocr_signature
WHERE chunk LIKE '%Handwritten Signatures%'
LIMIT 5;

-- Documents without any handwritten signatures
SELECT 
    filename,
    title
FROM parsed_document_ocr
WHERE page_index = 0
  AND (hand_signatures IS NULL 
       OR ARRAY_SIZE(hand_signatures) = 0 
       OR hand_signatures[0]::VARCHAR = 'None|None|None')
ORDER BY filepath
LIMIT 20;

-- =============================================================================
-- Example queries
-- =============================================================================

-- Find all handwritten signatures by a specific person
-- SELECT * FROM v_hand_signatures WHERE signer_name ILIKE '%Pickett%';

-- Find documents signed in a specific year
-- SELECT * FROM v_hand_signatures WHERE signature_date LIKE '%2023%';

-- List all documents with handwritten signatures
-- SELECT DISTINCT filename, title FROM v_hand_signatures;

-- Search for signed documents in Cortex Search
-- SELECT PARSE_JSON(
--     SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
--         'document_search_ocr_signature',
--         '{
--             "query": "signed by Anna Nilsson",
--             "columns": ["filepath", "filename", "title", "chunk"],
--             "limit": 10
--         }'
--     )
-- ):results AS search_results;
