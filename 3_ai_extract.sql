-- Configuration
SET SF_DATABASE = 'DOCS';
SET SF_SCHEMA = 'PUBLIC';
SET SF_STAGE = '@DOCS.PUBLIC.STG';

-- Folder filter: set to specific file or folder, or '%' for all
SET SF_FOLDER = '%';

-- Set context
USE DATABASE IDENTIFIER($SF_DATABASE);
USE SCHEMA IDENTIFIER($SF_SCHEMA);

-- Add new columns to parsed_document table
ALTER TABLE parsed_document
ADD COLUMN IF NOT EXISTS title VARCHAR;

ALTER TABLE parsed_document
ADD COLUMN IF NOT EXISTS print_date VARCHAR;

ALTER TABLE parsed_document
ADD COLUMN IF NOT EXISTS language VARCHAR;

ALTER TABLE parsed_document
ADD COLUMN IF NOT EXISTS summary VARCHAR;

ALTER TABLE parsed_document
ADD COLUMN IF NOT EXISTS hand_signatures VARIANT;

-- =============================================================================
-- STEP 1: Extract metadata (title, print_date, language) using AI_EXTRACT
-- =============================================================================

-- Step 1a: Process documents with <= 125 pages using TO_FILE (better date extraction)
UPDATE parsed_document p
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
    FROM parsed_document
    WHERE page_index = 0
      AND title IS NULL
      AND page_count <= 125  -- TO_FILE limit
      AND filepath LIKE $SF_FOLDER
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

-- Step 1b: Process documents with > 125 pages using parsed content (fallback)
UPDATE parsed_document p
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
        FROM parsed_document
        WHERE page_index < 10
          AND filepath LIKE $SF_FOLDER
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
-- STEP 2: Generate summary using AI_COMPLETE with first 10 pages content
-- =============================================================================

UPDATE parsed_document p
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
        FROM parsed_document
        WHERE page_index < 10
          AND filepath LIKE $SF_FOLDER
        GROUP BY filepath, filename
        HAVING MAX(CASE WHEN page_index = 0 THEN summary END) IS NULL  -- Only docs without summary
    )
) src
WHERE p.filepath = src.filepath
  AND p.filename = src.filename
  AND p.page_index = 0
  AND p.summary IS NULL;

-- =============================================================================
-- STEP 3: Extract handwritten signatures using AI_EXTRACT
-- =============================================================================

UPDATE parsed_document p
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
    FROM parsed_document
    WHERE page_index = 0
      AND hand_signatures IS NULL
      AND page_count <= 125  -- TO_FILE limit
      AND filepath LIKE $SF_FOLDER
) src
WHERE p.filepath = src.filepath 
  AND p.filename = src.filename 
  AND p.page_index = 0
  AND p.hand_signatures IS NULL;

-- Clean up hand_signatures: keep only entries with real name AND real date
UPDATE parsed_document p
SET hand_signatures = src.valid_signatures
FROM (
    SELECT 
        pd.filepath,
        pd.filename,
        ARRAY_AGG(s.value) AS valid_signatures
    FROM parsed_document pd,
    LATERAL FLATTEN(input => pd.hand_signatures) s
    WHERE pd.page_index = 0
      AND pd.hand_signatures IS NOT NULL
      AND pd.filepath LIKE $SF_FOLDER
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
UPDATE parsed_document p
SET hand_signatures = NULL
WHERE p.page_index = 0
  AND p.hand_signatures IS NOT NULL
  AND p.filepath LIKE $SF_FOLDER
  AND p.filepath NOT IN (
      SELECT DISTINCT pd.filepath
      FROM parsed_document pd,
      LATERAL FLATTEN(input => pd.hand_signatures) s
      WHERE pd.page_index = 0
        AND pd.hand_signatures IS NOT NULL
        AND TRIM(SPLIT_PART(s.value::VARCHAR, '|', 1)) != 'None'
        AND TRIM(SPLIT_PART(s.value::VARCHAR, '|', 1)) != ''
        AND TRIM(SPLIT_PART(s.value::VARCHAR, '|', 3)) != 'None'
        AND TRIM(SPLIT_PART(s.value::VARCHAR, '|', 3)) != ''
  );

-- =============================================================================
-- STEP 4: Propagate metadata from page 0 to all other pages
-- =============================================================================

UPDATE parsed_document p
SET 
    title = src.title,
    print_date = src.print_date,
    language = src.language,
    summary = src.summary,
    hand_signatures = src.hand_signatures
FROM (
    SELECT filepath, filename, title, print_date, language, summary, hand_signatures
    FROM parsed_document
    WHERE page_index = 0
      AND title IS NOT NULL
      AND filepath LIKE $SF_FOLDER
) src
WHERE p.filepath = src.filepath
  AND p.filename = src.filename
  AND p.page_index > 0
  AND p.title IS NULL;

-- =============================================================================
-- Verification queries
-- =============================================================================

-- Verify extracted metadata
SELECT 
    filename,
    title,
    print_date,
    language,
    LEFT(summary, 200) AS summary_preview,
    CASE WHEN hand_signatures IS NOT NULL THEN 'Yes' ELSE 'No' END AS has_signatures
FROM parsed_document 
WHERE page_index = 0
  AND filepath LIKE $SF_FOLDER
ORDER BY filepath 
LIMIT 20;

-- Summary statistics by language
SELECT 
    language,
    COUNT(DISTINCT filename) AS document_count
FROM parsed_document
WHERE page_index = 0
  AND filepath LIKE $SF_FOLDER
GROUP BY language
ORDER BY document_count DESC;

-- Documents with valid signatures
SELECT 
    'Documents with valid hand_signatures' AS category,
    COUNT(DISTINCT filename) AS count
FROM parsed_document
WHERE page_index = 0 
  AND hand_signatures IS NOT NULL
  AND ARRAY_SIZE(hand_signatures) > 0
  AND filepath LIKE $SF_FOLDER;

-- Documents with missing metadata
SELECT 
    filename,
    CASE WHEN title IS NULL OR title = '' THEN 'Missing' ELSE 'OK' END AS title_status,
    CASE WHEN print_date IS NULL OR print_date = '' THEN 'Missing' ELSE 'OK' END AS date_status,
    CASE WHEN language IS NULL OR language = '' THEN 'Missing' ELSE 'OK' END AS language_status,
    CASE WHEN summary IS NULL OR summary = '' THEN 'Missing' ELSE 'OK' END AS summary_status
FROM parsed_document
WHERE page_index = 0
  AND filepath LIKE $SF_FOLDER
  AND (title IS NULL OR title = '' 
       OR print_date IS NULL OR print_date = ''
       OR language IS NULL OR language = ''
       OR summary IS NULL OR summary = '');
