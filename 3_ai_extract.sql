-- Configuration
SET SF_DATABASE = 'DOCS';
SET SF_SCHEMA = 'PUBLIC';
SET SF_STAGE = '@DOCS.PUBLIC.STG';

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
ADD COLUMN IF NOT EXISTS has_images BOOLEAN;

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
        GROUP BY filepath, filename
        HAVING MAX(CASE WHEN page_index = 0 THEN summary END) IS NULL  -- Only docs without summary
    )
) src
WHERE p.filepath = src.filepath
  AND p.filename = src.filename
  AND p.page_index = 0
  AND p.summary IS NULL;

-- =============================================================================
-- STEP 3: Detect images on each page using AI_FILTER
-- =============================================================================

UPDATE parsed_document
SET has_images = ai_filter(
    prompt('Excluding the document header, does the content include any images that feature a caption or figure number?: {0}', page_content)
)
WHERE has_images IS NULL;

-- =============================================================================
-- STEP 4: Propagate metadata from page 0 to all other pages
-- =============================================================================

UPDATE parsed_document p
SET 
    title = src.title,
    print_date = src.print_date,
    language = src.language,
    summary = src.summary
FROM (
    SELECT filepath, filename, title, print_date, language, summary
    FROM parsed_document
    WHERE page_index = 0
      AND title IS NOT NULL
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
    has_images
FROM parsed_document 
WHERE page_index = 0
ORDER BY filepath 
LIMIT 20;

-- Summary statistics by language
SELECT 
    language,
    COUNT(DISTINCT filename) AS document_count
FROM parsed_document
WHERE page_index = 0
GROUP BY language
ORDER BY document_count DESC;

-- Image statistics per document
SELECT 
    filename,
    MAX(page_count) AS page_count,
    COUNT(CASE WHEN has_images = TRUE THEN 1 END) AS pages_with_images,
    ROUND(COUNT(CASE WHEN has_images = TRUE THEN 1 END) * 100.0 / MAX(page_count), 1) AS image_percentage
FROM parsed_document
GROUP BY filepath, filename
ORDER BY pages_with_images DESC
LIMIT 20;

-- Documents with missing metadata
SELECT 
    filename,
    CASE WHEN title IS NULL OR title = '' THEN 'Missing' ELSE 'OK' END AS title_status,
    CASE WHEN print_date IS NULL OR print_date = '' THEN 'Missing' ELSE 'OK' END AS date_status,
    CASE WHEN language IS NULL OR language = '' THEN 'Missing' ELSE 'OK' END AS language_status,
    CASE WHEN summary IS NULL OR summary = '' THEN 'Missing' ELSE 'OK' END AS summary_status,
    CASE WHEN has_images IS NULL THEN 'Missing' ELSE 'OK' END AS has_images_status
FROM parsed_document
WHERE page_index = 0
  AND (title IS NULL OR title = '' 
       OR print_date IS NULL OR print_date = ''
       OR language IS NULL OR language = ''
       OR summary IS NULL OR summary = ''
       OR has_images IS NULL);
