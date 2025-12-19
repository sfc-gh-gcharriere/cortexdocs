-- Configuration
SET SF_DATABASE = 'DOCS';
SET SF_SCHEMA = 'PUBLIC';
SET SF_STAGE = '@DOCS.PUBLIC.STG';

-- Folder filter: set to folder path to process specific folder, or '%' for all documents
-- Examples: 'Clinical/%', '%' (all)
SET SF_FOLDER = 'PROJ-17483.pdf';

-- Set context
USE DATABASE IDENTIFIER($SF_DATABASE);
USE SCHEMA IDENTIFIER($SF_SCHEMA);

-- Create table for parsed documents (if not exists)
CREATE TABLE IF NOT EXISTS parsed_document (
    filepath VARCHAR,           -- Full relative path (e.g., 'Clinical/Studies/subfolder/file.pdf')
    filename VARCHAR,           -- Just the filename (e.g., 'file.pdf')
    page_count INT,             -- Total pages in document
    page_index INT,             -- Current page index (0-based)
    page_content VARCHAR,       -- Page content text (extracted from parsed_doc:content)
    parsed_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Parse and insert documents from the specified folder
-- Only parses documents not already in the table
INSERT INTO parsed_document (filepath, filename, page_count, page_index, page_content)
WITH files AS (
    SELECT RELATIVE_PATH AS filepath
    FROM DIRECTORY(@STG) 
    WHERE RELATIVE_PATH LIKE $SF_FOLDER
      AND RELATIVE_PATH NOT IN (SELECT DISTINCT filepath FROM parsed_document)  -- Skip already parsed
),
parsed AS (
    SELECT 
        f.filepath,
        SPLIT_PART(f.filepath, '/', -1) AS filename,
        AI_PARSE_DOCUMENT(
            TO_FILE($SF_STAGE, f.filepath),
            { 'mode': 'OCR', 'page_split': true }
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

-- Verify parsed documents for the folder
SELECT 
    filename, 
    filepath,
    page_count, 
    COUNT(*) AS pages_inserted 
FROM parsed_document 
WHERE filepath LIKE $SF_FOLDER
GROUP BY filename, filepath, page_count 
ORDER BY filepath;

-- Summary: total documents and pages parsed
SELECT 
    COUNT(DISTINCT filepath) AS total_documents,
    COUNT(*) AS total_pages
FROM parsed_document
WHERE filepath LIKE $SF_FOLDER;
