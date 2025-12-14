-- Configuration
SET SF_DATABASE = 'GLD';
SET SF_SCHEMA = 'PUBLIC';
SET SF_DOCUMENT = 'PROJ-5192.pdf';  -- Specify document to process (use '%' for all)

-- Set context
USE DATABASE IDENTIFIER($SF_DATABASE);
USE SCHEMA IDENTIFIER($SF_SCHEMA);

-- =============================================================================
-- Create table to store extracted figure information
-- =============================================================================

CREATE TABLE IF NOT EXISTS page_figures (
    filepath VARCHAR,
    filename VARCHAR,
    page_index INT,
    figure_number VARCHAR,
    figure_title VARCHAR,
    image_references VARCHAR,
    extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- =============================================================================
-- Extract figure information from pages using AI_EXTRACT
-- =============================================================================
-- Uses AI_EXTRACT to identify figure numbers, titles, and image references
-- from each page of parsed documents.

INSERT INTO page_figures (filepath, filename, page_index, figure_number, figure_title, image_references)
SELECT 
    p.filepath,
    p.filename,
    p.page_index,
    extracted_data:figure_number::VARCHAR AS figure_number,
    extracted_data:figure_title::VARCHAR AS figure_title,
    extracted_data:image_references::VARCHAR AS image_references
FROM parsed_document p,
LATERAL (
    SELECT AI_EXTRACT(
        p.page_content,
        OBJECT_CONSTRUCT(
            'figure_number', 'The figure number (e.g. 14.2.1.1)',
            'figure_title', 'The full title or description of the figure',
            'image_references', 'List of image file references found (e.g. img-0.jpeg)'
        )
    ):response AS extracted_data
) extraction
WHERE p.has_images = TRUE
  AND p.filename LIKE $SF_DOCUMENT
  AND NOT EXISTS (
    SELECT 1 FROM page_figures pf 
    WHERE pf.filepath = p.filepath 
      AND pf.filename = p.filename 
      AND pf.page_index = p.page_index
  );

-- =============================================================================
-- Create table to store image analysis results
-- =============================================================================

CREATE TABLE IF NOT EXISTS image_analysis (
    filepath VARCHAR,
    filename VARCHAR,
    page_index INT,
    figure_number VARCHAR,
    image_type VARCHAR,
    image_description VARCHAR,
    data_type VARCHAR,
    analyzed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- =============================================================================
-- Analyze images using AI_COMPLETE (coming soon)
-- =============================================================================
-- Uses AI_COMPLETE with claude-3-5-sonnet to analyze images embedded in PDF pages.
-- Note: AI_COMPLETE on images embedded in PDFs is planned to be supported soon.

-- INSERT INTO image_analysis (filepath, filename, page_index, figure_number, image_type, image_description, data_type)
-- SELECT 
--     pf.filepath,
--     pf.filename,
--     pf.page_index,
--     pf.figure_number,
--     analysis:type::VARCHAR AS image_type,
--     analysis:description::VARCHAR AS image_description,
--     analysis:data_type::VARCHAR AS data_type
-- FROM page_figures pf,
-- LATERAL (
--     SELECT PARSE_JSON(
--         AI_COMPLETE(
--             'claude-3-5-sonnet',
--             'Describe what you see in this image. Focus on any charts, graphs, or data visualizations. Respond in JSON only with fields: type (chart/graph/table/image), description, data_type.',
--             TO_FILE('@STG', pf.filepath),
--             OBJECT_CONSTRUCT('page', pf.page_index)
--         )
--     ) AS analysis
-- ) img_analysis
-- WHERE pf.filename LIKE $SF_DOCUMENT
--   AND pf.image_references IS NOT NULL 
--   AND pf.image_references != 'None'
--   AND NOT EXISTS (
--     SELECT 1 FROM image_analysis ia 
--     WHERE ia.filepath = pf.filepath 
--       AND ia.filename = pf.filename 
--       AND ia.page_index = pf.page_index
--   );

-- =============================================================================
-- Verification queries
-- =============================================================================

-- View extracted figures
SELECT 
    filename,
    page_index,
    figure_number,
    figure_title,
    image_references
FROM page_figures
ORDER BY filename, page_index
LIMIT 20;

-- Summary statistics
SELECT 
    COUNT(*) AS total_pages_with_figures,
    COUNT(DISTINCT filename) AS documents_with_figures,
    COUNT(CASE WHEN figure_number IS NOT NULL AND figure_number != 'None' THEN 1 END) AS pages_with_figure_numbers,
    COUNT(CASE WHEN image_references IS NOT NULL AND image_references != 'None' THEN 1 END) AS pages_with_image_refs
FROM page_figures;

-- View image analysis results (when available)
-- SELECT 
--     filename,
--     page_index,
--     figure_number,
--     image_type,
--     image_description,
--     data_type
-- FROM image_analysis
-- ORDER BY filename, page_index
-- LIMIT 20;
