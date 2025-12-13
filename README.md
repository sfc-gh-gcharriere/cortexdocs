# Document Search

Upload and parse PDF documents to Snowflake using AI_PARSE_DOCUMENT, extract metadata using AI_EXTRACT and AI_COMPLETE, and enable semantic search with Cortex Search.

## Prerequisites

- [Snow CLI](https://docs.snowflake.com/en/developer-guide/snowflake-cli-v2/index) installed and configured
- Snowflake account with Cortex AI features enabled

## Configuration

All scripts use the following configuration (edit at the top of each file if needed):

| Setting | Value |
|---------|-------|
| Database | `DOCS` |
| Schema | `PUBLIC` |
| Stage | `STG` |

## Setup

### 1. Create Database, Schema, and Stage

Run the setup SQL to create all Snowflake objects:

```bash
snow sql -f 1_setup.sql
```

This will:
- Enable Cortex cross-region support
- Create database `DOCS`
- Create schema `PUBLIC`
- Create stage `STG` with directory enabled and SSE encryption

### 2. Upload PDF Files

Place your PDF files in the `./data` directory (subdirectories are supported).

Run the upload script:

```bash
./upload_to_snowflake.sh
```

This will:
- Scan `./data` for all PDF files
- Upload files to the stage preserving directory structure
- Refresh stage metadata
- Display upload summary

### 3. Parse Documents

Run the parse commands to extract text from PDFs using AI:

```bash
snow sql -f 2_ai_parse.sql --database DOCS --schema PUBLIC
```

This will:
- Create the `parsed_document` table
- Parse PDFs using `AI_PARSE_DOCUMENT` with layout mode and page splitting
- Store extracted content per page

**Configuration**: Edit `SF_FOLDER` variable at the top to process specific folders:
```sql
SET SF_FOLDER = 'Clinical/%';  -- Process only Clinical folder
SET SF_FOLDER = '%';           -- Process all documents
```

### 4. Extract Metadata and Summaries

Run the extraction commands to get metadata and summaries:

```bash
snow sql -f 3_ai_extract.sql --database DOCS --schema PUBLIC
```

This will:
- Add `title`, `print_date`, `language`, `summary` columns to `parsed_document`
- Extract metadata using `AI_EXTRACT` with `TO_FILE` (accesses PDF metadata for dates)
- Generate summaries using `AI_COMPLETE` with Claude 3.5 Sonnet
- Propagate metadata from page 0 to all pages of each document

#### AI_EXTRACT

Uses Snowflake Cortex `AI_EXTRACT` function to extract structured metadata from documents:

```sql
AI_EXTRACT(
    TO_FILE('@STAGE', filepath),
    OBJECT_CONSTRUCT(
        'title', 'The main title or subject of this document...',
        'print_date', 'Any date found in the document...',
        'language', 'The primary language...'
    )
)
```

**Key features:**
- Uses `TO_FILE` to access original PDF (better date extraction from PDF metadata)
- Documents ≤125 pages: Uses `TO_FILE` directly
- Documents >125 pages: Falls back to parsed content (first 10 pages)
- Only processes documents not yet extracted (idempotent)

#### AI_COMPLETE

Uses Snowflake Cortex `AI_COMPLETE` function to generate document summaries:

```sql
AI_COMPLETE(
    'claude-3-5-sonnet',
    CONCAT(
        'Provide a brief 2-3 sentence summary...',
        LEFT(combined_content, 8000)
    )
)
```

**Key features:**
- Uses Claude 3.5 Sonnet model for high-quality summaries
- Combines first 10 pages content for better context (8000 chars max)
- Only processes documents without summaries (idempotent)

### 5. Create Cortex Search Service

Run the Cortex Search setup to enable semantic search:

```bash
snow sql -f 4_cortex_search.sql --database DOCS --schema PUBLIC
```

This will:
- Create `doc_chunks` table with chunked document content
- Create `document_search` Cortex Search service for semantic search

#### Document Chunking

Uses `SPLIT_TEXT_MARKDOWN_HEADER` to create searchable chunks:

```sql
SNOWFLAKE.CORTEX.SPLIT_TEXT_MARKDOWN_HEADER(
    page_content,
    OBJECT_CONSTRUCT('#', 'header_1', '##', 'header_2'),
    2000,  -- chunk size (characters)
    300    -- overlap (characters)
)
```

**Key features:**
- Splits content into ~2000 character chunks with 300 char overlap
- Preserves markdown headers (`header_1`, `header_2`) for context
- Includes document metadata (filepath, title, page) in each chunk
- Creates scoped file URLs for document access

#### Cortex Search Service

Creates a semantic search service on the chunked documents:

```sql
CREATE CORTEX SEARCH SERVICE document_search
ON chunk
ATTRIBUTES title, filename, filepath, language, print_date, summary, header_1, header_2, page_index
WAREHOUSE = COMPUTE_WH
TARGET_LAG = '1 hour'
```

**Searchable attributes:**
- `chunk` - Main searchable content
- `title`, `filename`, `filepath` - Document identifiers
- `language`, `print_date`, `summary` - Metadata
- `header_1`, `header_2` - Section headers
- `page_index` - Page location

## File Structure

```
.
├── README.md                 # This file
├── 1_setup.sql               # Database, schema, stage creation
├── 2_ai_parse.sql            # Document parsing commands
├── 3_ai_extract.sql          # Metadata extraction and summaries
├── 4_cortex_search.sql       # Chunking and Cortex Search service
├── 5_cost.sql                # Cost analysis queries
├── upload_to_snowflake.sh    # Upload script for PDF files
└── data/                     # PDF files to upload (not tracked in git)
```

## Table Schemas

### parsed_document

| Column | Type | Description |
|--------|------|-------------|
| `filepath` | VARCHAR | Full relative path (e.g., `Clinical/Studies/file.pdf`) |
| `filename` | VARCHAR | Just the filename |
| `page_count` | INT | Total pages in document |
| `page_index` | INT | Current page index (0-based) |
| `page_content` | VARCHAR | Page content text |
| `parsed_at` | TIMESTAMP | When the document was parsed |
| `title` | VARCHAR | Document title (from AI_EXTRACT) |
| `print_date` | VARCHAR | Document date (from AI_EXTRACT) |
| `language` | VARCHAR | Document language (from AI_EXTRACT) |
| `summary` | VARCHAR | Document summary (from AI_COMPLETE) |

### doc_chunks

| Column | Type | Description |
|--------|------|-------------|
| `filepath` | VARCHAR | Full relative path |
| `filename` | VARCHAR | Just the filename |
| `page_index` | INT | Page index (0-based) |
| `page_count` | INT | Total pages in document |
| `title` | VARCHAR | Document title |
| `print_date` | VARCHAR | Document date |
| `language` | VARCHAR | Document language |
| `summary` | VARCHAR | Document summary |
| `file_url` | VARCHAR | Scoped URL to original file |
| `chunk` | VARCHAR | Searchable chunk with context |
| `header_1` | VARCHAR | Level 1 markdown header |
| `header_2` | VARCHAR | Level 2 markdown header |
| `chunk_index` | INT | Chunk index within page |

## Example Queries

### Basic SQL Queries

```sql
-- Count files in stage
SELECT COUNT(*) FROM DIRECTORY(@STG);

-- List all parsed documents with metadata
SELECT DISTINCT filename, title, print_date, language, page_count 
FROM parsed_document 
WHERE page_index = 0;

-- Search by title
SELECT filename, title, LEFT(summary, 200) AS summary_preview
FROM parsed_document 
WHERE page_index = 0
  AND title ILIKE '%protocol%';

-- Search content
SELECT filename, page_index, page_content 
FROM parsed_document 
WHERE page_content ILIKE '%search term%';

-- Documents by language
SELECT language, COUNT(DISTINCT filename) AS document_count
FROM parsed_document
WHERE page_index = 0
GROUP BY language
ORDER BY document_count DESC;
```

### Cortex Search Queries (Semantic Search)

```sql
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

-- Search with language filter
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

-- Search within specific folder
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
```

