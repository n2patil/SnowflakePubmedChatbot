--PubMed Central
create if not exists database PMC_DATA;
create schema pmc_oa_opendata;
create warehouse pmc_wh WAREHOUSE_SIZE = 'small' WAREHOUSE_TYPE = 'standard' AUTO_SUSPEND = 300 AUTO_RESUME = TRUE INITIALLY_SUSPENDED = TRUE COMMENT = 'warehouse for PMC Chatbot';

use warehouse pmc_wh;

create or replace stage pmc_oa_comm_raw url = 's3://pmc-oa-opendata/oa_comm';


--https://www.ncbi.nlm.nih.gov/pmc/tools/pmcaws/

--ls @pmc_oa_comm_raw PATTERN='/txt/all/PMC1249490.txt';

create 
or replace file format my_csv_format type = csv skip_header=1;

-- select 
--   * 
-- FROM 
--   TABLE(
--     INFER_SCHEMA(
--       LOCATION => '@pmc_oa_comm_raw/txt/metadata/csv/oa_comm.filelist.csv', 
--       FILE_FORMAT => 'my_csv_format'
--     )
--   );

--Key,ETag,Article Citation,AccessionID,Last Updated UTC (YYYY-MM-DD HH:MM:SS),PMID,License,Retracted

CREATE 
or replace TABLE oa_comm_metadata (
Key text,
etag text,
article_citation text,
accessionid text,
last_updated_utc  text ,
pmid text, 
license text,
retracted text
);

copy into oa_comm_metadata FROM @pmc_oa_comm_raw/txt/metadata/csv/oa_comm.filelist.csv 
file_format= 'my_csv_format'
ON_ERROR= 'CONTINUE';

select * from oa_comm_metadata order by accessionid asc limit 20;

--ls @pmc_oa_comm_raw/txt/all/PMC10000000.txt;


--Extract text chunks from articles
create or replace function txt_text_chunker(file_url string)
returns table (chunk varchar)
language python
runtime_version = '3.9'
handler = 'txt_text_chunker'
packages = ('snowflake-snowpark-python','langchain')
as
$$
from snowflake.snowpark.types import StringType, StructField, StructType
from langchain.text_splitter import RecursiveCharacterTextSplitter
from snowflake.snowpark.files import SnowflakeFile
import io
import logging
import pandas as pd

class txt_text_chunker:

    def read_txt(self, file_url: str) -> str:
    
        logger = logging.getLogger("udf_logger")
        logger.info(f"Opening file {file_url}")
    
        with SnowflakeFile.open(file_url, 'rb') as f:
            buffer = io.BytesIO(f.readall())

        try:
            text = buffer.getvalue().decode('utf-8').replace('\n', ' ').replace('\0', ' ')

        except Exception as e:
            logger.warn(f"Unable to extract from file {file_url}: {str(e)}")
            text = "Unable to Extract"
        
        return text

    def process(self, file_url: str):

        text = self.read_txt(file_url)
        
        text_splitter = RecursiveCharacterTextSplitter(
            chunk_size = 4000, #Adjust this as you see fit
            chunk_overlap  = 400, #This let's text have some form of overlap. Useful for keeping chunks contextual
            length_function = len
        )
    
        chunks = text_splitter.split_text(text)
        df = pd.DataFrame(chunks, columns=['chunk'])
        
        yield from df.itertuples(index=False, name=None)
$$;



alter warehouse pmc_wh set warehouse_size= 'X-Large';


--Table to load text from pmc articles in AWS external stage 
create or replace TABLE PMC_OA_CHUNKS_TABLE ( 
    RELATIVE_PATH VARCHAR(16777216), -- Relative path to the file
    ABS_PATH  VARCHAR(16777216), -- Path for the file
    Etag VARCHAR(16777216),
    SCOPED_FILE_URL VARCHAR(16777216), -- Scoped url (you can choose which one to keep depending on your use case)
    CHUNK VARCHAR(16777216)-- Piece of text
);


--Loads Articles that have not been processed yet into PMC_OA_CHUNKS_TABLE
--Batch processing 100 articles by using "LIMIT 100", stay under 1000 articles given cortex search prpr performance
insert into PMC_OA_chunks_table (relative_path, abs_path,etag,scoped_file_url, chunk)
with oa_comm as (select array_to_string(array_slice(split(key, '/'),1,4), '/') as relative_path , etag from oa_comm_metadata where etag not in (select etag from PMC_OA_chunks_table ) limit 100)
select relative_path, 
            get_absolute_path(@pmc_oa_comm_raw, relative_path) as abs_path,
            etag ,
            build_scoped_file_url(@pmc_oa_comm_raw, relative_path) as scoped_file_url,
            func.chunk as chunk
    from 
        oa_comm,
        TABLE(txt_text_chunker(build_scoped_file_url(@pmc_oa_comm_raw, relative_path))) as func;


--Check to see if example article is in the service
select distinct accessionid from pmc_data.pmc_oa_opendata.pmc_service_vw  where accessionid='PMC9388525';

-- If the result above  is null, uncomment below to manually add the article with the below insert :

-- insert into PMC_OA_chunks_table (relative_path, abs_path,etag,scoped_file_url, chunk)
-- with oa_comm as (select array_to_string(array_slice(split(key, '/'),1,4), '/') as relative_path , etag from oa_comm_metadata where etag not in (select etag from PMC_OA_chunks_table ) and accessionid='PMC9388525')
-- select relative_path, 
--             get_absolute_path(@pmc_oa_comm_raw, relative_path) as abs_path,
--             etag ,
--             build_scoped_file_url(@pmc_oa_comm_raw, relative_path) as scoped_file_url,
--             func.chunk as chunk
--     from 
--         oa_comm,
--         TABLE(txt_text_chunker(build_scoped_file_url(@pmc_oa_comm_raw, relative_path))) as func;




--View for Search service definition
create or replace view pmc_service_vw as (select pmc_oa_chunks_table.etag, article_citation,accessionid, last_updated_utc, pmid, retracted, license, chunk  from pmc_data.pmc_oa_opendata.PMC_OA_chunks_table left join  pmc_data.pmc_oa_opendata.oa_comm_metadata on pmc_oa_chunks_table.etag=oa_comm_metadata.etag);


-- Create search service
Create or replace cortex search service my_pmc_search_service on chunk 
attributes etag, article_citation, accessionid, last_updated_utc, pmid, retracted, license 
warehouse= pmc_wh
target_lag= '1 min'
as (
select etag, article_citation,accessionid, last_updated_utc, pmid, retracted, license, chunk from pmc_data.pmc_oa_opendata.pmc_service_vw
);

--ALTER CORTEX SEARCH SERVICE my_pmc_search_service SET warehouse = pmc_wh;
Describe Cortex Search service my_pmc_search_service;

