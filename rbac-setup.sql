-- Grant all privileges on catalog to data engineers
GRANT ALL PRIVILEGES ON CATALOG assignment TO `data-engineers-group`;

-- Grant usage on catalog and curated schema to analysts
GRANT USAGE ON CATALOG assignment TO `data-analysts-group`;
GRANT USAGE ON SCHEMA assignment.curated TO `data-analysts-group`;

-- Grant SELECT on all existing views in curated schema
GRANT SELECT ON ALL VIEWS IN SCHEMA assignment.curated TO `data-analysts-group`;

