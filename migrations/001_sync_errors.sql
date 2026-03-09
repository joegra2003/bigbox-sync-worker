CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE TABLE IF NOT EXISTS crm.sync_errors (
  id uuid DEFAULT uuid_generate_v4() PRIMARY KEY,
  raw_event jsonb NOT NULL,
  error_message text,
  table_name varchar,
  retried boolean DEFAULT false,
  created_at timestamptz DEFAULT now()
);
