-- Add name search columns if they don't exist
ALTER TABLE voters ADD COLUMN last_name TEXT DEFAULT '';
ALTER TABLE voters ADD COLUMN first_name TEXT DEFAULT '';
CREATE INDEX IF NOT EXISTS idx_voters_last_name ON voters(last_name);
CREATE INDEX IF NOT EXISTS idx_voters_first_name ON voters(first_name);
