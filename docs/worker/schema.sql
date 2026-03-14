-- FCI FieldMap v4 — D1 Schema
-- Run: wrangler d1 execute fci-canvass-db --file=schema.sql

CREATE TABLE IF NOT EXISTS voters (
  id          TEXT PRIMARY KEY,
  data        TEXT NOT NULL,        -- full JSON voter object
  lat         REAL,
  lon         REAL,
  municipality TEXT,
  township    TEXT,
  village     TEXT,
  precinct    TEXT,
  precinct_name TEXT,
  st_house    TEXT,
  st_senate   TEXT,
  cong_dist   TEXT,
  ward        TEXT,
  score       TEXT,
  party       TEXT
);

CREATE INDEX IF NOT EXISTS idx_voters_st_house    ON voters(st_house);
CREATE INDEX IF NOT EXISTS idx_voters_st_senate   ON voters(st_senate);
CREATE INDEX IF NOT EXISTS idx_voters_cong_dist   ON voters(cong_dist);
CREATE INDEX IF NOT EXISTS idx_voters_municipality ON voters(municipality);
CREATE INDEX IF NOT EXISTS idx_voters_township    ON voters(township);
CREATE INDEX IF NOT EXISTS idx_voters_village     ON voters(village);
CREATE INDEX IF NOT EXISTS idx_voters_precinct    ON voters(precinct);
CREATE INDEX IF NOT EXISTS idx_voters_ward        ON voters(ward);
CREATE INDEX IF NOT EXISTS idx_voters_score       ON voters(score);

CREATE TABLE IF NOT EXISTS contacts (
  campaign_id    TEXT NOT NULL,
  voter_id       TEXT NOT NULL,
  contact_status TEXT DEFAULT 'pending',
  contact_reason TEXT DEFAULT '',
  yard_sign      INTEGER DEFAULT 0,
  opp_yard       INTEGER DEFAULT 0,
  notes          TEXT DEFAULT '',
  spoke_with     TEXT DEFAULT '[]',  -- JSON array
  new_ally       INTEGER DEFAULT 0,
  restricted     INTEGER DEFAULT 0,
  score_override INTEGER DEFAULT 0,
  score          TEXT DEFAULT '',
  updated_at     TEXT DEFAULT (datetime('now')),
  updated_by     TEXT DEFAULT '',    -- volunteer identifier
  PRIMARY KEY (campaign_id, voter_id)
);

CREATE INDEX IF NOT EXISTS idx_contacts_campaign ON contacts(campaign_id);
CREATE INDEX IF NOT EXISTS idx_contacts_updated  ON contacts(campaign_id, updated_at);

CREATE TABLE IF NOT EXISTS campaigns (
  id         TEXT PRIMARY KEY,
  data       TEXT NOT NULL,          -- full JSON campaign object
  created_at TEXT DEFAULT (datetime('now')),
  updated_at TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS sync_log (
  id         INTEGER PRIMARY KEY AUTOINCREMENT,
  campaign_id TEXT,
  voter_id    TEXT,
  action      TEXT,
  ts          TEXT DEFAULT (datetime('now'))
);
