-- FCI FieldMap v4 — Auth Migration
-- Run: wrangler d1 execute fci-canvass-db --remote --file=auth_migration.sql

CREATE TABLE IF NOT EXISTS users (
  id           TEXT PRIMARY KEY,          -- uuid
  email        TEXT NOT NULL UNIQUE,
  password_hash TEXT NOT NULL,            -- SHA-256(salt:password)
  salt         TEXT NOT NULL,
  role         TEXT NOT NULL DEFAULT 'volunteer', -- admin | candidate | volunteer
  campaign_id  TEXT,                      -- for candidate/volunteer: which campaign
  name         TEXT DEFAULT '',
  created_at   TEXT DEFAULT (datetime('now')),
  last_login   TEXT
);

CREATE INDEX IF NOT EXISTS idx_users_email      ON users(email);
CREATE INDEX IF NOT EXISTS idx_users_campaign   ON users(campaign_id);
CREATE INDEX IF NOT EXISTS idx_users_role       ON users(role);

CREATE TABLE IF NOT EXISTS sessions (
  token        TEXT PRIMARY KEY,          -- 32-char random hex
  user_id      TEXT NOT NULL,
  role         TEXT NOT NULL,
  campaign_id  TEXT,
  created_at   TEXT DEFAULT (datetime('now')),
  expires_at   TEXT NOT NULL,             -- ISO datetime
  FOREIGN KEY (user_id) REFERENCES users(id)
);

CREATE INDEX IF NOT EXISTS idx_sessions_user    ON sessions(user_id);
CREATE INDEX IF NOT EXISTS idx_sessions_expires ON sessions(expires_at);
