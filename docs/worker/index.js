/**
 * FCI FieldMap v4 — Cloudflare Worker API
 *
 * Auth routes (no X-FCI-Key required):
 *   POST /api/auth/login              email + password → session token
 *   POST /api/auth/logout             invalidate session token
 *   POST /api/auth/register           create user (admin only OR first admin bootstrap)
 *   GET  /api/auth/me                 get current user info from session token
 *
 * All other routes require either:
 *   X-FCI-Key header (legacy API key — kept for backwards compat)
 *   OR X-FCI-Session header (session token from login)
 *
 * Admin-only routes additionally require role === 'admin'
 */

const CORS = {
  'Access-Control-Allow-Origin':  '*',
  'Access-Control-Allow-Methods': 'GET,POST,DELETE,OPTIONS',
  'Access-Control-Allow-Headers': 'Content-Type,X-FCI-Key,X-FCI-Session',
  'Access-Control-Max-Age':       '86400',
};

const SESSION_TTL_HOURS = 72; // sessions last 3 days

function json(data, status = 200) {
  return new Response(JSON.stringify(data), {
    status,
    headers: { ...CORS, 'Content-Type': 'application/json' },
  });
}

function err(msg, status = 400) {
  return new Response(JSON.stringify({ error: msg }), {
    status,
    headers: { ...CORS, 'Content-Type': 'application/json' },
  });
}

// ── Crypto helpers ─────────────────────────────────────────────────────────
async function sha256(str) {
  const buf = await crypto.subtle.digest('SHA-256', new TextEncoder().encode(str));
  return Array.from(new Uint8Array(buf)).map(b => b.toString(16).padStart(2, '0')).join('');
}

function randomHex(bytes = 16) {
  const arr = new Uint8Array(bytes);
  crypto.getRandomValues(arr);
  return Array.from(arr).map(b => b.toString(16).padStart(2, '0')).join('');
}

function uuid() {
  return randomHex(4) + '-' + randomHex(2) + '-' + randomHex(2) + '-' + randomHex(2) + '-' + randomHex(6);
}

async function hashPassword(password, salt) {
  return sha256(`fci_auth:${salt}:${password}`);
}

// ── Session helpers ────────────────────────────────────────────────────────
async function createSession(userId, role, campaignId, env) {
  const token     = randomHex(32);
  const now       = new Date();
  const expiresAt = new Date(now.getTime() + SESSION_TTL_HOURS * 3600 * 1000).toISOString();
  await env.DB.prepare(`
    INSERT INTO sessions (token, user_id, role, campaign_id, created_at, expires_at)
    VALUES (?, ?, ?, ?, ?, ?)
  `).bind(token, userId, role, campaignId || null, now.toISOString(), expiresAt).run();
  return { token, expiresAt, role, campaignId };
}

async function validateSession(request, env) {
  const token = request.headers.get('X-FCI-Session');
  if (!token) return null;
  const session = await env.DB.prepare(
    `SELECT * FROM sessions WHERE token = ? AND expires_at > ?`
  ).bind(token, new Date().toISOString()).first();
  if (!session) return null;
  return session;
}

async function authCheck(request, env) {
  // Accept either legacy API key OR session token
  const key = request.headers.get('X-FCI-Key');
  if (key && key === env.FCI_API_KEY) {
    return { ok: true, role: 'admin', legacy: true };
  }
  const session = await validateSession(request, env);
  if (session) return { ok: true, ...session };
  return null;
}

// ── Auth routes ────────────────────────────────────────────────────────────
async function authLogin(request, env) {
  const { email, password } = await request.json();
  if (!email || !password) return err('Email and password required');

  const user = await env.DB.prepare(
    'SELECT * FROM users WHERE email = ?'
  ).bind(email.toLowerCase().trim()).first();

  if (!user) return err('Invalid email or password', 401);

  const hash = await hashPassword(password, user.salt);
  if (hash !== user.password_hash) return err('Invalid email or password', 401);

  // Update last login
  await env.DB.prepare('UPDATE users SET last_login = ? WHERE id = ?')
    .bind(new Date().toISOString(), user.id).run();

  const session = await createSession(user.id, user.role, user.campaign_id, env);

  return json({
    ok: true,
    token: session.token,
    expiresAt: session.expiresAt,
    user: {
      id:         user.id,
      email:      user.email,
      name:       user.name,
      role:       user.role,
      campaignId: user.campaign_id,
    },
  });
}

async function authLogout(request, env) {
  const token = request.headers.get('X-FCI-Session');
  if (token) {
    await env.DB.prepare('DELETE FROM sessions WHERE token = ?').bind(token).run();
  }
  return json({ ok: true });
}

async function authMe(request, env) {
  const session = await validateSession(request, env);
  if (!session) return err('Not authenticated', 401);

  const user = await env.DB.prepare('SELECT * FROM users WHERE id = ?')
    .bind(session.user_id).first();
  if (!user) return err('User not found', 404);

  return json({
    user: {
      id:         user.id,
      email:      user.email,
      name:       user.name,
      role:       user.role,
      campaignId: user.campaign_id,
    },
    session: {
      expiresAt:  session.expires_at,
      campaignId: session.campaign_id,
    },
  });
}

async function authRegister(request, env, callerSession) {
  const body = await request.json();
  const { email, password, name, role, campaignId } = body;

  if (!email || !password) return err('Email and password required');
  if (!['admin', 'candidate', 'volunteer'].includes(role)) return err('Invalid role');
  if (password.length < 6) return err('Password must be at least 6 characters');

  // Check if ANY admin exists — if not, allow first admin bootstrap
  const adminCount = await env.DB.prepare(
    "SELECT COUNT(*) as n FROM users WHERE role = 'admin'"
  ).first();

  const isBootstrap = adminCount.n === 0;

  if (!isBootstrap) {
    // Must be logged in as admin to create users
    if (!callerSession || callerSession.role !== 'admin') {
      return err('Admin access required', 403);
    }
    // Only admins can create admins
    if (role === 'admin' && callerSession.role !== 'admin') {
      return err('Only admins can create admin accounts', 403);
    }
  }

  // Check email not already taken
  const existing = await env.DB.prepare('SELECT id FROM users WHERE email = ?')
    .bind(email.toLowerCase().trim()).first();
  if (existing) return err('An account with that email already exists');

  const id   = uuid();
  const salt = randomHex(16);
  const hash = await hashPassword(password, salt);

  await env.DB.prepare(`
    INSERT INTO users (id, email, password_hash, salt, role, campaign_id, name, created_at)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
  `).bind(
    id,
    email.toLowerCase().trim(),
    hash,
    salt,
    role,
    campaignId || null,
    name || '',
    new Date().toISOString()
  ).run();

  return json({ ok: true, id, email, role });
}

async function listUsers(env, callerSession) {
  if (!callerSession || callerSession.role !== 'admin') {
    return err('Admin access required', 403);
  }
  const result = await env.DB.prepare(
    'SELECT id, email, name, role, campaign_id, created_at, last_login FROM users ORDER BY created_at DESC'
  ).all();
  return json({ users: result.results });
}

async function deleteUser(userId, env, callerSession) {
  if (!callerSession || callerSession.role !== 'admin') {
    return err('Admin access required', 403);
  }
  await env.DB.prepare('DELETE FROM users WHERE id = ?').bind(userId).run();
  await env.DB.prepare('DELETE FROM sessions WHERE user_id = ?').bind(userId).run();
  return json({ ok: true });
}

async function updateUser(userId, request, env, callerSession) {
  if (!callerSession || callerSession.role !== 'admin') {
    return err('Admin access required', 403);
  }
  const body = await request.json();
  const updates = [];
  const params  = [];

  if (body.name !== undefined)       { updates.push('name = ?');        params.push(body.name); }
  if (body.role !== undefined)       { updates.push('role = ?');        params.push(body.role); }
  if (body.campaignId !== undefined) { updates.push('campaign_id = ?'); params.push(body.campaignId || null); }
  if (body.password)  {
    const user = await env.DB.prepare('SELECT salt FROM users WHERE id = ?').bind(userId).first();
    if (!user) return err('User not found', 404);
    const hash = await hashPassword(body.password, user.salt);
    updates.push('password_hash = ?');
    params.push(hash);
  }

  if (!updates.length) return err('Nothing to update');
  params.push(userId);

  await env.DB.prepare(`UPDATE users SET ${updates.join(', ')} WHERE id = ?`)
    .bind(...params).run();

  return json({ ok: true });
}

// ── Voter fetch ────────────────────────────────────────────────────────────
async function getVoters(request, env) {
  const url    = new URL(request.url);
  const p      = url.searchParams;
  const where  = [];
  const params = [];

  if (p.get('st_house'))     { where.push("st_house = ?");     params.push(p.get('st_house')); }
  if (p.get('st_senate'))    { where.push("st_senate = ?");    params.push(p.get('st_senate')); }
  if (p.get('cong_dist'))    { where.push("cong_dist LIKE ?");    params.push('%' + p.get('cong_dist').replace(/[^0-9]/g,'') + '%'); }
  if (p.get('county_num'))   { where.push('county_num = ?');      params.push(p.get('county_num')); }
  if (p.get('municipality')) { where.push("municipality LIKE ?"); params.push('%' + p.get('municipality').replace(/ CITY$/i,'').trim() + '%'); }
  if (p.get('township'))     { where.push("township LIKE ?");     params.push('%' + p.get('township').replace(/ TWP$/i,'').trim() + '%'); }
  if (p.get('village'))      { where.push("village LIKE ?");      params.push('%' + p.get('village').replace(/ VLG$/i,'').trim() + '%'); }
  if (p.get('precinct'))     { where.push('precinct = ?');        params.push(p.get('precinct')); }
  if (p.get('ward'))         { where.push("ward LIKE ?");         params.push('%' + p.get('ward') + '%'); }
  if (p.get('score'))        { where.push('score = ?');           params.push(p.get('score')); }
  if (p.get('last_name'))    { where.push("data LIKE ?"); params.push('%' + p.get('last_name').toUpperCase() + '%'); }
  if (p.get('first_name'))   { where.push("data LIKE ?"); params.push('%\"' + p.get('first_name').toUpperCase() + '%'); }

  where.push("party != 'R'");
  const whereSQL = 'WHERE ' + where.join(' AND ');
  const limit    = Math.min(parseInt(p.get('limit') || '60000'), 60000);
  const offset   = parseInt(p.get('offset') || '0');
  // Always exclude registered Republicans
  where.push("party != 'R'");
  const whereSQL2 = 'WHERE ' + where.join(' AND ');
  const sql      = `SELECT data FROM voters ${whereSQL2} LIMIT ? OFFSET ?`;
  params.push(limit, offset);

  const result = await env.DB.prepare(sql).bind(...params).all();
  const voters = result.results.map(r => JSON.parse(r.data));
  return json({ voters, count: voters.length, offset });
}

async function countVoters(request, env) {
  const url    = new URL(request.url);
  const p      = url.searchParams;
  const where  = [];
  const params = [];

  if (p.get('st_house'))     { where.push('st_house = ?');     params.push(p.get('st_house')); }
  if (p.get('st_senate'))    { where.push('st_senate = ?');    params.push(p.get('st_senate')); }
  if (p.get('cong_dist'))    { where.push("cong_dist LIKE ?"); params.push('%' + p.get('cong_dist').replace(/[^0-9]/g,'') + '%'); }
  if (p.get('county_num'))   { where.push('county_num = ?');    params.push(p.get('county_num')); }
  if (p.get('municipality')) { where.push('municipality = ?'); params.push(p.get('municipality')); }
  if (p.get('township'))     { where.push('township = ?');     params.push(p.get('township')); }
  if (p.get('village'))      { where.push('village = ?');      params.push(p.get('village')); }
  if (p.get('precinct'))     { where.push('precinct = ?');     params.push(p.get('precinct')); }
  if (p.get('score'))        { where.push('score = ?');        params.push(p.get('score')); }

  const whereSQL = where.length ? 'WHERE ' + where.join(' AND ') : '';
  const result   = await env.DB.prepare(`SELECT COUNT(*) as n FROM voters ${whereSQL}`)
                              .bind(...params).first();
  return json({ count: result.n });
}

// ── Contacts ───────────────────────────────────────────────────────────────
async function getContacts(cid, request, env) {
  const url   = new URL(request.url);
  const since = url.searchParams.get('since');
  let sql     = 'SELECT * FROM contacts WHERE campaign_id = ?';
  const args  = [cid];
  if (since) { sql += ' AND updated_at > ?'; args.push(since); }
  const result = await env.DB.prepare(sql).bind(...args).all();
  return json({ contacts: result.results, ts: new Date().toISOString() });
}

async function upsertContacts(cid, request, env) {
  const body    = await request.json();
  const updates = Array.isArray(body) ? body : [body];
  if (!updates.length) return json({ ok: true, count: 0 });

  const now  = new Date().toISOString();
  const stmt = env.DB.prepare(`
    INSERT INTO contacts
      (campaign_id, voter_id, contact_status, contact_reason,
       yard_sign, opp_yard, notes, spoke_with, new_ally, restricted,
       score_override, score, updated_at, updated_by)
    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    ON CONFLICT(campaign_id, voter_id) DO UPDATE SET
      contact_status = excluded.contact_status,
      contact_reason = excluded.contact_reason,
      yard_sign      = excluded.yard_sign,
      opp_yard       = excluded.opp_yard,
      notes          = excluded.notes,
      spoke_with     = excluded.spoke_with,
      new_ally       = excluded.new_ally,
      restricted     = excluded.restricted,
      score_override = excluded.score_override,
      score          = excluded.score,
      updated_at     = excluded.updated_at,
      updated_by     = excluded.updated_by
  `);

  const BATCH = 100;
  let count = 0;
  for (let i = 0; i < updates.length; i += BATCH) {
    const chunk = updates.slice(i, i + BATCH);
    const batch = chunk.map(u => stmt.bind(
      cid, u.id,
      u.contactStatus  || 'pending',
      u.contactReason  || '',
      u.yardSign  ? 1 : 0,
      u.oppYard   ? 1 : 0,
      u.notes     || '',
      JSON.stringify(u.spokeWith || []),
      u.newAlly   ? 1 : 0,
      u.restricted? 1 : 0,
      u.scoreOverride ? 1 : 0,
      u.score     || '',
      now,
      u.updatedBy || '',
    ));
    await env.DB.batch(batch);
    count += chunk.length;
  }
  return json({ ok: true, count, ts: now });
}

// ── Campaigns ─────────────────────────────────────────────────────────────
async function getCampaigns(env) {
  const result = await env.DB.prepare('SELECT data FROM campaigns ORDER BY rowid DESC').all();
  const campaigns = {};
  result.results.forEach(r => {
    const c = JSON.parse(r.data);
    campaigns[c.id] = c;
  });
  return json({ campaigns });
}

async function saveCampaign(request, env) {
  const cam = await request.json();
  if (!cam.id) return err('Campaign must have an id');
  const now = new Date().toISOString();
  await env.DB.prepare(`
    INSERT INTO campaigns (id, data, created_at, updated_at)
    VALUES (?, ?, ?, ?)
    ON CONFLICT(id) DO UPDATE SET data = excluded.data, updated_at = excluded.updated_at
  `).bind(cam.id, JSON.stringify(cam), now, now).run();
  return json({ ok: true, id: cam.id });
}

async function deleteCampaign(id, env) {
  await env.DB.prepare('DELETE FROM campaigns WHERE id = ?').bind(id).run();
  await env.DB.prepare('DELETE FROM contacts WHERE campaign_id = ?').bind(id).run();
  return json({ ok: true });
}

// ── Admin: bulk voter load ─────────────────────────────────────────────────
async function loadVoters(request, env) {
  if (request.headers.get('X-FCI-Admin') !== env.FCI_ADMIN_KEY) {
    return err('Admin key required for voter load', 403);
  }
  const body   = await request.json();
  const voters = body.voters;
  if (!Array.isArray(voters) || !voters.length) return err('voters array required');
  // Ensure county_num column exists (safe to run repeatedly)
  try { await env.DB.prepare("ALTER TABLE voters ADD COLUMN county_num TEXT DEFAULT '23'").run(); } catch(e) {}
  if (body.replace) await env.DB.prepare('DELETE FROM voters').run();

  const stmt = env.DB.prepare(`
    INSERT OR REPLACE INTO voters
      (id, data, lat, lon, municipality, township, village,
       precinct, precinct_name, st_house, st_senate, cong_dist, ward, score, party, county_num)
    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
  `);

  const BATCH = 100;
  let count = 0;
  for (let i = 0; i < voters.length; i += BATCH) {
    const chunk = voters.slice(i, i + BATCH);
    const batch = chunk.map(v => stmt.bind(
      v.id, JSON.stringify(v),
      v.lat || null, v.lon || null,
      (v.municipality || '').toUpperCase(),
      (v.township     || '').toUpperCase(),
      (v.village      || '').toUpperCase(),
      (v.precinct     || '').toUpperCase(),
      (v.precinctName || '').toUpperCase(),
      (v.stHouse      || '').toUpperCase(),
      (v.stSenate     || '').toUpperCase(),
      (v.congDist     || '').toUpperCase(),
      (v.ward         || '').toUpperCase(),
      v.score  || '',
      v.party  || '',
      v.countyNum || '23',
    ));
    await env.DB.batch(batch);
    count += chunk.length;
  }
  return json({ ok: true, loaded: count });
}

// ── Admin district fix ───────────────────────────────────────────────────────
async function fixDistrict(request, env) {
  const body = await request.json();
  const { id, stHouse, stSenate, congDist, countyNum } = body;
  if (!id) return err('id required');
  const existing = await env.DB.prepare('SELECT data FROM voters WHERE id = ?').bind(id).first();
  if (!existing) return err('Voter not found', 404);
  const record = JSON.parse(existing.data);
  if (stHouse   !== undefined) record.stHouse   = stHouse;
  if (stSenate  !== undefined) record.stSenate  = stSenate;
  if (congDist  !== undefined) record.congDist  = congDist;
  if (countyNum !== undefined) record.countyNum = countyNum;
  await env.DB.prepare(
    'UPDATE voters SET data=?, st_house=?, st_senate=?, cong_dist=?, county_num=? WHERE id=?'
  ).bind(JSON.stringify(record), stHouse||'', stSenate||'', congDist||'', countyNum||'', id).run();
  return json({ ok: true });
}

// ── Admin voter search (no party filter) ────────────────────────────────────
async function searchVotersAdmin(request, env) {
  const p = new URL(request.url).searchParams;
  const where = [];
  const params = [];
  if (p.get('last_name')) { where.push("data LIKE ?"); params.push('%' + p.get('last_name').toUpperCase() + '%'); }
  const whereSQL = where.length ? 'WHERE ' + where.join(' AND ') : '';
  const limit = Math.min(parseInt(p.get('limit') || '20'), 100);
  params.push(limit);
  const result = await env.DB.prepare(`SELECT data FROM voters ${whereSQL} LIMIT ?`).bind(...params).all();
  const voters = result.results.map(r => JSON.parse(r.data));
  return json({ voters, count: voters.length });
}

// ── Partial voter update (donations, party, score) ──────────────────────────
async function updateVoters(request, env) {
  const body = await request.json();
  const voters = body.voters;
  if (!Array.isArray(voters) || !voters.length) return err('voters array required');

  let updated = 0;
  for (const v of voters) {
    if (!v.id) continue;
    // Fetch existing record
    const existing = await env.DB.prepare('SELECT data FROM voters WHERE id = ?').bind(v.id).first();
    if (!existing) continue;
    const record = JSON.parse(existing.data);
    // Apply updates
    if (v.donations !== undefined) record.donations = v.donations;
    if (v.party     !== undefined) record.party     = v.party;
    if (v.score     !== undefined) record.score     = v.score;
    // Update party column in DB too
    const newParty = v.party !== undefined ? v.party : record.party || '';
    await env.DB.prepare('UPDATE voters SET data = ?, party = ? WHERE id = ?')
      .bind(JSON.stringify(record), newParty.toUpperCase(), v.id).run();
    updated++;
  }
  return json({ ok: true, updated });
}

// ── Router ────────────────────────────────────────────────────────────────
export default {
  async fetch(request, env, ctx) {
    if (request.method === 'OPTIONS') {
      return new Response(null, { status: 204, headers: CORS });
    }

    const url      = new URL(request.url);
    const pathname = url.pathname;
    const method   = request.method;

    // ── Public routes (no auth) ──────────────────────────────────────────
    if (pathname === '/api/ping') {
      return json({ ok: true, ts: new Date().toISOString() });
    }
    if (pathname === '/api/auth/login'    && method === 'POST') return authLogin(request, env);
    if (pathname === '/api/auth/logout'   && method === 'POST') return authLogout(request, env);
    if (pathname === '/api/auth/me'       && method === 'GET')  return authMe(request, env);

    // ── Auth check for all other routes ─────────────────────────────────
    const session = await authCheck(request, env);
    if (!session) return err('Unauthorized', 401);

    // ── Auth management (admin only) ─────────────────────────────────────
    if (pathname === '/api/auth/register' && method === 'POST') return authRegister(request, env, session);
    if (pathname === '/api/auth/users'    && method === 'GET')  return listUsers(env, session);

    const userMatch = pathname.match(/^\/api\/auth\/users\/(.+)$/);
    if (userMatch) {
      const uid = decodeURIComponent(userMatch[1]);
      if (method === 'DELETE') return deleteUser(uid, env, session);
      if (method === 'PATCH')  return updateUser(uid, request, env, session);
    }

    // ── Data routes ──────────────────────────────────────────────────────
    if (pathname === '/api/voters'       && method === 'GET') return getVoters(request, env);
    if (pathname === '/api/voters/count' && method === 'GET') return countVoters(request, env);

    const contactMatch = pathname.match(/^\/api\/contacts\/(.+)$/);
    if (contactMatch) {
      const cid = decodeURIComponent(contactMatch[1]);
      if (method === 'GET')  return getContacts(cid, request, env);
      if (method === 'POST') return upsertContacts(cid, request, env);
    }

    if (pathname === '/api/campaigns' && method === 'GET')  return getCampaigns(env);
    if (pathname === '/api/campaigns' && method === 'POST') return saveCampaign(request, env);
    const camMatch = pathname.match(/^\/api\/campaigns\/(.+)$/);
    if (camMatch && method === 'DELETE') return deleteCampaign(decodeURIComponent(camMatch[1]), env);

    if (pathname === '/api/admin/load-voters'   && method === 'POST') return loadVoters(request, env);
    if (pathname === '/api/admin/update-voters'  && method === 'POST') return updateVoters(request, env);
    if (pathname === '/api/admin/search-voters'  && method === 'GET')  return searchVotersAdmin(request, env);
    if (pathname === '/api/admin/fix-district'   && method === 'POST') return fixDistrict(request, env);

    return err('Not found', 404);
  },
};
