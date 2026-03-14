/**
 * FCI FieldMap v4 — Cloudflare Worker API
 * Routes:
 *   GET  /api/voters              fetch voters (filtered)
 *   GET  /api/voters/count        count voters matching filters
 *   GET  /api/contacts/:cid       get all contacts for a campaign
 *   POST /api/contacts/:cid       upsert one or many contacts
 *   GET  /api/campaigns           list all campaigns
 *   POST /api/campaigns           create or update a campaign
 *   DELETE /api/campaigns/:id     delete a campaign
 *   POST /api/admin/load-voters   bulk-load voter data (admin key required)
 *   GET  /api/ping                health check
 *
 * Auth: every request must include header  X-FCI-Key: <secret>
 *       Set the secret with:  wrangler secret put FCI_API_KEY
 */

const CORS = {
  'Access-Control-Allow-Origin':  '*',
  'Access-Control-Allow-Methods': 'GET,POST,DELETE,OPTIONS',
  'Access-Control-Allow-Headers': 'Content-Type,X-FCI-Key',
  'Access-Control-Max-Age':       '86400',
};

function json(data, status = 200) {
  return new Response(JSON.stringify(data), {
    status,
    headers: { ...CORS, 'Content-Type': 'application/json' },
  });
}

function err(msg, status = 400) {
  return json({ error: msg }, status);
}

function auth(request, env) {
  const key = request.headers.get('X-FCI-Key');
  if (!key || key !== env.FCI_API_KEY) return false;
  return true;
}

// ── Voter fetch ────────────────────────────────────────────────────────────
async function getVoters(request, env) {
  const url    = new URL(request.url);
  const p      = url.searchParams;

  // Build WHERE clauses
  const where  = [];
  const params = [];

  if (p.get('st_house'))     { where.push('st_house = ?');     params.push(p.get('st_house')); }
  if (p.get('st_senate'))    { where.push('st_senate = ?');    params.push(p.get('st_senate')); }
  if (p.get('cong_dist'))    { where.push('cong_dist = ?');    params.push(p.get('cong_dist')); }
  if (p.get('municipality')) { where.push('municipality = ?'); params.push(p.get('municipality')); }
  if (p.get('township'))     { where.push('township = ?');     params.push(p.get('township')); }
  if (p.get('village'))      { where.push('village = ?');      params.push(p.get('village')); }
  if (p.get('precinct'))     { where.push('precinct = ?');     params.push(p.get('precinct')); }
  if (p.get('ward'))         { where.push('ward = ?');         params.push(p.get('ward')); }
  if (p.get('score'))        { where.push('score = ?');        params.push(p.get('score')); }

  const whereSQL = where.length ? 'WHERE ' + where.join(' AND ') : '';
  const limit    = Math.min(parseInt(p.get('limit') || '60000'), 60000);
  const offset   = parseInt(p.get('offset') || '0');

  const sql = `SELECT data FROM voters ${whereSQL} LIMIT ? OFFSET ?`;
  params.push(limit, offset);

  const result = await env.DB.prepare(sql).bind(...params).all();
  const voters = result.results.map(r => JSON.parse(r.data));

  return json({ voters, count: voters.length, offset });
}

async function countVoters(request, env) {
  const url   = new URL(request.url);
  const p     = url.searchParams;
  const where = [];
  const params = [];

  if (p.get('st_house'))     { where.push('st_house = ?');     params.push(p.get('st_house')); }
  if (p.get('st_senate'))    { where.push('st_senate = ?');    params.push(p.get('st_senate')); }
  if (p.get('cong_dist'))    { where.push('cong_dist = ?');    params.push(p.get('cong_dist')); }
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
  const url    = new URL(request.url);
  const since  = url.searchParams.get('since'); // ISO datetime for delta sync

  let sql    = 'SELECT * FROM contacts WHERE campaign_id = ?';
  const args = [cid];
  if (since) { sql += ' AND updated_at > ?'; args.push(since); }

  const result = await env.DB.prepare(sql).bind(...args).all();
  return json({ contacts: result.results, ts: new Date().toISOString() });
}

async function upsertContacts(cid, request, env) {
  const body = await request.json();
  // Accept single object or array
  const updates = Array.isArray(body) ? body : [body];
  if (!updates.length) return json({ ok: true, count: 0 });

  const now = new Date().toISOString();
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

  // Batch in groups of 100
  const BATCH = 100;
  let count = 0;
  for (let i = 0; i < updates.length; i += BATCH) {
    const chunk = updates.slice(i, i + BATCH);
    const batch = chunk.map(u => stmt.bind(
      cid,
      u.id,
      u.contactStatus  || 'pending',
      u.contactReason  || '',
      u.yardSign  ? 1 : 0,
      u.oppYard   ? 1 : 0,
      u.notes          || '',
      JSON.stringify(u.spokeWith || []),
      u.newAlly   ? 1 : 0,
      u.restricted? 1 : 0,
      u.scoreOverride ? 1 : 0,
      u.score          || '',
      now,
      u.updatedBy      || '',
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
  // Extra check: must also include X-FCI-Admin header
  if (request.headers.get('X-FCI-Admin') !== env.FCI_ADMIN_KEY) {
    return err('Admin key required for voter load', 403);
  }

  const body   = await request.json();
  const voters = body.voters;
  if (!Array.isArray(voters) || !voters.length) return err('voters array required');

  // Clear existing voters first if requested
  if (body.replace) {
    await env.DB.prepare('DELETE FROM voters').run();
  }

  const stmt = env.DB.prepare(`
    INSERT OR REPLACE INTO voters
      (id, data, lat, lon, municipality, township, village,
       precinct, precinct_name, st_house, st_senate, cong_dist, ward, score, party)
    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
  `);

  const BATCH = 100;
  let count = 0;
  for (let i = 0; i < voters.length; i += BATCH) {
    const chunk = voters.slice(i, i + BATCH);
    const batch = chunk.map(v => stmt.bind(
      v.id,
      JSON.stringify(v),
      v.lat || null,
      v.lon || null,
      (v.municipality || '').toUpperCase(),
      (v.township     || '').toUpperCase(),
      (v.village      || '').toUpperCase(),
      (v.precinct     || '').toUpperCase(),
      (v.precinctName || '').toUpperCase(),
      (v.stHouse      || '').toUpperCase(),
      (v.stSenate     || '').toUpperCase(),
      (v.congDist     || '').toUpperCase(),
      (v.ward         || '').toUpperCase(),
      v.score         || '',
      v.party         || '',
    ));
    await env.DB.batch(batch);
    count += chunk.length;
  }

  return json({ ok: true, loaded: count });
}

// ── Router ────────────────────────────────────────────────────────────────
export default {
  async fetch(request, env, ctx) {
    // CORS preflight
    if (request.method === 'OPTIONS') {
      return new Response(null, { status: 204, headers: CORS });
    }

    // Auth check (skip for OPTIONS/ping)
    const url      = new URL(request.url);
    const pathname = url.pathname;

    if (pathname !== '/api/ping' && !auth(request, env)) {
      return err('Unauthorized', 401);
    }

    const method = request.method;

    // Ping
    if (pathname === '/api/ping') {
      return json({ ok: true, ts: new Date().toISOString() });
    }

    // Voters
    if (pathname === '/api/voters' && method === 'GET') return getVoters(request, env);
    if (pathname === '/api/voters/count' && method === 'GET') return countVoters(request, env);

    // Contacts
    const contactMatch = pathname.match(/^\/api\/contacts\/(.+)$/);
    if (contactMatch) {
      const cid = decodeURIComponent(contactMatch[1]);
      if (method === 'GET')  return getContacts(cid, request, env);
      if (method === 'POST') return upsertContacts(cid, request, env);
    }

    // Campaigns
    if (pathname === '/api/campaigns' && method === 'GET')  return getCampaigns(env);
    if (pathname === '/api/campaigns' && method === 'POST') return saveCampaign(request, env);
    const camMatch = pathname.match(/^\/api\/campaigns\/(.+)$/);
    if (camMatch && method === 'DELETE') return deleteCampaign(decodeURIComponent(camMatch[1]), env);

    // Admin voter load
    if (pathname === '/api/admin/load-voters' && method === 'POST') return loadVoters(request, env);

    return err('Not found', 404);
  },
};
