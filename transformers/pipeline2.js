/**
 * Transformers for callbox_pipeline2 tables.
 *
 * Sync transformers (return { schema, table, record }):
 *   clients, contracts, contract_quotes, contracts_signature, employees
 *
 * Async transformers (return Promise, receive { pg, mysql }):
 *   client_list_details  → crm.contacts           (G3)
 *   events_tm_ob_txn     → crm.activities +        (G5)
 *                          crm.outreach
 */

// ─── CHANNEL MAP ─────────────────────────────────────────────────────────────
// callbox_pipeline2.channels_lkp → crm.channel_type
const CHANNEL_MAP = {
  1:  'call',     // Calling
  2:  'email',    // Email
  3:  'linkedin', // Social Media (closest match)
  4:  'whatsapp', // Instant Message
  5:  'sms',      // SMS
  6:  'webchat',  // Chat
  7:  'webchat',  // Web Form
  8:  'webchat',  // Webinar
  10: 'webchat',  // Zoom
  11: null,       // Data Enrichment — not an outreach channel
  12: 'call',     // Inbound Call
};

// ─── SYNC TRANSFORMERS ────────────────────────────────────────────────────────

function clients(data) {
  const statusMap = {
    active: 'active', inactive: 'inactive',
    onhold: 'inactive', deleted: 'churned',
  };
  return {
    schema: 'crm',
    table: 'tenants',
    record: {
      source: 'pipeline2',
      source_id: String(data.client_id),
      name: data.campaign_name ?? data.referred_by ?? `Client #${data.client_id}`,
      status: statusMap[data.x] ?? 'active',
    },
  };
}

function contracts(data) {
  return {
    schema: 'crm',
    table: 'contracts',
    record: {
      source: 'pipeline2',
      source_id: String(data.id),
      tenant_id: data.client_id ? String(data.client_id) : null,
      name: data.name ?? null,
      status: data.status ?? null,
      start_date: data.start_date ?? null,
      end_date: data.end_date ?? null,
      amount: data.amount ?? null,
    },
  };
}

function contract_quotes(data) {
  return {
    schema: 'crm',
    table: 'contract_quotes',
    record: {
      source: 'pipeline2',
      source_id: String(data.id),
      contract_id: data.contract_id ? String(data.contract_id) : null,
      name: data.name ?? null,
      status: data.status ?? null,
      amount: data.amount ?? null,
    },
  };
}

function contracts_signature(data) {
  return {
    schema: 'crm',
    table: 'contract_signatures',
    record: {
      source: 'pipeline2',
      source_id: String(data.id),
      contract_id: data.contract_id ? String(data.contract_id) : null,
      signer_name: data.signer_name ?? data.name ?? null,
      signer_email: data.signer_email ?? data.email ?? null,
      signed_at: data.signed_at ?? data.created_at ?? null,
      status: data.status ?? null,
    },
  };
}

function employees(data) {
  return [
    {
      schema: 'auth',
      table: 'users',
      record: {
        source: 'pipeline2',
        source_id: String(data.id),
        email: data.email ?? null,
        role: data.role ?? null,
        status: data.status ?? null,
      },
    },
    {
      schema: 'identity',
      table: 'people',
      record: {
        source: 'pipeline2',
        source_id: String(data.id),
        first_name: data.first_name ?? data.fname ?? null,
        last_name: data.last_name ?? data.lname ?? null,
        email: data.email ?? null,
        phone: data.phone ?? null,
      },
    },
  ];
}

// ─── G3: client_list_details → crm.contacts ──────────────────────────────────

/**
 * Resolves a contact record from client_list_details.
 * Requires async lookups to Supabase (person_id, campaign_list_id)
 * and MySQL (lead_status via event_state_lkp).
 *
 * Returns null if required foreign keys cannot be resolved (skip event).
 */
async function client_list_details(data, { pg, mysql }) {
  const {
    client_list_detail_id,
    client_list_id,
    target_detail_id,
    event_tm_ob_txn_id,
    x,
    timestamp,
  } = data;

  // 1. Resolve person_id — identity.people seeded with source='pipeline2'
  const personRes = await pg.query(
    `SELECT id FROM identity.people
     WHERE source = 'pipeline2' AND source_id = $1
     LIMIT 1`,
    [String(target_detail_id)]
  );
  if (!personRes.rows.length) {
    // Target not yet in identity.people (dw2 lag or not yet seeded) — skip
    return null;
  }
  const person_id = personRes.rows[0].id;

  // 2. Resolve campaign_list_id — crm.lists seeded with source='pipeline2'
  const listRes = await pg.query(
    `SELECT cl.id
     FROM crm.campaign_lists cl
     JOIN crm.lists l ON l.id = cl.list_id
     WHERE l.source = 'pipeline2' AND l.source_id = $1
     LIMIT 1`,
    [String(client_list_id)]
  );
  if (!listRes.rows.length) {
    // List not yet loaded — skip (will be retried when list CDC event arrives)
    return null;
  }
  const campaign_list_id = listRes.rows[0].id;

  // 3. Resolve lead_status from event_state_lkp via last activity event
  let lead_status = 'identification'; // safe default
  if (event_tm_ob_txn_id) {
    const [rows] = await mysql.query(
      `SELECT esl.funnel_stage
       FROM callbox_pipeline2.events_tm_ob_txn txn
       JOIN callbox_pipeline2.events_tm_ob_lkp lkp
         ON lkp.event_tm_ob_lkp_id = txn.event_tm_ob_lkp_id
       JOIN callbox_pipeline2.event_state_lkp esl
         ON esl.event_state_lkp_id = lkp.event_state_lkp_id
       WHERE txn.event_tm_ob_txn_id = ?
       LIMIT 1`,
      [event_tm_ob_txn_id]
    );
    if (rows.length && rows[0].funnel_stage) {
      lead_status = rows[0].funnel_stage.toLowerCase(); // direct 1:1 enum match
    }
  }

  // 4. Map x (soft delete) → contact status
  const status = x ? 'archived' : 'active';

  return {
    schema: 'crm',
    table: 'contacts',
    record: {
      person_id,
      campaign_list_id,
      lead_status,
      status,
      source: 'pipeline2',
      source_id: String(client_list_detail_id),
      created_at: timestamp ?? undefined,
    },
  };
}

// ─── G5: events_tm_ob_txn → crm.activities + crm.outreach ───────────────────

/**
 * Maps an outbound activity event to crm.activities (base) and crm.outreach (detail).
 * Returns a two-element array: [activities_target, outreach_target].
 * outreach_target.record.__depends_on_activity = true signals the caller to
 * resolve activity_id from the activities upsert before writing outreach.
 *
 * Returns null if required foreign keys cannot be resolved.
 */
async function events_tm_ob_txn(data, { pg, mysql }) {
  const {
    event_tm_ob_txn_id,
    event_tm_ob_lkp_id,
    client_list_detail_id,
    op_center_lkp_id,
    timestamp,
  } = data;

  // 1. Resolve contact_id + campaign_id via crm.contacts chain
  const contactRes = await pg.query(
    `SELECT c.id AS contact_id, camp.id AS campaign_id
     FROM crm.contacts c
     JOIN crm.campaign_lists cl  ON cl.id  = c.campaign_list_id
     JOIN crm.campaigns     camp ON camp.id = cl.campaign_id
     WHERE c.source = 'pipeline2' AND c.source_id = $1
     LIMIT 1`,
    [String(client_list_detail_id)]
  );
  if (!contactRes.rows.length) {
    // Contact not yet synced (G3 must run first) — skip
    return null;
  }
  const { contact_id, campaign_id } = contactRes.rows[0];

  // 2. Resolve actor_id from iam.actors (op_center_lkp_id = employee id)
  let actor_id = null;
  if (op_center_lkp_id) {
    const actorRes = await pg.query(
      `SELECT id FROM iam.actors
       WHERE source = 'pipeline2' AND source_id = $1
       LIMIT 1`,
      [String(op_center_lkp_id)]
    );
    if (actorRes.rows.length) actor_id = actorRes.rows[0].id;
  }

  // 3. Resolve channel + event state from MySQL
  const [lkpRows] = await mysql.query(
    `SELECT lkp.channel_lkp_id,
            esl.funnel_stage,
            esl.event_state,
            esl.event_type
     FROM callbox_pipeline2.events_tm_ob_lkp lkp
     JOIN callbox_pipeline2.event_state_lkp esl
       ON esl.event_state_lkp_id = lkp.event_state_lkp_id
     WHERE lkp.event_tm_ob_lkp_id = ?
     LIMIT 1`,
    [event_tm_ob_lkp_id]
  );

  const channel_lkp_id = lkpRows[0]?.channel_lkp_id ?? null;
  const channel_type   = CHANNEL_MAP[channel_lkp_id] ?? 'call';
  const event_state    = lkpRows[0]?.event_state  ?? null;
  const funnel_stage   = lkpRows[0]?.funnel_stage ?? null;

  const occurred_at = timestamp ?? new Date().toISOString();

  // 4. crm.activities (base record)
  const activitiesTarget = {
    schema: 'crm',
    table: 'activities',
    record: {
      contact_id,
      campaign_id,
      actor_id,
      activity_type: channel_type,   // call | email | sms | linkedin | etc.
      channel_type,
      direction: 'outbound',
      event_state,
      funnel_stage,
      occurred_at,
      source: 'pipeline2',
      source_id: String(event_tm_ob_txn_id),
    },
  };

  // 5. crm.outreach (detail record — needs activity_id from step 4)
  //    __depends_on_activity signals processEvent to wire activity_id after upsert
  const outreachTarget = {
    schema: 'crm',
    table: 'outreach',
    __depends_on_activity: true,
    record: {
      // activity_id injected by processEvent after activities upsert
      channel_type,
      campaign_id,
      contact_id,
      actor_id,
      direction: 'outbound',
      status: resolveOutreachStatus(event_state),
      is_ai_initiated: false,
      occurred_at,
      source: 'pipeline2',
      source_id: String(event_tm_ob_txn_id),
    },
  };

  return [activitiesTarget, outreachTarget];
}

/**
 * Map event_state → crm.outreach_status enum.
 * Heuristic based on common pipeline2 event states.
 */
function resolveOutreachStatus(event_state) {
  if (!event_state) return 'pending';
  const s = event_state.toLowerCase();
  if (s.includes('connect') || s.includes('answer') || s.includes('talk'))  return 'connected';
  if (s.includes('sent') || s.includes('send'))                             return 'sent';
  if (s.includes('deliver'))                                                 return 'delivered';
  if (s.includes('bounce'))                                                  return 'bounced';
  if (s.includes('fail') || s.includes('no answer') || s.includes('busy')) return 'failed';
  return 'sent';
}

module.exports = {
  // sync
  clients,
  contracts,
  contract_quotes,
  contracts_signature,
  employees,
  // async (G3, G5)
  client_list_details,
  events_tm_ob_txn,
};
