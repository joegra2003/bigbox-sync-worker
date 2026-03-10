/**
 * Transformers for callbox_crm tables.
 */

function accounts(data) {
  // callbox_crm.accounts = prospect companies → identity.orgs
  // (NOT crm.tenants — that's for Callbox's paying clients only)
  // phone/email/address live in identity.org_branches/person_channels (future enrichment)
  const typeMap = {
    'Private':      'company',
    'Public':       'company',
    'Government':   'government',
    'Non-Profit':   'nonprofit',
    'Agency':       'agency',
  };
  return {
    schema: 'identity',
    table: 'orgs',
    record: {
      source:      'crm',
      source_id:   String(data.account_id ?? data.id),
      name:        data.name ?? data.company_name ?? `Account #${data.account_id ?? data.id}`,
      entity_type: typeMap[data.type] ?? 'unknown',
      industry:    data.sic || null,
    },
  };
}

function users(data) {
  return [
    {
      schema: 'auth',
      table: 'users',
      record: {
        source: 'mysql',
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
        source: 'mysql',
        source_id: String(data.id),
        first_name: data.first_name ?? data.fname ?? null,
        last_name: data.last_name ?? data.lname ?? null,
        email: data.email ?? null,
        phone: data.phone ?? null,
      },
    },
  ];
}

function campaigns(data) {
  // Multi-step: needs tenant lookup + creates list + campaign + campaign_list junction
  return { _type: 'campaign_full', data };
}

module.exports = { accounts, users, campaigns };
