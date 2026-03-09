/**
 * Transformers for callbox_crm tables.
 */

function accounts(data) {
  return {
    schema: 'crm',
    table: 'tenants',
    record: {
      source: 'mysql',
      source_id: String(data.id),
      name: data.name ?? data.company_name ?? null,
      email: data.email ?? null,
      phone: data.phone ?? null,
      status: 'prospect',
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
  return {
    schema: 'crm',
    table: 'campaigns',
    record: {
      source: 'mysql',
      source_id: String(data.id),
      name: data.name ?? null,
      status: data.status ?? null,
      tenant_id: data.client_id ?? data.account_id ?? null,
    },
  };
}

module.exports = { accounts, users, campaigns };
