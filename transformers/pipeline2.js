/**
 * Transformers for callbox_pipeline2 tables.
 * Each function receives Maxwell event data and returns { schema, table, record }.
 */

function clients(data) {
  return {
    schema: 'crm',
    table: 'tenants',
    record: {
      source: 'mysql',
      source_id: String(data.id),
      name: data.name ?? null,
      email: data.email ?? null,
      phone: data.phone ?? null,
      address: data.address ?? null,
      city: data.city ?? null,
      state: data.state ?? null,
      zip: data.zip ?? null,
      country: data.country ?? null,
      status: data.status ?? null,
    },
  };
}

function contracts(data) {
  return {
    schema: 'crm',
    table: 'contracts',
    record: {
      source: 'mysql',
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
      source: 'mysql',
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
      source: 'mysql',
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

module.exports = { clients, contracts, contract_quotes, contracts_signature, employees };
