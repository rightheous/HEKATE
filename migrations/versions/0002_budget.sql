
CREATE TABLE budget_accounts (
	id TEXT NOT NULL,
	scope_kind VARCHAR(8) NOT NULL,
	scope_ref TEXT NOT NULL,
	period_id TEXT NOT NULL,
	limit_amount NUMERIC NOT NULL,
	spent_amount NUMERIC DEFAULT '0' NOT NULL,
	held_amount NUMERIC DEFAULT '0' NOT NULL,
	created_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
	PRIMARY KEY (id),
	CONSTRAINT uq_budget_account_period UNIQUE (scope_kind, scope_ref, period_id),
	CONSTRAINT ck_budget_account_scope_kind CHECK (scope_kind IN ('TASK','SYSTEM')),
	CONSTRAINT ck_budget_account_nonnegative CHECK (limit_amount >= 0 AND spent_amount >= 0 AND held_amount >= 0),
	CONSTRAINT ck_budget_account_finite CHECK (limit_amount < 'Infinity'::numeric AND limit_amount > '-Infinity'::numeric AND spent_amount < 'Infinity'::numeric AND spent_amount > '-Infinity'::numeric AND held_amount < 'Infinity'::numeric AND held_amount > '-Infinity'::numeric)
)

;


CREATE TABLE budget_reservations (
	id TEXT NOT NULL,
	operation_id TEXT NOT NULL,
	purpose TEXT NOT NULL,
	amount NUMERIC NOT NULL,
	status VARCHAR(24) NOT NULL,
	pricing_version TEXT NOT NULL,
	system_period_id TEXT NOT NULL,
	created_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
	settled_at TIMESTAMP WITH TIME ZONE,
	PRIMARY KEY (id),
	CONSTRAINT uq_budget_reservation_purpose UNIQUE (operation_id, purpose),
	CONSTRAINT ck_budget_reservation_purpose CHECK (purpose IN ('operation_envelope','final_response')),
	CONSTRAINT ck_budget_reservation_status CHECK (status IN ('RESERVED','PENDING_SETTLEMENT','SETTLED','RELEASED')),
	CONSTRAINT ck_budget_reservation_finite CHECK (amount >= 0 AND amount < 'Infinity'::numeric AND amount > '-Infinity'::numeric),
	FOREIGN KEY(operation_id) REFERENCES operations (id) ON DELETE RESTRICT
)

;


CREATE TABLE reservation_accounts (
	reservation_id TEXT NOT NULL,
	account_id TEXT NOT NULL,
	held_amount NUMERIC NOT NULL,
	created_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
	CONSTRAINT pk_reservation_accounts PRIMARY KEY (reservation_id, account_id),
	CONSTRAINT ck_reservation_accounts_finite CHECK (held_amount >= 0 AND held_amount < 'Infinity'::numeric AND held_amount > '-Infinity'::numeric),
	FOREIGN KEY(reservation_id) REFERENCES budget_reservations (id) ON DELETE RESTRICT,
	FOREIGN KEY(account_id) REFERENCES budget_accounts (id) ON DELETE RESTRICT
)

;


CREATE TABLE provider_calls (
	accounting_call_id TEXT NOT NULL,
	permit_id TEXT NOT NULL,
	operation_id TEXT NOT NULL,
	attempt_id TEXT NOT NULL,
	reservation_id TEXT NOT NULL,
	registry_id TEXT NOT NULL,
	call_kind TEXT NOT NULL,
	slot_key TEXT NOT NULL,
	intent_hash VARCHAR(64) NOT NULL,
	model TEXT NOT NULL,
	status VARCHAR(16) NOT NULL,
	max_input_tokens INTEGER NOT NULL,
	max_output_tokens INTEGER NOT NULL,
	allocation_amount NUMERIC NOT NULL,
	pricing_version TEXT NOT NULL,
	input_usd_per_million NUMERIC NOT NULL,
	output_usd_per_million NUMERIC NOT NULL,
	model_profile_verified BOOLEAN DEFAULT false NOT NULL,
	pricing_verified BOOLEAN DEFAULT false NOT NULL,
	tokenizer_verified BOOLEAN DEFAULT false NOT NULL,
	test_only BOOLEAN DEFAULT false NOT NULL,
	input_revision INTEGER NOT NULL,
	fence INTEGER NOT NULL,
	conversation_id TEXT,
	lease_owner TEXT NOT NULL,
	expires_at TIMESTAMP WITH TIME ZONE NOT NULL,
	consumed_at TIMESTAMP WITH TIME ZONE,
	dispatched_at TIMESTAMP WITH TIME ZONE,
	provider_call_id TEXT,
	created_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
	PRIMARY KEY (accounting_call_id),
	CONSTRAINT uq_provider_call_slot UNIQUE (operation_id, slot_key),
	CONSTRAINT ck_provider_calls_status CHECK (status IN ('ALLOCATED','CONSUMED','DISPATCHED','RUNNING','UNKNOWN','QUIESCENT','EXPIRED','REVOKED')),
	CONSTRAINT ck_provider_calls_limits CHECK (max_input_tokens >= 0 AND max_output_tokens >= 0 AND input_revision >= 1 AND fence >= 1),
	CONSTRAINT ck_provider_calls_amounts CHECK (allocation_amount >= 0 AND input_usd_per_million >= 0 AND output_usd_per_million >= 0),
	UNIQUE (permit_id),
	FOREIGN KEY(operation_id) REFERENCES operations (id) ON DELETE RESTRICT,
	FOREIGN KEY(attempt_id) REFERENCES attempts (id) ON DELETE RESTRICT,
	FOREIGN KEY(reservation_id) REFERENCES budget_reservations (id) ON DELETE RESTRICT,
	FOREIGN KEY(registry_id) REFERENCES agent_registry (id) ON DELETE RESTRICT
)

;

CREATE INDEX ix_provider_calls_operation ON provider_calls (operation_id);


CREATE TABLE call_permits (
	permit_id TEXT NOT NULL,
	accounting_call_id TEXT NOT NULL,
	state VARCHAR(12) NOT NULL,
	issued_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
	expires_at TIMESTAMP WITH TIME ZONE NOT NULL,
	consumed_at TIMESTAMP WITH TIME ZONE,
	revoked_at TIMESTAMP WITH TIME ZONE,
	PRIMARY KEY (permit_id),
	CONSTRAINT ck_call_permits_state CHECK (state IN ('ISSUED','CONSUMED','REVOKED','EXPIRED')),
	UNIQUE (accounting_call_id),
	FOREIGN KEY(accounting_call_id) REFERENCES provider_calls (accounting_call_id) ON DELETE RESTRICT
)

;


CREATE TABLE call_allocations (
	accounting_call_id TEXT NOT NULL,
	reservation_id TEXT NOT NULL,
	account_id TEXT NOT NULL,
	amount NUMERIC NOT NULL,
	CONSTRAINT pk_call_allocations PRIMARY KEY (accounting_call_id, account_id),
	CONSTRAINT fk_call_allocations_reservation_account FOREIGN KEY(reservation_id, account_id) REFERENCES reservation_accounts (reservation_id, account_id) ON DELETE RESTRICT,
	CONSTRAINT ck_call_allocations_finite CHECK (amount >= 0 AND amount < 'Infinity'::numeric AND amount > '-Infinity'::numeric),
	FOREIGN KEY(accounting_call_id) REFERENCES provider_calls (accounting_call_id) ON DELETE RESTRICT
)

;


CREATE TABLE usage_observations (
	id TEXT NOT NULL,
	accounting_call_id TEXT NOT NULL,
	provider_call_id TEXT,
	source TEXT NOT NULL,
	observation_identity TEXT NOT NULL,
	payload_hash VARCHAR(64) NOT NULL,
	completeness VARCHAR(12) NOT NULL,
	input_tokens INTEGER,
	output_tokens INTEGER,
	cache_tokens INTEGER,
	reasoning_tokens INTEGER,
	total_tokens INTEGER,
	reported_cost_usd NUMERIC,
	observed_at TIMESTAMP WITH TIME ZONE NOT NULL,
	received_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
	PRIMARY KEY (id),
	CONSTRAINT uq_usage_observation_payload UNIQUE (accounting_call_id, source, payload_hash),
	CONSTRAINT ck_usage_observation_completeness CHECK (completeness IN ('UNKNOWN','PARTIAL','COMPLETE')),
	CONSTRAINT ck_usage_observation_tokens CHECK ((input_tokens IS NULL OR input_tokens >= 0) AND (output_tokens IS NULL OR output_tokens >= 0) AND (cache_tokens IS NULL OR cache_tokens >= 0) AND (reasoning_tokens IS NULL OR reasoning_tokens >= 0) AND (total_tokens IS NULL OR total_tokens >= 0)),
	FOREIGN KEY(accounting_call_id) REFERENCES provider_calls (accounting_call_id) ON DELETE RESTRICT
)

;

CREATE INDEX ix_usage_observations_identity ON usage_observations (accounting_call_id, observation_identity);


CREATE TABLE usage_projections (
	accounting_call_id TEXT NOT NULL,
	completeness VARCHAR(12) NOT NULL,
	has_conflict BOOLEAN DEFAULT false NOT NULL,
	settlement_state VARCHAR(12) DEFAULT 'PENDING' NOT NULL,
	input_tokens INTEGER,
	output_tokens INTEGER,
	cache_tokens INTEGER,
	reasoning_tokens INTEGER,
	total_tokens INTEGER,
	reported_cost_usd NUMERIC,
	reported_cost_source TEXT,
	evaluated_cost_usd NUMERIC,
	overrun BOOLEAN DEFAULT false NOT NULL,
	updated_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
	PRIMARY KEY (accounting_call_id),
	CONSTRAINT ck_usage_projection_completeness CHECK (completeness IN ('UNKNOWN','PARTIAL','COMPLETE')),
	CONSTRAINT ck_usage_projection_settlement CHECK (settlement_state IN ('PENDING','SETTLED','CONFLICT')),
	FOREIGN KEY(accounting_call_id) REFERENCES provider_calls (accounting_call_id) ON DELETE RESTRICT
)

;


CREATE TABLE budget_ledger (
	id TEXT NOT NULL,
	account_id TEXT NOT NULL,
	reservation_id TEXT,
	accounting_call_id TEXT,
	effect_key TEXT NOT NULL,
	effect_type VARCHAR(16) NOT NULL,
	amount NUMERIC NOT NULL,
	held_delta NUMERIC DEFAULT '0' NOT NULL,
	spent_delta NUMERIC DEFAULT '0' NOT NULL,
	created_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
	PRIMARY KEY (id),
	CONSTRAINT uq_budget_ledger_effect UNIQUE (account_id, effect_key),
	CONSTRAINT ck_budget_ledger_effect_type CHECK (effect_type IN ('HOLD','SETTLE','RELEASE','ADJUSTMENT')),
	CONSTRAINT ck_budget_ledger_finite CHECK (amount > '-Infinity'::numeric AND amount < 'Infinity'::numeric AND held_delta > '-Infinity'::numeric AND held_delta < 'Infinity'::numeric AND spent_delta > '-Infinity'::numeric AND spent_delta < 'Infinity'::numeric),
	FOREIGN KEY(account_id) REFERENCES budget_accounts (id) ON DELETE RESTRICT,
	FOREIGN KEY(reservation_id) REFERENCES budget_reservations (id) ON DELETE RESTRICT,
	FOREIGN KEY(accounting_call_id) REFERENCES provider_calls (accounting_call_id) ON DELETE RESTRICT
)

;

ALTER TABLE attempts ADD CONSTRAINT fk_attempts_reservation FOREIGN KEY(reservation_id) REFERENCES budget_reservations (id) ON DELETE RESTRICT;

ALTER TABLE provider_calls ADD CONSTRAINT fk_provider_calls_permit_id FOREIGN KEY(permit_id) REFERENCES call_permits (permit_id) DEFERRABLE INITIALLY DEFERRED;
