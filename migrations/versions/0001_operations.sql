
CREATE TABLE authorization_scopes (
	id TEXT NOT NULL,
	principal_id TEXT NOT NULL,
	policy_version TEXT NOT NULL,
	authz_epoch INTEGER DEFAULT '0' NOT NULL,
	created_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
	PRIMARY KEY (id),
	CONSTRAINT ck_authorization_scopes_epoch CHECK (authz_epoch >= 0)
)

;


CREATE TABLE tasks (
	id TEXT NOT NULL,
	owner_scope TEXT NOT NULL,
	question TEXT NOT NULL,
	input_revision INTEGER NOT NULL,
	constraints_hash VARCHAR(64) NOT NULL,
	status VARCHAR(16) NOT NULL,
	topic_id TEXT,
	base_position_version INTEGER DEFAULT '0' NOT NULL,
	deadline TIMESTAMP WITH TIME ZONE NOT NULL,
	outcome TEXT,
	stop_reason TEXT,
	cancel_requested_at TIMESTAMP WITH TIME ZONE,
	critic_agents INTEGER DEFAULT '0' NOT NULL,
	review_rounds INTEGER DEFAULT '0' NOT NULL,
	schema_repairs INTEGER DEFAULT '0' NOT NULL,
	transient_retries INTEGER DEFAULT '0' NOT NULL,
	tool_calls INTEGER DEFAULT '0' NOT NULL,
	provider_calls INTEGER DEFAULT '0' NOT NULL,
	created_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
	PRIMARY KEY (id),
	CONSTRAINT ck_tasks_revision CHECK (input_revision >= 1),
	CONSTRAINT ck_tasks_position_version CHECK (base_position_version >= 0),
	CONSTRAINT ck_tasks_status CHECK (status IN ('QUEUED','RUNNING','WAITING','STOPPING','COMPLETED','FAILED','CANCELLED')),
	CONSTRAINT ck_tasks_counters CHECK (critic_agents >= 0 AND review_rounds >= 0 AND schema_repairs >= 0 AND transient_retries >= 0 AND tool_calls >= 0 AND provider_calls >= 0),
	FOREIGN KEY(owner_scope) REFERENCES authorization_scopes (id)
)

;


CREATE TABLE task_inputs (
	task_id TEXT NOT NULL,
	revision INTEGER NOT NULL,
	question TEXT NOT NULL,
	constraints JSONB DEFAULT '{}'::jsonb NOT NULL,
	constraints_hash VARCHAR(64) NOT NULL,
	accepted_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
	CONSTRAINT pk_task_inputs PRIMARY KEY (task_id, revision),
	CONSTRAINT ck_task_inputs_revision CHECK (revision >= 1),
	FOREIGN KEY(task_id) REFERENCES tasks (id) ON DELETE RESTRICT
)

;


CREATE TABLE operations (
	id TEXT NOT NULL,
	owner_scope TEXT NOT NULL,
	task_id TEXT,
	kind TEXT NOT NULL,
	request_hash VARCHAR(64) NOT NULL,
	state VARCHAR(20) NOT NULL,
	dispatch_state VARCHAR(24) DEFAULT 'NOT_STARTED' NOT NULL,
	execution_state VARCHAR(16) DEFAULT 'PENDING' NOT NULL,
	binding JSONB NOT NULL,
	envelope JSONB NOT NULL,
	receipt JSONB,
	observation JSONB DEFAULT '{}'::jsonb NOT NULL,
	last_error TEXT,
	created_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
	updated_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
	PRIMARY KEY (id),
	CONSTRAINT ck_operations_state CHECK (state IN ('CLAIMED','ADMITTED','COMPLETED','FAILED','UNKNOWN')),
	CONSTRAINT ck_operations_dispatch_state CHECK (dispatch_state IN ('NOT_STARTED','INTENT_RECORDED','DISPATCHED','UNKNOWN','QUIESCENT')),
	CONSTRAINT ck_operations_execution_state CHECK (execution_state IN ('PENDING','RUNNING','UNKNOWN','QUIESCENT')),
	FOREIGN KEY(owner_scope) REFERENCES authorization_scopes (id),
	FOREIGN KEY(task_id) REFERENCES tasks (id)
)

;


CREATE TABLE agent_registry (
	id TEXT NOT NULL,
	owner_scope TEXT NOT NULL,
	task_id TEXT,
	role VARCHAR(12) NOT NULL,
	persistence VARCHAR(12) DEFAULT 'ephemeral' NOT NULL,
	creation_operation_id TEXT NOT NULL,
	provider_agent_id TEXT,
	intended_state VARCHAR(20) NOT NULL,
	observed_state VARCHAR(16) DEFAULT 'UNKNOWN' NOT NULL,
	observed_at TIMESTAMP WITH TIME ZONE,
	active_attempt_id TEXT,
	policy_version TEXT NOT NULL,
	created_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
	PRIMARY KEY (id),
	CONSTRAINT ck_agent_registry_role CHECK (role IN ('hekate','critic')),
	CONSTRAINT ck_agent_registry_persistence CHECK (persistence IN ('persistent','ephemeral')),
	CONSTRAINT ck_agent_registry_observation CHECK (observed_state IN ('PRESENT','ABSENT','RUNNING','STOPPED','UNKNOWN')),
	FOREIGN KEY(owner_scope) REFERENCES authorization_scopes (id),
	FOREIGN KEY(task_id) REFERENCES tasks (id),
	FOREIGN KEY(creation_operation_id) REFERENCES operations (id),
	UNIQUE (provider_agent_id)
)

;

CREATE UNIQUE INDEX uq_agent_registry_critic_per_task ON agent_registry (task_id) WHERE role = 'critic' AND task_id IS NOT NULL;

CREATE UNIQUE INDEX uq_agent_registry_persistent_hekate ON agent_registry (owner_scope) WHERE role = 'hekate' AND persistence = 'persistent';


CREATE TABLE agent_leases (
	registry_id TEXT NOT NULL,
	owner_worker TEXT NOT NULL,
	fence INTEGER NOT NULL,
	expires_at TIMESTAMP WITH TIME ZONE NOT NULL,
	PRIMARY KEY (registry_id),
	CONSTRAINT ck_agent_leases_fence CHECK (fence >= 1),
	FOREIGN KEY(registry_id) REFERENCES agent_registry (id) ON DELETE CASCADE
)

;


CREATE TABLE attempts (
	id TEXT NOT NULL,
	task_id TEXT NOT NULL,
	kind TEXT NOT NULL,
	parent_attempt_id TEXT,
	review_round INTEGER DEFAULT '0' NOT NULL,
	input_revision INTEGER NOT NULL,
	agent_registry_id TEXT NOT NULL,
	status VARCHAR(16) NOT NULL,
	operation_id TEXT NOT NULL,
	reservation_id TEXT,
	deadline TIMESTAMP WITH TIME ZONE NOT NULL,
	created_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
	PRIMARY KEY (id),
	CONSTRAINT ck_attempts_revision_round CHECK (input_revision >= 1 AND review_round >= 0),
	CONSTRAINT ck_attempts_status CHECK (status IN ('PENDING','DISPATCHED','RUNNING','SUCCEEDED','FAILED','TIMED_OUT','CANCELLED')),
	FOREIGN KEY(task_id) REFERENCES tasks (id) ON DELETE RESTRICT,
	FOREIGN KEY(parent_attempt_id) REFERENCES attempts (id) ON DELETE RESTRICT,
	FOREIGN KEY(agent_registry_id) REFERENCES agent_registry (id) ON DELETE RESTRICT,
	UNIQUE (operation_id),
	FOREIGN KEY(operation_id) REFERENCES operations (id) ON DELETE RESTRICT
)

;

CREATE INDEX ix_attempts_task_status ON attempts (task_id, status);


CREATE TABLE agent_execution_holds (
	id TEXT NOT NULL,
	registry_id TEXT NOT NULL,
	operation_id TEXT NOT NULL,
	state VARCHAR(16) NOT NULL,
	reason TEXT,
	created_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
	quiescent_at TIMESTAMP WITH TIME ZONE,
	PRIMARY KEY (id),
	CONSTRAINT ck_agent_execution_holds_state CHECK (state IN ('PENDING','RUNNING','UNKNOWN','QUIESCENT')),
	FOREIGN KEY(registry_id) REFERENCES agent_registry (id) ON DELETE RESTRICT,
	UNIQUE (operation_id),
	FOREIGN KEY(operation_id) REFERENCES operations (id) ON DELETE RESTRICT
)

;

CREATE UNIQUE INDEX uq_agent_execution_hold_active_registry ON agent_execution_holds (registry_id) WHERE quiescent_at IS NULL;


CREATE TABLE outbox (
	id TEXT NOT NULL,
	operation_id TEXT NOT NULL,
	kind TEXT NOT NULL,
	generation INTEGER DEFAULT '0' NOT NULL,
	payload JSONB DEFAULT '{}'::jsonb NOT NULL,
	status VARCHAR(12) DEFAULT 'PENDING' NOT NULL,
	available_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
	claim_owner TEXT,
	claim_fence INTEGER,
	claim_expires_at TIMESTAMP WITH TIME ZONE,
	created_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
	acked_at TIMESTAMP WITH TIME ZONE,
	PRIMARY KEY (id),
	CONSTRAINT uq_outbox_generation UNIQUE (operation_id, kind, generation),
	CONSTRAINT ck_outbox_generation CHECK (generation >= 0),
	CONSTRAINT ck_outbox_status CHECK (status IN ('PENDING','CLAIMED','ACKED','FAILED')),
	FOREIGN KEY(operation_id) REFERENCES operations (id) ON DELETE RESTRICT
)

;

CREATE INDEX ix_outbox_due ON outbox (status, available_at);


CREATE TABLE inbox (
	id TEXT NOT NULL,
	provider_scope TEXT NOT NULL,
	stable_event_key TEXT NOT NULL,
	payload_hash VARCHAR(64) NOT NULL,
	payload JSONB NOT NULL,
	received_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
	processed_at TIMESTAMP WITH TIME ZONE,
	PRIMARY KEY (id),
	CONSTRAINT uq_inbox_same_observation UNIQUE (provider_scope, stable_event_key, payload_hash)
)

;

CREATE INDEX ix_inbox_event_identity ON inbox (provider_scope, stable_event_key);


CREATE TABLE audit_events (
	id TEXT NOT NULL,
	owner_scope TEXT,
	task_id TEXT,
	attempt_id TEXT,
	operation_id TEXT,
	registry_id TEXT,
	event_kind TEXT NOT NULL,
	safe_payload JSONB DEFAULT '{}'::jsonb NOT NULL,
	created_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
	PRIMARY KEY (id),
	FOREIGN KEY(owner_scope) REFERENCES authorization_scopes (id),
	FOREIGN KEY(task_id) REFERENCES tasks (id),
	FOREIGN KEY(attempt_id) REFERENCES attempts (id),
	FOREIGN KEY(operation_id) REFERENCES operations (id),
	FOREIGN KEY(registry_id) REFERENCES agent_registry (id)
)

;


CREATE TABLE recovery_cases (
	operation_id TEXT NOT NULL,
	reason TEXT NOT NULL,
	observations JSONB DEFAULT '{}'::jsonb NOT NULL,
	resolution TEXT,
	updated_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
	PRIMARY KEY (operation_id),
	FOREIGN KEY(operation_id) REFERENCES operations (id) ON DELETE RESTRICT
)

;

ALTER TABLE agent_registry ADD CONSTRAINT fk_agent_registry_active_attempt FOREIGN KEY(active_attempt_id) REFERENCES attempts (id) ON DELETE SET NULL;
