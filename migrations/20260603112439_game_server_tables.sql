CREATE TABLE sessions (
    primary_key BIGSERIAL PRIMARY KEY,
    user_id     TEXT NOT NULL,
    token       TEXT NOT NULL UNIQUE,
    join_token  TEXT NOT NULL UNIQUE
);

CREATE TABLE server_nonces (
    primary_key BIGSERIAL PRIMARY KEY,
    nonce       TEXT NOT NULL UNIQUE,
    expires_at  BIGINT NOT NULL
);

CREATE TABLE global_chat_messages (
    primary_key BIGSERIAL PRIMARY KEY,
    user_id     TEXT NOT NULL,
    sent_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    content     TEXT NOT NULL
);
