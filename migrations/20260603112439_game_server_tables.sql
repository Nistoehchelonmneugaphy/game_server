CREATE TABLE users (
    primary_key BIGSERIAL PRIMARY KEY,
    user_id     TEXT NOT NULL UNIQUE,
    username    TEXT NOT NULL UNIQUE

);

CREATE TABLE sessions (
    primary_key BIGSERIAL PRIMARY KEY,
    user_id     TEXT NOT NULL REFERENCES users(user_id) ON DELETE RESTRICT,
    token       TEXT NOT NULL UNIQUE
);

CREATE TABLE server_nonces (
    primary_key BIGSERIAL PRIMARY KEY,
    nonce       TEXT NOT NULL UNIQUE,
    expires_at  BIGINT NOT NULL
);

CREATE TABLE global_chat_messages (
    primary_key BIGSERIAL PRIMARY KEY,
    message_id  TEXT NOT NULL UNIQUE,
    user_id     TEXT NOT NULL REFERENCES users(user_id) ON DELETE RESTRICT,
    sent_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    content     TEXT NOT NULL
);
