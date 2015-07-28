CREATE SCHEMA sri2postgres AUTHORIZATION admin;

SET search_path TO sri2postgres;

DROP TABLE IF EXISTS "jsonb" CASCADE;

CREATE TABLE "jsonb" (
    "key" uuid unique,
    "details" jsonb
);