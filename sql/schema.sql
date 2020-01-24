CREATE SCHEMA sri2db AUTHORIZATION admin;

SET search_path TO sri2db;

DROP TABLE IF EXISTS "jsonb" CASCADE;

CREATE TABLE "jsonb" (
    "key" uuid unique,
    "value" jsonb
);

DROP SCHEMA IF EXISTS sri2db CASCADE;