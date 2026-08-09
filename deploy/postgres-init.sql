-- One database per service (ADR-0006: a service owns its database;
-- nothing reads across).
CREATE DATABASE ordering;
CREATE DATABASE billing;
CREATE DATABASE inventory;
CREATE DATABASE shipping;
