-- Enable UUID extension
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Accounts Table (Target for SCD Type 2 tracking in Silver)
CREATE TABLE accounts (
    account_id VARCHAR(50) PRIMARY KEY,
    merchant_id VARCHAR(50) NOT NULL,
    account_name VARCHAR(150) NOT NULL,
    tier VARCHAR(20) NOT NULL, -- e.g., 'ENTERPRISE', 'STANDARD'
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Invoices Table
CREATE TABLE invoices (
    invoice_id VARCHAR(50) PRIMARY KEY,
    account_id VARCHAR(50) NOT NULL REFERENCES accounts(account_id),
    total_amount DECIMAL(18, 4) NOT NULL CHECK (total_amount >= 0),
    currency VARCHAR(3) NOT NULL, -- 'INR', 'USD', etc.
    status VARCHAR(30) NOT NULL, -- 'ISSUED', 'PAID', 'CANCELLED'
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Ledger Journal Entries Table (Captured via Debezium CDC)
CREATE TABLE ledger_entries (
    entry_id UUID DEFAULT uuid_generate_v4() PRIMARY KEY,
    invoice_id VARCHAR(50) NOT NULL REFERENCES invoices(invoice_id),
    account_id VARCHAR(50) NOT NULL REFERENCES accounts(account_id),
    debit DECIMAL(18, 4) NOT NULL DEFAULT 0.0000 CHECK (debit >= 0),
    credit DECIMAL(18, 4) NOT NULL DEFAULT 0.0000 CHECK (credit >= 0),
    entry_ts TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Set replica identity full for Debezium CDC row captures
ALTER TABLE accounts REPLICA IDENTITY FULL;
ALTER TABLE invoices REPLICA IDENTITY FULL;
ALTER TABLE ledger_entries REPLICA IDENTITY FULL;