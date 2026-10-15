-- ============================================================================
-- Gated Real-Time Auction Engine | PostgreSQL Database Schema & Seed Data
-- Enforces strict ACID compliance, sequence monotonic constraints, and escrow audits.
-- ============================================================================

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Drop existing tables if needed (clean slate)
DROP TABLE IF EXISTS telemetry_metrics CASCADE;
DROP TABLE IF EXISTS escrow_transactions CASCADE;
DROP TABLE IF EXISTS bids CASCADE;
DROP TABLE IF EXISTS auctions CASCADE;
DROP TABLE IF EXISTS escrow_accounts CASCADE;
DROP TABLE IF EXISTS users CASCADE;

-- ----------------------------------------------------------------------------
-- 1. USERS: Authenticated Bidders with KYC & Tier Classifications
-- ----------------------------------------------------------------------------
CREATE TABLE users (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    username VARCHAR(64) UNIQUE NOT NULL,
    email VARCHAR(255) UNIQUE NOT NULL,
    hashed_password VARCHAR(255) NOT NULL,
    full_name VARCHAR(128) NOT NULL,
    kyc_status VARCHAR(32) NOT NULL DEFAULT 'VERIFIED'
        CHECK (kyc_status IN ('UNVERIFIED', 'PENDING', 'VERIFIED', 'VIP')),
    tier VARCHAR(32) NOT NULL DEFAULT 'GOLD'
        CHECK (tier IN ('STANDARD', 'GOLD', 'PLATINUM', 'DIAMOND')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ----------------------------------------------------------------------------
-- 2. ESCROW ACCOUNTS: Financial collateral required for gated bidding
-- ----------------------------------------------------------------------------
CREATE TABLE escrow_accounts (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE UNIQUE,
    total_balance NUMERIC(16, 2) NOT NULL DEFAULT 100000.00 CHECK (total_balance >= 0),
    locked_collateral NUMERIC(16, 2) NOT NULL DEFAULT 0.00 CHECK (locked_collateral >= 0),
    available_balance NUMERIC(16, 2) GENERATED ALWAYS AS (total_balance - locked_collateral) STORED,
    currency VARCHAR(8) NOT NULL DEFAULT 'USD',
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ----------------------------------------------------------------------------
-- 3. AUCTIONS: High-value real-time assets with soft-close & anti-sniping
-- ----------------------------------------------------------------------------
CREATE TABLE auctions (
    id VARCHAR(64) PRIMARY KEY,
    title VARCHAR(255) NOT NULL,
    description TEXT NOT NULL,
    category VARCHAR(64) NOT NULL,
    starting_price NUMERIC(16, 2) NOT NULL CHECK (starting_price > 0),
    reserve_price NUMERIC(16, 2) NOT NULL CHECK (reserve_price >= starting_price),
    current_price NUMERIC(16, 2) NOT NULL CHECK (current_price >= starting_price),
    min_increment NUMERIC(16, 2) NOT NULL DEFAULT 250.00 CHECK (min_increment > 0),
    highest_bidder_id UUID REFERENCES users(id),
    highest_bidder_name VARCHAR(64),
    bid_count INT NOT NULL DEFAULT 0 CHECK (bid_count >= 0),
    sequence_id BIGINT NOT NULL DEFAULT 0 CHECK (sequence_id >= 0),
    status VARCHAR(32) NOT NULL DEFAULT 'ACTIVE'
        CHECK (status IN ('UPCOMING', 'ACTIVE', 'PAUSED', 'ENDED', 'SETTLED')),
    min_escrow_required NUMERIC(16, 2) NOT NULL DEFAULT 5000.00 CHECK (min_escrow_required >= 0),
    start_time TIMESTAMPTZ NOT NULL,
    end_time TIMESTAMPTZ NOT NULL,
    soft_close_window_seconds INT NOT NULL DEFAULT 15 CHECK (soft_close_window_seconds >= 5),
    extension_seconds INT NOT NULL DEFAULT 30 CHECK (extension_seconds >= 10),
    image_url TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ----------------------------------------------------------------------------
-- 4. BIDS: Immutable transaction ledger with strict per-auction monotonic sequence
-- ----------------------------------------------------------------------------
CREATE TABLE bids (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    auction_id VARCHAR(64) NOT NULL REFERENCES auctions(id) ON DELETE CASCADE,
    bidder_id UUID NOT NULL REFERENCES users(id),
    bidder_name VARCHAR(64) NOT NULL,
    sequence_id BIGINT NOT NULL,
    amount NUMERIC(16, 2) NOT NULL CHECK (amount > 0),
    latency_ms NUMERIC(8, 2) NOT NULL DEFAULT 1.25,
    status VARCHAR(32) NOT NULL DEFAULT 'ACCEPTED'
        CHECK (status IN ('ACCEPTED', 'REJECTED', 'SUPERSEDED')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_auction_sequence UNIQUE (auction_id, sequence_id)
);

-- Index for high-speed chronological lookups
CREATE INDEX idx_bids_auction_created ON bids (auction_id, created_at DESC);
CREATE INDEX idx_bids_bidder ON bids (bidder_id);

-- ----------------------------------------------------------------------------
-- 5. ESCROW TRANSACTIONS: Append-only financial audit log
-- ----------------------------------------------------------------------------
CREATE TABLE escrow_transactions (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    escrow_id UUID NOT NULL REFERENCES escrow_accounts(id),
    auction_id VARCHAR(64) REFERENCES auctions(id),
    transaction_type VARCHAR(32) NOT NULL
        CHECK (transaction_type IN ('DEPOSIT', 'HOLD', 'RELEASE', 'WIN_SETTLEMENT', 'WITHDRAWAL')),
    amount NUMERIC(16, 2) NOT NULL,
    idempotency_key VARCHAR(128) UNIQUE NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_escrow_tx_idempotency ON escrow_transactions (idempotency_key);

-- ----------------------------------------------------------------------------
-- 6. TELEMETRY METRICS: Engine throughput & latency telemetry snapshots
-- ----------------------------------------------------------------------------
CREATE TABLE telemetry_metrics (
    id BIGSERIAL PRIMARY KEY,
    auction_id VARCHAR(64) NOT NULL,
    active_ws_clients INT NOT NULL DEFAULT 0,
    bids_per_second NUMERIC(8, 2) NOT NULL DEFAULT 0.0,
    p50_latency_ms NUMERIC(8, 3) NOT NULL DEFAULT 0.0,
    p95_latency_ms NUMERIC(8, 3) NOT NULL DEFAULT 0.0,
    p99_latency_ms NUMERIC(8, 3) NOT NULL DEFAULT 0.0,
    recorded_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ============================================================================
-- SEED DATA: Pre-configured verified VIP bidders and active luxury auctions
-- ============================================================================

-- Seed 5 VIP / Platinum Bidders
INSERT INTO users (id, username, email, hashed_password, full_name, kyc_status, tier) VALUES
('a0000001-0000-0000-0000-000000000001', 'apex_collector', 'apex@sothebys-vip.com', '$2b$12$e80yq7K9bL9rD6y1tWfJse8F6dD3yS8T9Wq1tWfJse8F6dD3yS8T9', 'Alistair Vance', 'VIP', 'DIAMOND'),
('a0000002-0000-0000-0000-000000000002', 'hyper_bidder', 'hyper@monacoprivate.mc', '$2b$12$e80yq7K9bL9rD6y1tWfJse8F6dD3yS8T9Wq1tWfJse8F6dD3yS8T9', 'Elena Rostova', 'VIP', 'DIAMOND'),
('a0000003-0000-0000-0000-000000000003', 'geneva_vault', 'vault@geneva-reserve.ch', '$2b$12$e80yq7K9bL9rD6y1tWfJse8F6dD3yS8T9Wq1tWfJse8F6dD3yS8T9', 'Henri Laurent', 'VERIFIED', 'PLATINUM'),
('a0000004-0000-0000-0000-000000000004', 'nordic_fund', 'bids@nordicquant.oslo', '$2b$12$e80yq7K9bL9rD6y1tWfJse8F6dD3yS8T9Wq1tWfJse8F6dD3yS8T9', 'Astrid Lindholm', 'VERIFIED', 'PLATINUM'),
('a0000005-0000-0000-0000-000000000005', 'silicon_syndicate', 'partner@angelventures.sf', '$2b$12$e80yq7K9bL9rD6y1tWfJse8F6dD3yS8T9Wq1tWfJse8F6dD3yS8T9', 'Marcus Chen', 'VERIFIED', 'GOLD');

-- Seed Escrow Accounts with heavy collateral balances
INSERT INTO escrow_accounts (user_id, total_balance, locked_collateral, currency) VALUES
('a0000001-0000-0000-0000-000000000001', 750000.00, 0.00, 'USD'),
('a0000002-0000-0000-0000-000000000002', 500000.00, 0.00, 'USD'),
('a0000003-0000-0000-0000-000000000003', 350000.00, 0.00, 'USD'),
('a0000004-0000-0000-0000-000000000004', 250000.00, 0.00, 'USD'),
('a0000005-0000-0000-0000-000000000005', 150000.00, 0.00, 'USD');

-- Seed 4 High-Frequency Luxury Auctions with Live Countdown Windows
INSERT INTO auctions (
    id, title, description, category, starting_price, reserve_price, current_price,
    min_increment, highest_bidder_id, highest_bidder_name, bid_count, sequence_id,
    status, min_escrow_required, start_time, end_time, soft_close_window_seconds,
    extension_seconds, image_url
) VALUES
(
    'auc_shelby_gt500',
    '1967 Shelby GT500 Super Snake (1 of 1 Experimental 427 CI)',
    'Original prototype commissioned for Goodyear Thunderbolt tire tests. Powered by Carroll Shelby 427 FE medium-riser V8 generating 520HP. 4-speed manual top-loader. Verified provenance, Registry Certificate #001.',
    'Exotic Automobilia',
    120000.00,
    150000.00,
    145000.00,
    2500.00,
    'a0000001-0000-0000-0000-000000000001',
    'apex_collector',
    18,
    18,
    'ACTIVE',
    15000.00,
    NOW() - INTERVAL '30 minutes',
    NOW() + INTERVAL '25 minutes',
    15,
    30,
    'https://images.unsplash.com/photo-1584345604476-8ec5e12e42dd?auto=format&fit=crop&w=1200&q=80'
),
(
    'auc_patek_6300g',
    'Patek Philippe Grandmaster Chime Ref. 6300G-010 White Gold',
    'Most complicated Patek Philippe wristwatch ever produced in regular production. Double-faced reversible case featuring 20 complications, 5 chiming modes, acoustic alarm, and date repeater.',
    'Haute Horlogerie',
    210000.00,
    260000.00,
    235000.00,
    5000.00,
    'a0000002-0000-0000-0000-000000000002',
    'hyper_bidder',
    11,
    11,
    'ACTIVE',
    25000.00,
    NOW() - INTERVAL '15 minutes',
    NOW() + INTERVAL '40 minutes',
    15,
    30,
    'https://images.unsplash.com/photo-1522335789203-aabd1fc54bc9?auto=format&fit=crop&w=1200&q=80'
),
(
    'auc_ferrari_f40_lm',
    '1989 Ferrari F40 LM Michelotto Competizione (Chassis #79890)',
    'Factory Michelotto race-spec upgrade. Twin-turbocharged 2.9L Tipo F120B V8 producing 720+ HP at 2.6 bar boost. Carbon-kevlar monocoque, Brembo multi-piston monobloc brakes, OZ Racing magnesium wheels.',
    'Exotic Automobilia',
    350000.00,
    420000.00,
    390000.00,
    10000.00,
    'a0000003-0000-0000-0000-000000000003',
    'geneva_vault',
    9,
    9,
    'ACTIVE',
    40000.00,
    NOW() - INTERVAL '10 minutes',
    NOW() + INTERVAL '55 minutes',
    20,
    45,
    'https://images.unsplash.com/photo-1592198084033-aade902d1aae?auto=format&fit=crop&w=1200&q=80'
),
(
    'auc_carbon_credit_blk',
    '50,000 Verified Amazon Rainforest Carbon Offset Units (VCS-2026)',
    'Gold Standard certified nature-based REDD+ forest preservation credits. Verified satellite NDVI telemetry auditing canopy density with zero leakage covenants. Instant tokenized ledger retirement.',
    'Green Commodities',
    75000.00,
    90000.00,
    82500.00,
    1500.00,
    'a0000004-0000-0000-0000-000000000004',
    'nordic_fund',
    14,
    14,
    'ACTIVE',
    10000.00,
    NOW() - INTERVAL '5 minutes',
    NOW() + INTERVAL '15 minutes',
    15,
    30,
    'https://images.unsplash.com/photo-1511497584788-87676104235f?auto=format&fit=crop&w=1200&q=80'
);

-- Seed initial bids for the active auctions to demonstrate sequence monotonic history
INSERT INTO bids (auction_id, bidder_id, bidder_name, sequence_id, amount, latency_ms, status, created_at) VALUES
('auc_shelby_gt500', 'a0000005-0000-0000-0000-000000000005', 'silicon_syndicate', 16, 140000.00, 1.45, 'SUPERSEDED', NOW() - INTERVAL '6 minutes'),
('auc_shelby_gt500', 'a0000002-0000-0000-0000-000000000002', 'hyper_bidder', 17, 142500.00, 1.12, 'SUPERSEDED', NOW() - INTERVAL '3 minutes'),
('auc_shelby_gt500', 'a0000001-0000-0000-0000-000000000001', 'apex_collector', 18, 145000.00, 0.98, 'ACCEPTED', NOW() - INTERVAL '45 seconds'),

('auc_patek_6300g', 'a0000003-0000-0000-0000-000000000003', 'geneva_vault', 10, 230000.00, 1.34, 'SUPERSEDED', NOW() - INTERVAL '4 minutes'),
('auc_patek_6300g', 'a0000002-0000-0000-0000-000000000002', 'hyper_bidder', 11, 235000.00, 1.05, 'ACCEPTED', NOW() - INTERVAL '1 minute');
