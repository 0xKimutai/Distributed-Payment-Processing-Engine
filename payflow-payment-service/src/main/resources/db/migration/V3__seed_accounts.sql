-- =============================================================================
-- V3__seed_accounts.sql
--
-- 20 simulated Kenyan accounts with realistic names, M-Pesa-style phone numbers,
-- and varying balances to make payment scenarios interesting:
--   - High balance accounts (can send large payments)
--   - Low balance accounts (will fail if they try to send too much)
--   - Zero balance accounts (all payments from them should fail)
-- =============================================================================

INSERT INTO accounts (account_number, owner_name, phone_number, balance, currency) VALUES
-- High balance — reliable senders
('ACC-001', 'Kamau Njoroge',    '254712100001', 250000.0000, 'KES'),
('ACC-002', 'Wanjiku Muthoni',  '254722100002', 180000.0000, 'KES'),
('ACC-003', 'Otieno Odhiambo',  '254733100003', 320000.0000, 'KES'),
('ACC-004', 'Akinyi Owino',     '254711100004', 95000.0000,  'KES'),
('ACC-005', 'Mwangi Kariuki',   '254700100005', 410000.0000, 'KES'),

-- Medium balance
('ACC-006', 'Chebet Rotich',    '254712100006', 45000.0000,  'KES'),
('ACC-007', 'Omondi Onyango',   '254722100007', 62000.0000,  'KES'),
('ACC-008', 'Njambi Gichuru',   '254733100008', 28000.0000,  'KES'),
('ACC-009', 'Kipchoge Mutai',   '254711100009', 75000.0000,  'KES'),
('ACC-010', 'Amina Hassan',     '254700100010', 33000.0000,  'KES'),

-- Low balance — will cause INSUFFICIENT_FUNDS on large payments
('ACC-011', 'Baraka Mwenda',    '254712100011', 500.0000,    'KES'),
('ACC-012', 'Grace Wambua',     '254722100012', 1200.0000,   'KES'),
('ACC-013', 'Hassan Abdi',      '254733100013', 800.0000,    'KES'),

-- Zero balance — all payments from these will fail
('ACC-014', 'Fatuma Omar',      '254711100014', 0.0000,      'KES'),
('ACC-015', 'Juma Bakari',      '254700100015', 0.0000,      'KES'),

-- Receivers only (they receive but never send in our test scenarios)
('ACC-016', 'Mercy Achieng',    '254712100016', 5000.0000,   'KES'),
('ACC-017', 'Patrick Kimani',   '254722100017', 12000.0000,  'KES'),
('ACC-018', 'Susan Nyambura',   '254733100018', 8500.0000,   'KES'),
('ACC-019', 'David Mutua',      '254711100019', 22000.0000,  'KES'),
('ACC-020', 'Esther Wairimu',   '254700100020', 15000.0000,  'KES');
