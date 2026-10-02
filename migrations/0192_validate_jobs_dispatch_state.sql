-- §46 forward-fix (FORWARD_ONLY, 0184 precedent; card 32, ADR-0058 D-A). VALIDATE the dispatch-state
-- CHECK 0190 added NOT VALID. Every row written before 0190 has dispatch_state NULL (the column did
-- not exist), so the scan finds no violator; it runs under SHARE UPDATE EXCLUSIVE and does not block
-- DML (card-27 DM-7 precedent). The CHECK text is unchanged.
ALTER TABLE ops.jobs VALIDATE CONSTRAINT jobs_dispatch_state_check;
