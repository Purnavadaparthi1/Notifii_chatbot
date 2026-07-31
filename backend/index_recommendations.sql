-- Recommended indexes for dual-table chatbot queries.
-- Run after validating table/column names in your target database.

CREATE INDEX idx_track_packages_account_recipient
    ON track_packages (account_id, recipient_id);

CREATE INDEX idx_track_packages_account_date_received
    ON track_packages (account_id, date_received);

CREATE INDEX idx_track_packages_account_package_id
    ON track_packages (account_id, package_id);

CREATE INDEX idx_core_recipients_account_recipient
    ON core_recipients (account_id, recipient_id);

CREATE INDEX idx_core_recipients_account_status
    ON core_recipients (account_id, recipient_status);

CREATE INDEX idx_core_recipients_account_email
    ON core_recipients (account_id, email);

CREATE INDEX idx_core_recipients_account_cellphone
    ON core_recipients (account_id, cellphone);
