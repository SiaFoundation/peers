CREATE TABLE syncer_peers (
	peer_address TEXT PRIMARY KEY NOT NULL,
	first_seen INTEGER NOT NULL,
	current_height INTEGER NOT NULL,
	last_scan_attempt INTEGER NOT NULL,
	last_successful_scan INTEGER NOT NULL,
	next_scan_attempt INTEGER NOT NULL,
	consecutive_failures INTEGER NOT NULL,
	failure_rate REAL NOT NULL
);
CREATE INDEX syncer_peers_next_scan_attempt_idx ON syncer_peers (peer_address, next_scan_attempt);
CREATE INDEX syncer_peers_is_full_node_idx ON syncer_peers (peer_address, failure_rate, current_height);

CREATE TABLE peer_locations (
	peer_address TEXT REFERENCES syncer_peers(peer_address) NOT NULL,
	country_code TEXT NOT NULL,
	latitude REAL NOT NULL,
	longitude REAL NOT NULL
);
CREATE INDEX peer_locations_peer_address_idx ON peer_locations (peer_address);


CREATE TABLE global_settings (
	id INTEGER PRIMARY KEY CHECK(id=0), -- only one row allowed
	db_version INTEGER NOT NULL
);