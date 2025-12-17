package sqlite

import (
	"database/sql"
	"errors"
	"fmt"
	"time"

	"go.sia.tech/peerd/geoip"
	"go.sia.tech/peerd/peers"
)

// AddPeer adds a new peer to the database. If the peer already exists, no action is taken.
func (s *Store) AddPeer(address string) (exists bool, err error) {
	err = s.transaction(func(tx *txn) error {
		res, err := tx.Exec(`INSERT INTO syncer_peers (peer_address, first_seen, last_successful_scan, last_scan_attempt, next_scan_attempt, consecutive_failures, failure_rate, current_height, successful_scans) VALUES ($1, $2, 0, 0, 0, 0, 0, 0, 0) ON CONFLICT (peer_address) DO NOTHING`, address, sqlTime(time.Now()))
		if err != nil {
			return err
		}
		n, err := res.RowsAffected()
		if err != nil {
			return err
		}
		exists = n == 0
		return nil
	})
	return
}

// AddScan records the result of a peer scan.
func (s *Store) AddScan(scan peers.PeerScan) error {
	return s.transaction(func(tx *txn) error {
		if !scan.Successful {
			const query = `UPDATE syncer_peers SET last_scan_attempt=$1, next_scan_attempt=$2, failure_rate=$3, consecutive_failures=consecutive_failures+1 WHERE peer_address=$4`
			_, err := tx.Exec(query, sqlTime(time.Now()), sqlTime(scan.NextScanTime), scan.FailureRate, scan.Address)
			return err
		}

		const query = `UPDATE syncer_peers SET last_successful_scan=$1, last_scan_attempt=$2, next_scan_attempt=$3, failure_rate=$4, current_height=$5, consecutive_failures=0, successful_scans=successful_scans+1 WHERE peer_address=$6`
		_, err := tx.Exec(query, sqlTime(time.Now()), sqlTime(time.Now()), sqlTime(scan.NextScanTime), scan.FailureRate, scan.CurrentHeight, scan.Address)
		if err != nil {
			return err
		}

		if _, err = tx.Exec(`DELETE FROM peer_locations WHERE peer_address=$1`, scan.Address); err != nil {
			return fmt.Errorf("failed to clear existing locations: %w", err)
		}

		locationStmt, err := tx.Prepare(`INSERT INTO peer_locations (peer_address, country_code, latitude, longitude) VALUES ($1, $2, $3, $4)`)
		if err != nil {
			return fmt.Errorf("failed to prepare location insert statement: %w", err)
		}
		defer locationStmt.Close()

		for _, loc := range scan.Locations {
			if _, err = locationStmt.Exec(scan.Address, loc.CountryCode, loc.Latitude, loc.Longitude); err != nil {
				return fmt.Errorf("failed to insert location: %w", err)
			}
		}
		return err
	})
}

// PeersForScan retrieves peers that are due for scanning, up to the specified limit.
// The peers will not be returned again until the timeout duration has passed or
// their next_scan_attempt is updated.
func (s *Store) PeersForScan(timeout time.Duration, limit int) (results []peers.Peer, err error) {
	err = s.transaction(func(tx *txn) error {
		rows, err := tx.Query(`UPDATE syncer_peers SET next_scan_attempt=$1 
WHERE 
peer_address IN (SELECT peer_address FROM syncer_peers WHERE next_scan_attempt <= $2 ORDER BY first_seen ASC, next_scan_attempt ASC LIMIT $3)
RETURNING peer_address, first_seen, last_successful_scan, last_scan_attempt, consecutive_failures, failure_rate, successful_scans`, sqlTime(time.Now().Add(timeout)), sqlTime(time.Now()), limit)
		if err != nil {
			return err
		}
		defer rows.Close()

		for rows.Next() {
			p, err := scanPeer(rows)
			if err != nil {
				return err
			}
			results = append(results, p)
		}
		return rows.Err()
	})
	return
}

// Peers retrieves a list of peers from the database with the specified offset and limit.
// The peers are sorted by failure rate (ascending) and last successful scan time (descending).
func (s *Store) Peers(offset, limit int) (results []peers.Peer, err error) {
	err = s.transaction(func(tx *txn) error {
		rows, err := tx.Query(`SELECT peer_address, first_seen, last_successful_scan, last_scan_attempt, consecutive_failures, failure_rate, successful_scans 
FROM syncer_peers
WHERE last_successful_scan <> 0
ORDER BY failure_rate ASC, successful_scans DESC, last_successful_scan DESC -- most reliable history and most recently seen peers first
LIMIT $1 OFFSET $2`, limit, offset)
		if err != nil {
			return err
		}
		defer rows.Close()

		for rows.Next() {
			p, err := scanPeer(rows)
			if err != nil {
				return err
			}
			results = append(results, p)
		}
		return rows.Err()
	})
	return
}

// Peer retrieves a single peer by its address.
func (s *Store) Peer(addr string) (p peers.Peer, err error) {
	err = s.transaction(func(tx *txn) error {
		row := tx.QueryRow(`SELECT peer_address, first_seen, last_successful_scan, last_scan_attempt, consecutive_failures, failure_rate, successful_scans 
FROM syncer_peers
WHERE peer_address=$1`, addr)
		p, err = scanPeer(row)
		if errors.Is(err, sql.ErrNoRows) {
			return peers.ErrNotFound
		}
		return err
	})
	return
}

// PeerLocations retrieves the stored geographic locations for the specified peer address.
func (s *Store) PeerLocations(address string) (locations []geoip.Location, err error) {
	err = s.transaction(func(tx *txn) error {
		rows, err := tx.Query(`SELECT country_code, latitude, longitude FROM peer_locations WHERE peer_address=$1`, address)
		if err != nil {
			return err
		}
		defer rows.Close()

		for rows.Next() {
			var loc geoip.Location
			if err := rows.Scan(&loc.CountryCode, &loc.Latitude, &loc.Longitude); err != nil {
				return err
			}
			locations = append(locations, loc)
		}
		return rows.Err()
	})
	return
}

func scanPeer(s scanner) (p peers.Peer, err error) {
	err = s.Scan(&p.Address, (*sqlTime)(&p.FirstSeen), (*sqlTime)(&p.LastSuccessfulScan), (*sqlTime)(&p.LastScanAttempt), &p.ConsecutiveFailures, &p.FailureRate, &p.SuccessfulScans)
	return
}
