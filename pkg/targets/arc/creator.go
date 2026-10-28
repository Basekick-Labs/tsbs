package arc

import (
	"io"
	"log"
	"net/http"
	"time"
)

// dbCreator handles database creation and management for Arc
type dbCreator struct {
	url      string
	apiKey   string
	database string
}

func (d *dbCreator) Init() {
	// Arc doesn't require explicit initialization
}

func (d *dbCreator) DBExists(dbName string) bool {
	// For Arc, we assume databases can be created on-the-fly
	// Check if we can access the database by checking the health endpoint
	client := &http.Client{
		Timeout: 5 * time.Second,
	}

	// Try to access the root API endpoint to verify connectivity
	baseURL := d.url[:len(d.url)-len("/api/v1/write/msgpack")]
	if baseURL == d.url {
		// URL didn't end with the expected path, use as-is
		baseURL = d.url
	}

	req, err := http.NewRequest("GET", baseURL+"/health", nil)
	if err != nil {
		return false
	}

	if d.apiKey != "" {
		req.Header.Set("x-api-key", d.apiKey)
	}

	resp, err := client.Do(req)
	if err != nil {
		log.Printf("Warning: could not check Arc database existence: %v", err)
		return false
	}
	defer resp.Body.Close()

	// Discard response body
	_, _ = io.Copy(io.Discard, resp.Body)

	return resp.StatusCode == http.StatusOK
}

func (d *dbCreator) CreateDB(dbName string) error {
	// Arc creates databases automatically when data is written
	// We just need to verify connectivity
	if !d.DBExists(dbName) {
		log.Printf("Warning: could not verify Arc connectivity, but proceeding anyway")
	}
	log.Printf("Arc database '%s' will be created automatically on first write", dbName)
	return nil
}

func (d *dbCreator) RemoveOldDB(dbName string) error {
	// Arc doesn't support database deletion via API
	// This would need to be done manually or via Arc's admin interface
	log.Printf("Warning: Arc does not support automatic database deletion. Database '%s' must be removed manually if needed.", dbName)
	return nil
}

func (d *dbCreator) PostCreateDB(dbName string) error {
	// No post-creation steps needed for Arc
	return nil
}
