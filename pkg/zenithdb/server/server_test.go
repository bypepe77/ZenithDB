package server_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/bypepe77/ZenithDB/pkg/zenithdb"
	"github.com/bypepe77/ZenithDB/pkg/zenithdb/server"
)

func TestHTTPControlPlaneSchema(t *testing.T) {
	ctx := context.Background()
	schemaSource := `model User {
  id    String @id
  email String @unique
  name  String
}`
	db, err := zenithdb.Open(ctx, testSchema(), zenithdb.Options{})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()

	httpServer := httptest.NewServer(server.New(db, server.Options{Token: "secret", SchemaSource: schemaSource}))
	defer httpServer.Close()

	request, err := http.NewRequest(http.MethodGet, httpServer.URL+"/v1/schema", nil)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	request.Header.Set("Authorization", "Bearer secret")

	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatalf("get schema: %v", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("unexpected schema status: %s", response.Status)
	}
	var schemaBody struct {
		Schema string `json:"schema"`
	}
	if err := json.NewDecoder(response.Body).Decode(&schemaBody); err != nil {
		t.Fatalf("decode schema: %v", err)
	}
	if schemaBody.Schema != schemaSource {
		t.Fatalf("unexpected schema: %q", schemaBody.Schema)
	}

	validateRequest, err := http.NewRequest(http.MethodPost, httpServer.URL+"/v1/schema/validate", bytes.NewBufferString(`{"schema":"wrong"}`))
	if err != nil {
		t.Fatalf("new validate request: %v", err)
	}
	validateRequest.Header.Set("Authorization", "Bearer secret")
	validateRequest.Header.Set("Content-Type", "application/json")
	validateResponse, err := http.DefaultClient.Do(validateRequest)
	if err != nil {
		t.Fatalf("validate schema: %v", err)
	}
	defer validateResponse.Body.Close()
	if validateResponse.StatusCode != http.StatusConflict {
		t.Fatalf("unexpected validate status: %s", validateResponse.Status)
	}
}

func testSchema() zenithdb.Schema {
	return zenithdb.Schema{
		Models: []zenithdb.Model{
			{
				Name: "User",
				Fields: []zenithdb.Field{
					{Name: "id", Kind: zenithdb.FieldString, Required: true},
					{Name: "email", Kind: zenithdb.FieldString, Required: true},
					{Name: "name", Kind: zenithdb.FieldString, Required: true},
				},
				PrimaryKey: []string{"id"},
				Indexes: []zenithdb.Index{
					{Name: "user_email_unique", Fields: []string{"email"}, Unique: true},
				},
			},
		},
	}
}
