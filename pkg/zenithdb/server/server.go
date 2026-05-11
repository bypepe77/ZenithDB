package server

import (
	"encoding/json"
	"net/http"

	"github.com/bypepe77/ZenithDB/pkg/zenithdb"
)

type Options struct {
	Token        string
	SchemaSource string
}

type Server struct {
	db      *zenithdb.DB
	options Options
	mux     *http.ServeMux
}

func New(db *zenithdb.DB, options Options) *Server {
	server := &Server{
		db:      db,
		options: options,
		mux:     http.NewServeMux(),
	}
	server.routes()
	return server
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mux.ServeHTTP(w, r)
}

func (s *Server) routes() {
	s.mux.HandleFunc("GET /health", s.handleHealth)
	s.mux.HandleFunc("POST /v1/checkpoint", s.withAuth(s.handleCheckpoint))
	s.mux.HandleFunc("GET /v1/schema", s.withAuth(s.handleGetSchema))
	s.mux.HandleFunc("POST /v1/schema/validate", s.withAuth(s.handleValidateSchema))
}

func (s *Server) withAuth(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if s.options.Token != "" && r.Header.Get("Authorization") != "Bearer "+s.options.Token {
			writeError(w, http.StatusUnauthorized, "unauthorized")
			return
		}
		next(w, r)
	}
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) handleCheckpoint(w http.ResponseWriter, r *http.Request) {
	if err := s.db.Checkpoint(r.Context()); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) handleGetSchema(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, schemaResponse{Schema: s.options.SchemaSource})
}

func (s *Server) handleValidateSchema(w http.ResponseWriter, r *http.Request) {
	var request schemaRequest
	if !decodeRequest(w, r, &request) {
		return
	}
	if s.options.SchemaSource != "" && request.Schema != s.options.SchemaSource {
		writeError(w, http.StatusConflict, "remote schema differs from submitted schema")
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

type schemaRequest struct {
	Schema string `json:"schema"`
}

type schemaResponse struct {
	Schema string `json:"schema"`
}

type errorResponse struct {
	Error string `json:"error"`
}

func decodeRequest(w http.ResponseWriter, r *http.Request, value any) bool {
	decoder := json.NewDecoder(r.Body)
	decoder.UseNumber()
	if err := decoder.Decode(value); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return false
	}
	return true
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, errorResponse{Error: message})
}
