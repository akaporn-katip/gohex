// Package httpapi is ordering's driving adapter: the public REST edge —
// the only synchronous surface in the system (ADR-0008).
package httpapi

import (
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"

	"github.com/akaporn-katip/gohex/libs/cqrs"
	"github.com/akaporn-katip/gohex/libs/eventstore"
	"github.com/akaporn-katip/gohex/libs/kernel"
	"github.com/akaporn-katip/gohex/services/ordering/internal/app"
	"github.com/akaporn-katip/gohex/services/ordering/internal/domain"
	"github.com/akaporn-katip/gohex/services/ordering/internal/ports"
)

// New builds the HTTP handler. Every request runs inside an otelhttp
// root span — the start of the trace that the o11y weave carries through
// the store, the broker, and the other services.
func New(bus *cqrs.Bus, queries *cqrs.QueryBus) http.Handler {
	s := &server{bus: bus, queries: queries}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /orders", s.placeOrder)
	mux.HandleFunc("GET /orders/{id}", s.getOrder)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	return otelhttp.NewHandler(mux, "ordering")
}

type server struct {
	bus     *cqrs.Bus
	queries *cqrs.QueryBus
}

type placeOrderRequest struct {
	CustomerID string `json:"customer_id"` // optional; generated when empty
	Cents      int64  `json:"cents"`
	Currency   string `json:"currency"`
	Qty        int    `json:"qty"`
}

// placeOrder accepts the command and returns 202 immediately: the saga
// carries the flow from here, and GET /orders/{id} shows it progressing.
func (s *server) placeOrder(w http.ResponseWriter, r *http.Request) {
	var req placeOrderRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"code": "invalid_json", "message": err.Error()})
		return
	}

	// Parse, don't validate: the request only becomes a command if every
	// value object exists (ADR-0011).
	customer := kernel.NewID[domain.Customer]()
	if req.CustomerID != "" {
		parsed, err := kernel.ParseID[domain.Customer](req.CustomerID)
		if err != nil {
			writeError(w, err)
			return
		}
		customer = parsed
	}
	total, err := domain.NewMoney(req.Cents, req.Currency)
	if err != nil {
		writeError(w, err)
		return
	}
	qty, err := domain.NewQuantity(req.Qty)
	if err != nil {
		writeError(w, err)
		return
	}

	id := kernel.NewID[domain.Order]()
	cmd := app.PlaceOrder{ID: id, Customer: customer, Total: total, Qty: qty}
	if err := s.bus.Dispatch(r.Context(), cmd); err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]string{"order_id": id.String()})
}

func (s *server) getOrder(w http.ResponseWriter, r *http.Request) {
	summary, err := cqrs.Ask[ports.OrderSummary](r.Context(), s.queries, app.GetOrder{OrderID: r.PathValue("id")})
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, summary)
}

// writeError maps errors per ADR-0012: a DomainError is a definitive
// 4xx with its code in the body; anything else is a 500.
func writeError(w http.ResponseWriter, err error) {
	if de, ok := kernel.AsDomainError(err); ok {
		status := http.StatusUnprocessableEntity
		if errors.Is(err, ports.ErrOrderNotFound) || errors.Is(err, eventstore.ErrNotFound) {
			status = http.StatusNotFound
		}
		writeJSON(w, status, map[string]string{"code": de.Code, "message": de.Message})
		return
	}
	slog.Error("request failed", "error", err)
	writeJSON(w, http.StatusInternalServerError, map[string]string{"code": "internal", "message": "internal error"})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
