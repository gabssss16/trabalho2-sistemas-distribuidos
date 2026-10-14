package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/mail"
	"strings"
	"time"
)

type clienteEmail struct {
	apiKey, endpoint string
	http             *http.Client
}

func novoClienteEmail(apiKey, remetente string) (*clienteEmail, error) {
	apiKey = strings.TrimSpace(apiKey)
	if apiKey == "" {
		return nil, fmt.Errorf("configure RESEND_API_KEY para enviar promoções")
	}
	if _, err := mail.ParseAddress(remetente); err != nil {
		return nil, fmt.Errorf("configure PROMOCOES_EMAIL_FROM com um remetente autorizado no Resend")
	}
	return &clienteEmail{apiKey: apiKey, endpoint: "https://api.resend.com/emails", http: &http.Client{Timeout: 10 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}}, nil
}

func (c *clienteEmail) enviar(ctx context.Context, n notificacao) error {
	body, err := json.Marshal(n.Email)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.endpoint, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+c.apiKey)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Idempotency-Key", "promocao/"+n.ID)
	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("API de e-mail indisponível: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		// Não registra corpo/credenciais do provedor no log.
		return fmt.Errorf("API de e-mail retornou HTTP %d", resp.StatusCode)
	}
	var resultado struct {
		ID string `json:"id"`
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&resultado); err != nil || resultado.ID == "" {
		return fmt.Errorf("API de e-mail não confirmou identificador do envio")
	}
	return nil
}
