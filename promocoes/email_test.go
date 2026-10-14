package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sync"
	"testing"
)

func TestContratoResendComRetentativaPersistida(t *testing.T) {
	var mu sync.Mutex
	var chave string
	var mensagem mensagemEmail
	chamadas := 0
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		chamadas++
		if r.Method != "POST" || r.URL.Path != "/emails" || r.Header.Get("Authorization") != "Bearer chave-teste" || r.Header.Get("Content-Type") != "application/json" {
			t.Error("contrato HTTP inválido")
		}
		var atual mensagemEmail
		if err := json.NewDecoder(r.Body).Decode(&atual); err != nil {
			t.Error(err)
		}
		if chamadas == 1 {
			chave, mensagem = r.Header.Get("Idempotency-Key"), atual
			w.WriteHeader(429)
			return
		}
		if chave == "" || r.Header.Get("Idempotency-Key") != chave || !reflect.DeepEqual(atual, mensagem) {
			t.Error("repetição alterou corpo ou chave")
		}
		fmt.Fprint(w, `{"id":"email-123"}`)
	}))
	defer api.Close()
	cliente, err := novoClienteEmail("chave-teste", "Loja <loja@example.com>")
	if err != nil {
		t.Fatal(err)
	}
	cliente.endpoint = api.URL + "/emails"
	s := novoServico(t)
	s.enviar = cliente.enviar
	atualizar(t, s, interesse("ana@example.com", "cadastrar", 1, "A"))
	atualizar(t, s, interesse("bia@example.com", "cadastrar", 1, "B"))
	agendar(t, s, "A")
	s.processarPendentes(context.Background())
	if len(s.estado.Pendentes) != 1 {
		t.Fatal("perdeu notificação após limite da API")
	}
	reiniciado, err := abrirPromocoes(s.caminho, s.remetente)
	if err != nil {
		t.Fatal(err)
	}
	reiniciado.enviar = cliente.enviar
	reiniciado.processarPendentes(context.Background())
	reiniciado.processarPendentes(context.Background())
	mu.Lock()
	defer mu.Unlock()
	if chamadas != 2 || !reflect.DeepEqual(mensagem.To, []string{"ana@example.com"}) || mensagem.Subject == "" || mensagem.Text == "" || len(reiniciado.estado.Pendentes) != 0 {
		t.Fatal("envio não concluído corretamente")
	}
}

func TestRespostaInvalidaNaoConfirmaEnvio(t *testing.T) {
	for _, caso := range []struct {
		status int
		body   string
	}{{401, `{}`}, {500, `{}`}, {200, `{}`}, {200, `invalido`}} {
		t.Run(fmt.Sprint(caso.status, caso.body), func(t *testing.T) {
			api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(caso.status); fmt.Fprint(w, caso.body) }))
			defer api.Close()
			c, err := novoClienteEmail("teste", "loja@example.com")
			if err != nil {
				t.Fatal(err)
			}
			c.endpoint = api.URL
			if err := c.enviar(context.Background(), notificacao{ID: "teste"}); err == nil {
				t.Fatal("confirmou resposta inválida")
			}
		})
	}
}

func TestConfiguracaoObrigatoriaERedirecionamento(t *testing.T) {
	if _, err := novoClienteEmail("", "loja@example.com"); err == nil {
		t.Fatal("aceitou chave vazia")
	}
	if _, err := novoClienteEmail("teste", "invalido"); err == nil {
		t.Fatal("aceitou remetente inválido")
	}
	destino := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { t.Error("seguiu redirecionamento"); w.WriteHeader(200) }))
	defer destino.Close()
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { http.Redirect(w, r, destino.URL, 307) }))
	defer api.Close()
	c, err := novoClienteEmail("teste", "loja@example.com")
	if err != nil {
		t.Fatal(err)
	}
	c.endpoint = api.URL
	if err := c.enviar(context.Background(), notificacao{ID: "teste"}); err == nil {
		t.Fatal("aceitou redirect")
	}
}
