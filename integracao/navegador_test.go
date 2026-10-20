package integracao

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"
)

// Opcional: servidor WebDriver separado. Nunca utiliza o perfil pessoal.
func testarNavegador(t *testing.T, api string) {
	driver := os.Getenv("TEST_WEBDRIVER_URL")
	if driver == "" {
		t.Skip("configure TEST_WEBDRIVER_URL para o ensaio no navegador")
	}
	client := &http.Client{Timeout: 60 * time.Second}
	session := ""
	call := func(method, path string, data any) (any, error) {
		b, _ := json.Marshal(data)
		req, err := http.NewRequest(method, strings.TrimRight(driver, "/")+path, bytes.NewReader(b))
		if err != nil {
			return nil, err
		}
		req.Header.Set("Content-Type", "application/json")
		resp, err := client.Do(req)
		if err != nil {
			return nil, err
		}
		defer resp.Body.Close()
		raw, _ := io.ReadAll(resp.Body)
		var v struct {
			Value any `json:"value"`
		}
		if err := json.Unmarshal(raw, &v); err != nil {
			return nil, err
		}
		if resp.StatusCode >= 400 {
			return nil, fmt.Errorf("webdriver %d: %s", resp.StatusCode, raw)
		}
		return v.Value, nil
	}
	v, err := call("POST", "/session", map[string]any{"capabilities": map[string]any{"alwaysMatch": map[string]any{"browserName": "firefox", "moz:firefoxOptions": map[string]any{"args": []string{"-headless"}}}}})
	if err != nil {
		t.Fatal(err)
	}
	session = v.(map[string]any)["sessionId"].(string)
	baseURL := "/session/" + session
	defer call("DELETE", baseURL, nil)
	if _, err := call("POST", baseURL+"/url", map[string]string{"url": api}); err != nil {
		t.Fatal(err)
	}
	script := func(js string) (any, error) {
		return call("POST", baseURL+"/execute/sync", map[string]any{"script": js, "args": []any{}})
	}
	esperar := func(desc, js string) {
		t.Helper()
		deadline := time.Now().Add(15 * time.Second)
		for time.Now().Before(deadline) {
			v, err := script(js)
			if err == nil && v == true {
				return
			}
			time.Sleep(100 * time.Millisecond)
		}
		v, _ := script("return document.body.innerText")
		t.Fatalf("navegador: %s; tela=%v", desc, v)
	}
	esperar("catalogo", "return document.querySelectorAll('.produto button').length > 0")
	// Clique WebDriver gera gesto de usuario real para a abertura da nova aba.
	clicar := func(selector string) {
		t.Helper()
		el, err := call("POST", baseURL+"/element", map[string]string{"using": "css selector", "value": selector})
		if err != nil {
			t.Fatal(err)
		}
		id := el.(map[string]any)["element-6066-11e4-a52e-4f735466cecf"].(string)
		if _, err := call("POST", baseURL+"/element/"+id+"/click", map[string]any{}); err != nil {
			t.Fatal(err)
		}
	}
	clicar(".produto button")
	esperar("link de checkout", "return !!document.querySelector('#area-pagamento a')")
	v, err = call("GET", baseURL+"/window", nil)
	if err != nil {
		t.Fatal(err)
	}
	loja := v.(string)
	clicar("#area-pagamento a")
	var checkout string
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		v, err = call("GET", baseURL+"/window/handles", nil)
		if err != nil {
			t.Fatal(err)
		}
		for _, h := range v.([]any) {
			if h.(string) != loja {
				checkout = h.(string)
			}
		}
		if checkout != "" {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if checkout == "" {
		t.Fatal("checkout nao abriu nova aba")
	}
	if _, err := call("POST", baseURL+"/window", map[string]string{"handle": checkout}); err != nil {
		t.Fatal(err)
	}
	esperar("botoes do mock", "return !!document.querySelector('button[value=APROVADO]')")
	clicar("button[value=APROVADO]")
	if _, err := call("POST", baseURL+"/window", map[string]string{"handle": loja}); err != nil {
		t.Fatal(err)
	}
	esperar("SSE pedido enviado", "return document.querySelector('#status-texto').textContent === 'pedido.enviado'")
	t.Log("Firefox: catalogo, compra REST, checkout em nova aba e SSE final aprovados")
}
