package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
	"time"

	"trabalho2-sistemas-distribuidos/base"
	"trabalho2-sistemas-distribuidos/base/rabbitmq"
	"trabalho2-sistemas-distribuidos/internal/testutil"
)

func novoServico(t *testing.T) *servicoPromocoes {
	t.Helper()
	s, err := abrirPromocoes(filepath.Join(t.TempDir(), "promocoes.json"), "Loja <loja@example.com>")
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func interesse(email, acao string, versao int64, categorias ...string) base.InteressePromocaoPayload {
	return base.InteressePromocaoPayload{Email: email, Acao: acao, Versao: versao, Categorias: categorias}
}

func atualizar(t *testing.T, s *servicoPromocoes, i base.InteressePromocaoPayload) {
	t.Helper()
	if err := s.atualizarInteresse(i); err != nil {
		t.Fatal(err)
	}
}

func agendar(t *testing.T, s *servicoPromocoes, categoria string) {
	t.Helper()
	if err := s.agendar(Promocao{Produto: "Jogo de teste", Desconto: 25, Categoria: categoria}); err != nil {
		t.Fatal(err)
	}
}

func TestInscricoesPersistenciaCategoriasECancelamento(t *testing.T) {
	s := novoServico(t)
	atualizar(t, s, interesse(" Ana@Example.com ", "cadastrar", 1, "b", "A", "A"))
	atualizar(t, s, interesse("bia@example.com", "cadastrar", 1, "C"))
	if got := s.estado.Inscricoes["ana@example.com"].Categorias; !reflect.DeepEqual(got, []string{"A", "B"}) {
		t.Fatal(got)
	}
	agendar(t, s, "A")
	agendar(t, s, "C")
	if len(s.estado.Pendentes) != 2 {
		t.Fatal("não filtrou interesses")
	}
	atualizar(t, s, interesse("ana@example.com", "cancelar", 2, "A"))
	if len(s.estado.Pendentes) != 1 || !reflect.DeepEqual(s.estado.Inscricoes["ana@example.com"].Categorias, []string{"B"}) {
		t.Fatal("cancelamento parcial não removeu pendências")
	}
	reiniciado, err := abrirPromocoes(s.caminho, s.remetente)
	if err != nil {
		t.Fatal(err)
	}
	var destinatarios []string
	reiniciado.enviar = func(_ context.Context, n notificacao) error {
		destinatarios = append(destinatarios, n.Email.To[0])
		return nil
	}
	reiniciado.processarPendentes(context.Background())
	if !reflect.DeepEqual(destinatarios, []string{"bia@example.com"}) {
		t.Fatal(destinatarios)
	}
	atualizar(t, reiniciado, interesse("ana@example.com", "cancelar", 3))
	agendar(t, reiniciado, "B")
	if len(reiniciado.estado.Pendentes) != 0 {
		t.Fatal("agendou após cancelamento total")
	}
}

func TestDuplicatasVersoesAntigasEReinscricao(t *testing.T) {
	s := novoServico(t)
	cadastro := interesse("ana@example.com", "cadastrar", 1, "A")
	atualizar(t, s, cadastro)
	atualizar(t, s, cadastro)
	if err := s.atualizarInteresse(interesse("ana@example.com", "cadastrar", 1, "B")); !errors.Is(err, rabbitmq.ErrEventoInvalido) {
		t.Fatal("aceitou versão conflitante")
	}
	atualizar(t, s, interesse("ana@example.com", "cancelar", 3))
	reiniciado, err := abrirPromocoes(s.caminho, s.remetente)
	if err != nil {
		t.Fatal(err)
	}
	atualizar(t, reiniciado, cadastro)
	atualizar(t, reiniciado, interesse("ana@example.com", "cadastrar", 2, "B"))
	if len(reiniciado.estado.Inscricoes["ana@example.com"].Categorias) != 0 {
		t.Fatal("replay reativou inscrição cancelada")
	}
	atualizar(t, reiniciado, interesse("ana@example.com", "cadastrar", 4, "C"))
	if !reflect.DeepEqual(reiniciado.estado.Inscricoes["ana@example.com"].Categorias, []string{"C"}) {
		t.Fatal("não aceitou novo consentimento")
	}
}

func TestFalhaDeEnvioRetomaMesmaMensagemAposReinicio(t *testing.T) {
	s := novoServico(t)
	atualizar(t, s, interesse("ana@example.com", "cadastrar", 1, "A"))
	agendar(t, s, "A")
	var primeira notificacao
	s.enviar = func(_ context.Context, n notificacao) error { primeira = n; return fmt.Errorf("API indisponível") }
	s.processarPendentes(context.Background())
	if len(s.estado.Pendentes) != 1 {
		t.Fatal("perdeu envio com falha")
	}
	reiniciado, err := abrirPromocoes(s.caminho, "Outro <outro@example.com>")
	if err != nil {
		t.Fatal(err)
	}
	chamadas := 0
	reiniciado.enviar = func(_ context.Context, n notificacao) error {
		chamadas++
		if !reflect.DeepEqual(n, primeira) {
			t.Error("alterou mensagem ou chave na repetição")
		}
		return nil
	}
	reiniciado.processarPendentes(context.Background())
	reiniciado.processarPendentes(context.Background())
	if chamadas != 1 || len(reiniciado.estado.Pendentes) != 0 {
		t.Fatal("repetiu envio confirmado")
	}
}

func TestFalhaDePersistenciaNaoAlteraMemoria(t *testing.T) {
	s := novoServico(t)
	atualizar(t, s, interesse("ana@example.com", "cadastrar", 1, "A"))
	caminho := s.caminho
	s.caminho = filepath.Join(caminho, "invalido.json")
	if err := s.atualizarInteresse(interesse("ana@example.com", "cancelar", 2)); err == nil {
		t.Fatal("ignorou falha de gravação")
	}
	if len(s.estado.Inscricoes["ana@example.com"].Categorias) != 1 {
		t.Fatal("alterou memória sem salvar")
	}
	if err := s.agendar(Promocao{Produto: "Teste", Desconto: 20, Categoria: "A"}); err == nil || len(s.estado.Pendentes) != 0 {
		t.Fatal("agendou sem persistir")
	}
	s.caminho = caminho
	agendar(t, s, "A")
	s.caminho = filepath.Join(caminho, "invalido.json")
	s.enviar = func(context.Context, notificacao) error { return nil }
	s.processarPendentes(context.Background())
	if len(s.estado.Pendentes) != 1 {
		t.Fatal("perdeu chave de repetição após falha de confirmação")
	}
}

func TestCancelamentoEExpiracaoImpedemReenvio(t *testing.T) {
	s := novoServico(t)
	atualizar(t, s, interesse("ana@example.com", "cadastrar", 1, "A"))
	agendar(t, s, "A")
	var id string
	for chave := range s.estado.Pendentes {
		id = chave
	}
	atualizar(t, s, interesse("ana@example.com", "cancelar", 2))
	s.enviar = func(context.Context, notificacao) error { t.Fatal("enviou cancelado ou expirado"); return nil }
	if err := s.enviarPendente(context.Background(), id); err != nil {
		t.Fatal(err)
	}
	atualizar(t, s, interesse("ana@example.com", "cadastrar", 3, "A"))
	agendar(t, s, "A")
	for id, n := range s.estado.Pendentes {
		n.ExpiraEm = time.Now().Add(-time.Minute)
		s.estado.Pendentes[id] = n
	}
	s.processarPendentes(context.Background())
	if len(s.estado.Pendentes) != 0 {
		t.Fatal("manteve oferta expirada")
	}
}

func TestAtualizacoesConcorrentesConservamMaiorVersao(t *testing.T) {
	s := novoServico(t)
	var wg sync.WaitGroup
	for versao := int64(1); versao <= 20; versao++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := s.atualizarInteresse(interesse("ana@example.com", "cadastrar", versao, "A")); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	if s.estado.Inscricoes["ana@example.com"].Ultimo.Versao != 20 {
		t.Fatal("perdeu atualização mais recente")
	}
}

func TestDadosInvalidosEArquivoCorrompido(t *testing.T) {
	s := novoServico(t)
	for _, i := range []base.InteressePromocaoPayload{
		interesse("invalido", "cadastrar", 1, "A"), interesse("Nome <ana@example.com>", "cadastrar", 1, "A"),
		interesse("ana@example.com", "cadastrar", 0, "A"), interesse("ana@example.com", "outro", 1, "A"),
		interesse("ana@example.com", "cadastrar", 1), interesse("ana@example.com", "cadastrar", 1, "D"),
	} {
		if err := s.atualizarInteresse(i); !errors.Is(err, rabbitmq.ErrEventoInvalido) {
			t.Fatal("aceitou interesse inválido")
		}
	}
	for _, b := range []string{`null`, `{}`, `{`, `{"inscricoes":{},"pendentes":{"x":{}}}`} {
		path := filepath.Join(t.TempDir(), "estado.json")
		if err := os.WriteFile(path, []byte(b), 0600); err != nil {
			t.Fatal(err)
		}
		if _, err := abrirPromocoes(path, s.remetente); err == nil {
			t.Fatal("aceitou arquivo inválido")
		}
	}
}

func TestInteresseAssinadoDoPrincipal(t *testing.T) {
	evento, _ := testutil.EventoAssinado(t, "promocoes", "principal", base.InteressePromocao)
	i := interesse("ana@example.com", "cadastrar", 1, "A")
	payload, err := json.Marshal(i)
	if err != nil {
		t.Fatal(err)
	}
	evento.Payload = payload
	if err := base.AssinarEvento(&evento, "private.pem"); err != nil {
		t.Fatal(err)
	}
	got, err := lerInteresse(testutil.Delivery(t, evento))
	if err != nil || !reflect.DeepEqual(got, i) {
		t.Fatalf("evento válido: %+v %v", got, err)
	}
	for _, mudar := range []func(*base.Event){
		func(e *base.Event) { e.Producer = "estoque" },
		func(e *base.Event) { e.Type = base.PedidoCriado },
		func(e *base.Event) { e.Signature = "invalida" },
		func(e *base.Event) { e.Payload = []byte(`{}`) },
	} {
		alterado := evento
		mudar(&alterado)
		if _, err := lerInteresse(testutil.Delivery(t, alterado)); !errors.Is(err, rabbitmq.ErrEventoInvalido) {
			t.Fatal("aceitou alteração")
		}
	}
	d := testutil.Delivery(t, evento)
	d.RoutingKey = base.PedidoCriado
	if _, err := lerInteresse(d); !errors.Is(err, rabbitmq.ErrEventoInvalido) {
		t.Fatal("aceitou routing key incorreta")
	}
}
