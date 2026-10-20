package base

import (
	"fmt"
	"net/mail"
	"slices"
	"strings"
)

func NormalizarInteresse(i InteressePromocaoPayload) (InteressePromocaoPayload, error) {
	i.Email = strings.ToLower(strings.TrimSpace(i.Email))
	a, err := mail.ParseAddress(i.Email)
	if err != nil || a.Address != i.Email || !strings.Contains(i.Email, "@") {
		return i, fmt.Errorf("email inválido")
	}
	if i.Versao <= 0 || (i.Acao != "cadastrar" && i.Acao != "cancelar") {
		return i, fmt.Errorf("acao ou versao inválida")
	}
	i.Categorias = slices.Clone(i.Categorias)
	for n, c := range i.Categorias {
		c = strings.ToUpper(strings.TrimSpace(c))
		if c != "A" && c != "B" && c != "C" {
			return i, fmt.Errorf("categoria deve ser A, B ou C")
		}
		i.Categorias[n] = c
	}
	slices.Sort(i.Categorias)
	i.Categorias = slices.Compact(i.Categorias)
	if len(i.Categorias) == 0 {
		i.Categorias = nil
		if i.Acao == "cadastrar" {
			return i, fmt.Errorf("informe categorias")
		}
	}
	return i, nil
}
