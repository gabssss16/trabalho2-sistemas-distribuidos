package persistencia

import (
	"encoding/json"
	"os"
	"path/filepath"
)

// Salvar substitui o arquivo somente depois que o novo estado foi gravado.
// O serviço deve serializar os acessos e usar um arquivo exclusivo por instância.
func Salvar(caminho string, estado any) error {
	conteudo, err := json.MarshalIndent(estado, "", "  ")
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(caminho), 0700); err != nil {
		return err
	}
	f, err := os.CreateTemp(filepath.Dir(caminho), ".estado-*")
	if err != nil {
		return err
	}
	defer os.Remove(f.Name())
	if _, err = f.Write(conteudo); err != nil {
		f.Close()
		return err
	}
	if err = f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err = f.Close(); err != nil {
		return err
	}
	return os.Rename(f.Name(), caminho)
}
