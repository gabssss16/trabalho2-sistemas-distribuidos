package main

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"log"
	"os"
	"path/filepath"
)

func main() {
	// Apenas os serviços que publicam eventos precisam gerar chaves
	produtores := []string{"principal", "estoque", "pagamento", "entrega", "promocoes"}
	
	chavesPrivadas := make(map[string][]byte)
	chavesPublicas := make(map[string][]byte)

	log.Println("Gerando chaves matemáticas (RSA 2048 bits)...")

	// Gera o par de chaves para cada produtor
	for _, p := range produtores {
		chave, err := rsa.GenerateKey(rand.Reader, 2048)
		if err != nil {
			log.Fatalf("Erro ao gerar chave para %s: %v", p, err)
		}
		
		// Converte a chave privada para o formato exigido (PKCS#8)
		privadaBytes, _ := x509.MarshalPKCS8PrivateKey(chave)
		chavesPrivadas[p] = pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: privadaBytes})
		
		// Converte a chave pública para o formato padrão (PKIX)
		publicaBytes, _ := x509.MarshalPKIXPublicKey(&chave.PublicKey)
		chavesPublicas[p] = pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: publicaBytes})
	}

	// Mapeia o nome interno para o nome real da pasta no seu projeto
	diretorios := map[string]string{
		"principal":     "principal",
		"estoque":       "estoque",
		"pagamento":     "pagamento",
		"entrega":       "entrega",
		"promocoes":     "promocoes",
		"consumidor-c1": "consumidor-c1",
		"consumidor-c2": "consumidor-c2",
	}

	// 3. Distribui os arquivos fisicamente
	for nomeInterno, nomePasta := range diretorios {
		caminhoKeys := filepath.Join(nomePasta, "keys")
		os.MkdirAll(caminhoKeys, 0755)

		// Salva a própria chave privada (se este serviço for um produtor)
		if privBytes, existe := chavesPrivadas[nomeInterno]; existe {
			caminhoPrivada := filepath.Join(caminhoKeys, nomeInterno+"_private.pem")
			os.WriteFile(caminhoPrivada, privBytes, 0600)
		}

		// Copia as chaves públicas de TODOS os produtores para a pasta atual
		for pubNome, pubBytes := range chavesPublicas {
			caminhoPublica := filepath.Join(caminhoKeys, pubNome+"_public.pem")
			os.WriteFile(caminhoPublica, pubBytes, 0644)
		}
	}
	
	log.Println("Pastas 'keys' criadas e chaves distribuídas com sucesso!")
}