package base

import (
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"os"
)

// hashEvento inclui tipo, produtor e payload, mas não a própria assinatura.
func hashEvento(evento Event) ([32]byte, error) {
	evento.Signature = ""
	conteudo, err := json.Marshal(evento)
	if err != nil {
		return [32]byte{}, err
	}
	return sha256.Sum256(conteudo), nil
}

func lerChave(caminho string) ([]byte, error) {
	conteudo, err := os.ReadFile(caminho)
	if err != nil {
		return nil, err
	}
	bloco, _ := pem.Decode(conteudo)
	if bloco == nil {
		return nil, fmt.Errorf("chave PEM inválida: %s", caminho)
	}
	return bloco.Bytes, nil
}

// AssinarEvento usa uma chave privada RSA em PEM/PKCS#8.
func AssinarEvento(evento *Event, caminho string) error {
	der, err := lerChave(caminho)
	if err != nil {
		return err
	}
	chave, err := x509.ParsePKCS8PrivateKey(der)
	if err != nil {
		return err
	}
	privada, ok := chave.(*rsa.PrivateKey)
	if !ok {
		return fmt.Errorf("a chave privada deve ser RSA")
	}
	hash, err := hashEvento(*evento)
	if err != nil {
		return err
	}
	assinatura, err := rsa.SignPKCS1v15(rand.Reader, privada, crypto.SHA256, hash[:])
	if err != nil {
		return err
	}
	evento.Signature = base64.StdEncoding.EncodeToString(assinatura)
	return nil
}

// ValidarAssinatura usa a chave pública RSA do produtor em PEM/PKIX.
func ValidarAssinatura(evento Event, caminho string) (bool, error) {
	der, err := lerChave(caminho)
	if err != nil {
		return false, err
	}
	chave, err := x509.ParsePKIXPublicKey(der)
	if err != nil {
		return false, err
	}
	publica, ok := chave.(*rsa.PublicKey)
	if !ok {
		return false, fmt.Errorf("a chave pública deve ser RSA")
	}
	assinatura, err := base64.StdEncoding.DecodeString(evento.Signature)
	if err != nil {
		return false, err
	}
	hash, err := hashEvento(evento)
	if err != nil {
		return false, err
	}
	return rsa.VerifyPKCS1v15(publica, crypto.SHA256, hash[:], assinatura) == nil, nil
}
