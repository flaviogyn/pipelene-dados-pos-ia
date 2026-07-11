# Instalação do Docker na VPS Ubuntu (DigitalOcean)

Este diretório contém um script pronto para instalar Docker e o plugin Docker Compose em uma VPS Ubuntu.

## Como usar

1. Conecte-se à sua VPS como usuário com permissões de sudo.
2. Envie este diretório para a VPS ou clone o repositório.
3. Execute:

```bash
chmod +x docker/install-docker-ubuntu.sh
sudo ./docker/install-docker-ubuntu.sh
```

## Verificação

Após a instalação, valide com:

```bash
docker --version
docker compose version
```

## Próximo passo

Com o Docker instalado, você pode usar os arquivos Docker Compose já presentes em outras pastas do projeto.

## Observação

Se o comando `docker` exigir sudo, faça logout/login ou execute:

```bash
newgrp docker
```
