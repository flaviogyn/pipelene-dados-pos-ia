# Instalacao do Docker na VPS Ubuntu

Este diretorio contem um script para instalar o Docker Engine, a CLI, o runtime containerd e os plugins Buildx e Compose em uma VPS Ubuntu.

## Requisitos

- Sistema Ubuntu
- Acesso como root ou usuario com permissao de sudo
- Conexao SSH para copiar e executar o script na VPS

## Copiar o script do Windows para a VPS

No PowerShell, execute a partir da raiz do projeto:

```powershell
scp .\docker\install-docker-ubuntu.sh root@<IP>:/root/install-docker-ubuntu.sh
```

Substitua `<IP>` pelo IP da VPS.

## Executar como root

Depois de copiar o arquivo, conecte-se na VPS:

```bash
ssh root@<IP>
```

Na VPS, execute:

```bash
chmod +x /root/install-docker-ubuntu.sh
/root/install-docker-ubuntu.sh
```

Quando o script roda diretamente como root, nenhum usuario comum e adicionado ao grupo `docker`.

## Executar com sudo

Se voce estiver usando um usuario comum com sudo, copie ou clone o projeto na VPS e execute:

```bash
chmod +x docker/install-docker-ubuntu.sh
sudo ./docker/install-docker-ubuntu.sh
```

Nesse caso, o usuario que executou o `sudo` sera adicionado ao grupo `docker`.

## O que o script faz

- Verifica se o sistema e Ubuntu.
- Instala os pacotes necessarios para a instalacao do Docker.
- Configura o repositorio oficial do Docker.
- Instala Docker CE, Docker CLI, containerd, Buildx e Docker Compose Plugin.
- Habilita e inicia o servico do Docker.
- Adiciona o usuario do `sudo` ao grupo `docker`, quando existir.
- Habilita o firewall UFW e libera a porta 22, quando disponivel.

## Verificacao

Apos a instalacao, valide com:

```bash
docker --version
docker compose version
```

## Observacao sobre permissao

Se o comando `docker` continuar exigindo sudo para um usuario comum, faca logout/login ou execute:

```bash
newgrp docker
```

## Observacao sobre final de linha

O arquivo precisa estar com final de linha Linux (`LF`). Se ao executar aparecer erro parecido com `bash\r`, corrija na VPS com:

```bash
sed -i 's/\r$//' /root/install-docker-ubuntu.sh
```
