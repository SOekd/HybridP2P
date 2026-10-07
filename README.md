# HybridP2P

Protótipo de uma CDN híbrida feita com libp2p. Os arquivos circulam primeiro entre peers (Bitswap). Se não houver peers ou a transferência falhar, o cliente baixa da CDN normal por HTTPS.

Serve de base para uma comparação entre esse modelo e o cliente-servidor tradicional.

## O que precisa ter instalado

- Go 1.25 ou mais novo. As versões das libs estão no `go.mod` (`go-libp2p` 0.45.0, `go-libp2p-kad-dht` 0.35.1, `boxo` 0.35.2).
- MySQL, e Redis conforme o `configs/tracker.yaml`, para o tracker.
- Docker e Docker Compose, se quiser subir os seeders com o `docker-compose.yml`.
- Uma URL HTTPS servindo o arquivo de teste atrás de uma CDN (no artigo, Cloudflare). Ela precisa aceitar `Range`, porque o fallback usa requisições parciais.

## Build

```bash
./build.sh        # no Windows: build.bat
```

Os binários vão para `bin/`: `tracker`, `p2pcdn-daemon`, `p2pcdn` e `benchmark`, para Linux e Windows.

## Reproduzindo os experimentos

Os dois modos baixam o mesmo arquivo: 100 MB de dados aleatórios.

### 1. Arquivo de teste

```bash
head -c 104857600 /dev/urandom > test-file.bin
sha256sum test-file.bin   # esse hash é usado para registrar e baixar o arquivo
```

Publique o `test-file.bin` na origem da CDN. Ele serve tanto para o fallback quanto para o modo cliente-servidor.

### 2. Tracker

1. Crie o banco MySQL e ajuste `configs/tracker.yaml` (porta, credenciais, Redis). As tabelas são criadas na primeira execução.
2. Rode `./bin/tracker --config configs/tracker.yaml`. Também dá para usar o `Dockerfile`.

O tracker tem um console com `bandwidth <valor>`, `clear metrics`, `daemons`, `status` e outros. O `help` mostra todos.

### 3. Seeders

1. Copie o binário Linux para `deployments/p2pcdn-daemon` e o arquivo de teste para `deployments/seed/`.
2. Preencha `P2PCDN_TRACKER_URL` e `P2PCDN_P2P_EXTERNAL_IP` em `deployments/.env.benchmark`.
3. Suba os contêineres: `docker compose -f deployments/docker-compose.yml up -d`.
4. Nos seeders que vão participar, abra o REPL (`docker attach p2pcdn-seeder-N`) e rode `seed /app/seed/test-file.bin fallback <URL-da-CDN>`. Os outros ficam parados.

Se a porta P2P estiver bloqueada, rode `scripts/open-firewall.sh`.

### 4. Modo P2P híbrido

Repita para cada combinação de seeders (0, 1, 2, 4, 8) e banda de upload (10, 50, 100, 200 Mbps):

1. No console do tracker: `bandwidth 10mbps` (ou 50, 100, 200).
2. Deixe só o número desejado de seeders semeando.
3. No cliente, rode `bash scripts/benchmark.sh`. O script zera as métricas (`p2pcdn metrics reset`), faz 50 downloads com `--no-seed` e copia o resultado para `metrics_result.json`.
4. Salve o arquivo como `data/peer-to-peer/<N> seeds/metrics_<banda>mb.json`.

Cuidado: se as métricas não forem zeradas entre as condições, os registros da anterior continuam no arquivo (isso aconteceu uma vez, veja as observações abaixo).

### 5. Modo cliente-servidor

```bash
./bin/benchmark --url https://<sua-cdn>/test-file.bin --runs 55 --save client-server.json
```

`--limit` limita a banda de download, mas no artigo a referência rodou sem limite.

## Dados

- `data/client-server.json`: 55 execuções do modo cliente-servidor.
- `data/peer-to-peer/<N> seeds/metrics_<B>mb.json`: execuções P2P com N seeders e B Mbps de banda. O `mb` no nome é Mbps, não o tamanho do arquivo (que é sempre 100 MB).

Cada registro tem, entre outros campos, `throughput_mbps`, `latency_ms`, `duration_seconds`, `time_to_first_byte_seconds`, `connection_time_ms`, `chunks_from_p2p`, `chunks_from_http`, `peers_connected`, `success` e `error_message`.

Sobre os dados brutos:

- Em `8 seeds/metrics_200mb.json`, os 50 primeiros registros são cópias da rodada de 8 seeders a 100 Mbps (mesmos timestamps), porque as métricas não foram zeradas entre as duas. A análise do artigo usa só os registros de 51 a 100.
- Três execuções falharam por timeout na checagem de `Range` na CDN (`success: false`): 4 seeders a 100 Mbps, 8 a 10 Mbps e 8 a 50 Mbps. Foram refeitas, então todas as condições têm 50 execuções bem-sucedidas.
