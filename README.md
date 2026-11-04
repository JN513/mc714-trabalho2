# mc714-trabalho2# MC714 - Sistemas Distribuídos | Trabalho 2 (2º Semestre de 2026)

**Instituto de Computação (IC) — UNICAMP**  
**Professor:** Carlos Alberto Astudillo Trujillo  
**PED:** Diogo Maciel da Cunha  

---

## Visão Geral

Este repositório contém a implementação e avaliação experimental de dois algoritmos distribuídos clássicos complementares:

1. **Algoritmo de Eleição de Líder (Bully Algorithm / Algoritmo do Valentão):** Garante a escolha determinística do nó ativo de maior ID como coordenador do cluster.
2. **Detector de Falhas por Heartbeat:** Monitora periodicamente a vivacidade do líder e aciona o processo de eleição automaticamente em caso de estagnação ou queda (*crash-stop*).

O ambiente de execução é virtualizado utilizando **Docker Compose v2**, garantindo isolamento de rede e executabilidade independente de plataforma.

---

## Estrutura do Repositório

```text
.
├── compose.yml                 # Configuração dos contêineres e subrede Docker
├── pytest.ini                  # Configuração de escopo assíncrono do Pytest
├── README.md                   # Documentação do projeto
├── requirements.txt            # Dependências do projeto
├── Trabalho2-MC714_2s2026.pdf  # Enunciado da avaliação
├── report.pdf                  # Relatório técnico
├── src/
│   ├── Dockerfile              # Imagem OCI para os nós distribuídos
│   └── node/
│       ├── main.py             # Ponto de entrada e roteamento de mensagens
│       ├── network.py          # Camada de comunicação assíncrona (TCP/IP)
│       ├── alg1_bully.py       # Lógica do Algoritmo do Valentão (Bully)
│       └── alg2_heartbeat.py   # Lógica do Detector de Falhas por Heartbeat
├── tests/
│   ├── test_alg1.py            # Testes unitários do algoritmo Bully (pytest)
│   └── test_alg2.py            # Testes unitários do detector de Heartbeat (pytest)
└── experiments/
    ├── inject_faults.sh        # Script de injeção de falhas (crash-stop)
    ├── run_benchmarks.sh       # Automador de bateria de testes/experimentos
    └── data/                   # Logs brutos e arquivos CSV de resultados
```

---

## Pré-requisitos

Para executar e testar este projeto localmente, certifique-se de possuir instalado em sua máquina:

* **Docker Engine** (v20.10+) e **Docker Compose** (v2.0+)
* **Python 3.11+** (para execução local dos testes unitários)
* **bash**, **curl** e **pytest** (`pip install -r requirements.txt`)

---

## Como Executar o Sistema

### 1. Inicializando o Cluster com Docker Compose

Suba o ambiente virtualizado contendo 3 ou mais nós em background:

```bash
docker compose up -d --build
```

Para acompanhar os logs de comunicação em tempo real de todos os nós:

```bash
docker compose logs -f
```

### 2. Finalizando o Ambiente

Para parar e remover os contêineres e a rede virtual:

```bash
docker compose down -v
```

---

## Testes Unitários

Os testes unitários validam a corretude das transições de estado dos algoritmos de forma isolada, sem necessidade dos contêineres ativos.

Execute a suíte de testes com:

```bash
pytest tests/ -v
```

---

## Análise Experimental e Injeção de Falhas

### Injeção Manual de Falha (Crash-Stop)

O script `inject_faults.sh` desliga o contêiner do líder ativo e cronometra o tempo necessário para o cluster detectar a falha e eleger um novo líder.

```bash
# Uso: ./experiments/inject_faults.sh <NO_LIDER> <NO_MONITOR>
./experiments/inject_faults.sh node3 node2
```

### Execução de Bateria Automática de Benchmarks

Para realizar múltiplos testes independentes e armazenar os tempos de reconvergência em formato CSV:

```bash
chmod +x experiments/inject_faults.sh experiments/run_benchmarks.sh
./experiments/run_benchmarks.sh
```

Os resultados coletados serão salvos no diretório `experiments/data/benchmark_results.csv`.

---

## Modelo do Sistema e Propriedades

* **Modelo Temporal:** Parcialmente Síncrono (suposição de limiar máximo $T_{timeout}$ para detecção de inatividade do líder).
* **Canais de Comunicação:** Conexões TCP ponto a ponto (`asyncio.start_server` / `open_connection`) com garantias de entrega e ordenação FIFO por par de nós.
* **Modelo de Falhas:** *Crash-stop* e *Crash-recovery* (o nó de maior ID reassume a liderança ao reconectar-se ao cluster).
* **Propriedades de Corretude:**
  * **Safety (Segurança):** Em qualquer instante estável, existe no máximo um único nó coordenador no cluster.
  * **Liveness (Vivacidade):** Se o líder falhar, um nó vivo com o maior ID elegível acabará sendo declarado o novo coordenador.

---

## Autores e Entrega

* **Alunos:** Matheus Veiga Luchi (269494) e Julio Nunes Avelar (241163)
* **Link para o Vídeo de Demonstração:** 
* **Link para o Repositório:** 