# MC714 — Sistemas Distribuídos | Trabalho 2 (2º Semestre de 2026)

**Instituto de Computação (IC) — UNICAMP**
**Professor:** Carlos Alberto Astudillo Trujillo
**PED:** Diogo Maciel da Cunha

---

## Visão Geral

Este repositório contém a implementação e a avaliação experimental de dois
algoritmos distribuídos clássicos complementares:

1. **Eleição de Líder — Algoritmo do Valentão (Bully):** garante a escolha
   determinística do nó vivo de maior ID como coordenador do cluster.
2. **Detecção de Falhas por Heartbeat:** monitora periodicamente a vivacidade do
   líder e aciona automaticamente o processo de eleição em caso de queda
   (*crash-stop*), permitindo também *crash-recovery* (o nó de maior ID
   reassume a liderança ao reiniciar).

Como **atividade de bônus**, implementa-se um terceiro algoritmo de
**compromisso atômico — Two-Phase Commit (2PC)** — coordenado pelo líder eleito:

3. **Two-Phase Commit (2PC):** transações atômicas sobre um armazenamento
   chave-valor com travas, incluindo o tratamento da falha de participantes
   (aborto) e o clássico problema de bloqueio quando o coordenador cai entre as
   fases, mitigado por um protocolo de terminação cooperativo.

O ambiente de execução é virtualizado com **Docker Compose v2**, com cada nó
lógico rodando como um contêiner independente e comunicando-se exclusivamente
pela interface de rede da aplicação (TCP + JSON sobre `asyncio`).

---

## Estrutura do Repositório

```text
.
├── compose.yml                     # Cluster padrão de 3 nós (compose.yaml)
├── pytest.ini                      # Configuração do Pytest (modo asyncio)
├── requirements.txt                # Dependências do projeto
├── Trabalho2-MC714_2s2026.pdf      # Enunciado da avaliação
├── report.pdf                      # Relatório técnico (cópia)
├── relatorio/
│   ├── relatorio.tex               # Fonte IEEE do relatório
│   └── figures/                    # Figuras geradas pelos experimentos
├── src/
│   ├── Dockerfile                  # Imagem OCI dos nós distribuídos
│   └── node/
│       ├── main.py                 # Ponto de entrada e roteamento de mensagens
│       ├── network.py              # Comunicação assíncrona (TCP/IP + contadores)
│       ├── alg1_bully.py           # Algoritmo do Valentão (Bully)
│       ├── alg2_heartbeat.py       # Detector de Falhas por Heartbeat
│       └── alg3_2pc.py             # Bônus: Two-Phase Commit (compromisso atômico)
├── tests/
│   ├── test_alg1.py                # Testes unitários do Bully
│   ├── test_alg2.py                # Testes unitários do Heartbeat
│   ├── test_alg3.py                # Testes do 2PC (atomicidade e terminação)
│   └── test_integration.py         # Testes de segurança/vivacidade/recuperação
└── experiments/
    ├── inject_faults.sh            # Injeção manual de falha (crash-stop)
    ├── run_safety.py               # Cenário normal (segurança)
    ├── run_experiments.py          # Bateria automatizada (falha + recuperação)
    ├── run_2pc.py                  # Bateria do 2PC (vazão, latência, falhas)
    ├── analyze.py                  # Estatística e geração de figuras/tabelas
    ├── run_benchmarks.sh           # Orquestra os scripts acima
    └── data/                       # CSVs brutos e estatísticas
```

---

## Pré-requisitos

* **Docker Engine** (v20.10+) e **Docker Compose** (v2.0+)
* **Python 3.10+** (para testes e análise): `pip install -r requirements.txt`
* **bash** e **curl**

---

## Como Executar o Sistema

```bash
# Sobe o cluster de 3 nós em background
docker compose up -d --build

# Acompanha os logs de comunicação em tempo real
docker compose logs -f

# Encerra e remove contêineres e rede
docker compose down -v
```

### Parâmetros configuráveis (variáveis de ambiente por nó)

| Variável              | Padrão | Descrição                                        |
|-----------------------|--------|--------------------------------------------------|
| `NODE_ID`             | `1`    | Identificador lógico do nó (inteiro; maior = líder) |
| `PORT`                | `5000` | Porta TCP de escuta                              |
| `PEERS`               | —      | Lista `ID:HOST:PORT` dos demais nós              |
| `HEARTBEAT_INTERVAL`  | `1.0`  | Período entre heartbeats do líder (s)            |
| `HEARTBEAT_TIMEOUT`   | `3.0`  | Limiar de detecção de falha do líder (s)         |
| `ELECTION_TIMEOUT`    | `2.0`  | Espera por respostas antes de se autoproclamar (s) |
| `BOOTSTRAP_DELAY`     | `2.0`  | Atraso da eleição de inicialização/recuperação (s) |
| `VOTE_TIMEOUT`        | `2.0`  | 2PC: espera pelos votos dos participantes (s)     |
| `DECISION_TIMEOUT`    | `3.0`  | 2PC: atraso antes do protocolo de terminação (s)  |
| `CRASH_AFTER_PREPARE` | `0`    | 2PC: se `1`, o coordenador "cai" após o PREPARE (falha injetada) |
| `PARTICIPANT_VOTE_NO` | `0`    | 2PC: se `1`, o nó sempre vota `NO` (falha injetada) |

---

## Testes

```bash
pytest tests/ -v
```

* `test_alg1.py` / `test_alg2.py`: testes unitários das transições de estado.
* `test_integration.py`: rede em memória que valida **segurança** (no máximo um
  líder), **vivacidade** (novo líder quando o maior ID cai) e **crash-recovery**
  (o maior ID reassume a liderança ao reiniciar).
* `test_alg3.py`: testes do **2PC** (bônus) — travas, votos por conflito,
  atomicidade sob conflito/voto negativo e o protocolo de terminação
  cooperativo (aborto, `COMMIT` quando alguém já efetivou e bloqueio com nó
  inacessível).

A suíte completa tem **20 testes**.

---

## Análise Experimental

### Injeção manual de falha (crash-stop)

```bash
./experiments/inject_faults.sh node3 node2
```

Derruba o líder com `docker kill` (SIGKILL imediato) e mede o tempo até a
reconvergência.

> **Nota de reprodutibilidade:** os contêineres executam o Python como PID 1, que
> ignora `SIGTERM` por padrão. Por isso o script usa `docker kill` em vez de
> `docker stop`; este último aguardaria o *grace period* de 10 s antes do
> SIGKILL e inflaria artificialmente o tempo de reconvergência.

### Bateria automatizada completa

```bash
chmod +x experiments/*.sh
./experiments/run_benchmarks.sh
```

Executa, em sequência:

1. `run_safety.py` — cenário normal: confirma um único líder e ausência de
   eleições espúrias;
2. `run_experiments.py` — varia o limiar de heartbeat (`1, 2, 3, 5` s) e o
   número de nós (`3, 4, 5`) em seis ciclos de falha/recuperação por
   configuração, medindo tempo de
   reconvergência, decomposição detecção/eleição, número de mensagens e tempo de
   recuperação;
3. `run_2pc.py` — bateria do bônus 2PC: vazão/latência de transações
   concorrentes e cenários de falha de participante e de coordenador;
4. `analyze.py` — calcula média, desvio, p50/p95 e IC 95% e gera as figuras e
   tabelas do relatório em `relatorio/figures/` e `relatorio/tabelas/`.

Saídas: `experiments/data/*.csv`, `experiments/data/stats.json`, figuras e
tabelas.

---

## Modelo do Sistema e Propriedades

* **Modelo temporal:** parcialmente síncrono (limiar máximo `HEARTBEAT_TIMEOUT`
  para detecção de inatividade do líder).
* **Canais:** TCP ponto a ponto (`asyncio.start_server` / `open_connection`),
  com entrega confiável e ordenação FIFO por par de nós.
* **Modelo de falhas:** *crash-stop* e *crash-recovery*.
* **Hipóteses de relógio:** cada nó usa um relógio local para medir intervalos
  (não há sincronização de relógios físicos; os algoritmos não dependem disso).
* **Propriedades de correção:**
  * **Segurança:** em qualquer instante estável existe no máximo um coordenador.
  * **Vivacidade:** se o líder falha, o nó vivo de maior ID acaba sendo eleito.

### Bônus — Two-Phase Commit (2PC)

O coordenador do 2PC é o líder eleito pelo Bully. Cada nó mantém um
armazenamento chave-valor com travas; uma transação é atômica (**todos aplicam
ou todos descartam**) e uniforme (sem decisões divergentes). Se o coordenador
cai após o `PREPARE`, os participantes ficam **bloqueados**; um protocolo de
terminação cooperativo resolve a transação quando o coordenador reinicia
(*crash-recovery*). Essa é a limitação clássica do 2PC (não é não-bloqueante).

---

## Autores e Entrega

* **Alunos:** Matheus Veiga Luchi (269494) e Julio Nunes Avelar (241163)
* **Link para o Vídeo de Demonstração:** _(preencher)_
* **Link para o Repositório:** _(preencher)_
