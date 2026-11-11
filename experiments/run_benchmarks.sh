#!/usr/bin/env bash
#
# Automação da bateria experimental do Trabalho 2.
#
# Executa a suíte completa em Python (falha crash-stop + crash-recovery,
# variando o limiar de heartbeat e o número de nós), gera os CSVs em
# experiments/data/ e depois produz as figuras do relatório.
#
# Requisitos: Docker Engine + Docker Compose v2 e Python 3 com as dependências
# de requirements.txt (pip install -r requirements.txt).
set -e

cd "$(dirname "$0")/.."

echo "== 1/4 Executando cenário normal (verificação de segurança) =="
python3 experiments/run_safety.py

echo "== 2/4 Executando bateria de falhas e recuperação =="
python3 experiments/run_experiments.py

echo "== 3/4 Executando bateria do 2PC (bônus) =="
python3 experiments/run_2pc.py

echo "== 4/4 Gerando figuras e estatísticas =="
python3 experiments/analyze.py

echo "Concluído. Dados em experiments/data/ e figuras em relatorio/figures/."
