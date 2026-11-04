#!/usr/bin/env bash
set -e

ROUNDS=5
DATA_FILE="experiments/data/benchmark_results.csv"

mkdir -p experiments/data
echo "round,target_node,monitor_node,convergence_time_sec" > "$DATA_FILE"

echo "=================================================="
echo " Running Automated Benchmark ($ROUNDS Rounds)"
echo "=================================================="

for i in $(seq 1 $ROUNDS); do
    echo "---- Rodada $i/$ROUNDS ----"
    
    # Limpa contêineres e logs de execuções anteriores
    docker compose down -v > /dev/null 2>&1 || true
    docker compose up -d --build > /dev/null 2>&1
    
    # Aguarda inicialização e estabilização do cluster (5 segundos)
    sleep 5

    # Executa a injeção derrubando o node3 e monitorando o node2
    ./experiments/inject_faults.sh node3 node2

    # Extrai o último resultado gerado
    LAST_RESULT=$(tail -n 1 experiments/data/fault_injection_results.csv | cut -d',' -f2,3,4)
    echo "$i,$LAST_RESULT" >> "$DATA_FILE"
    
    sleep 1
done

docker compose down -v > /dev/null 2>&1 || true

echo "--------------------------------------------------"
echo " Bateria de testes concluída! Resultados gravados em:"
echo " $DATA_FILE"