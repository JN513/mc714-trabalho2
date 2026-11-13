#!/usr/bin/env bash
set -e

TARGET_LEADER=${1:-"node3"}
MONITOR_NODE=${2:-"node2"}
DATA_DIR="experiments/data"
OUTPUT_FILE="$DATA_DIR/fault_injection_results.csv"

mkdir -p "$DATA_DIR"

if [ ! -f "$OUTPUT_FILE" ]; then
    echo "timestamp,target_node,monitor_node,convergence_time_seconds" > "$OUTPUT_FILE"
fi

echo "=================================================="
echo "   Iniciando Experimento de Injeção de Falha"
echo "=================================================="
echo "Líder a ser derrubado: $TARGET_LEADER"
echo "Nó monitor de logs:     $MONITOR_NODE"
echo "--------------------------------------------------"

# Marca o tempo inicial usando Python nativo
START_TIME=$(python3 -c 'import time; print(time.time())')
START_TIME_RFC=$(date -u +"%Y-%m-%dT%H:%M:%SZ")

# Paralisa o contêiner do líder
docker stop -t 1 "$TARGET_LEADER" > /dev/null

echo "[$TARGET_LEADER parado]. Monitorando tempo de eleição..."

TIMEOUT=20
RECONVERGED=false
CONVERGENCE_TIME=0

for i in $(seq 1 $TIMEOUT); do
    sleep 1
    CURRENT_TIME=$(python3 -c 'import time; print(time.time())')
    
    # Verifica logs do nó monitor gerados após a parada
    LOGS=$(docker logs --since "$START_TIME_RFC" "$MONITOR_NODE" 2>&1 || true)
    
    if echo "$LOGS" | grep -qE "Novo líder reconhecido|VENCEU A ELEIÇÃO"; then
        CONVERGENCE_TIME=$(python3 -c "print(round($CURRENT_TIME - $START_TIME, 3))")
        RECONVERGED=true
        break
    fi
done

if [ "$RECONVERGED" = "true" ]; then
    echo "-> Reconvergência concluída com sucesso em: ${CONVERGENCE_TIME}s!"
    TIMESTAMP=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
    echo "$TIMESTAMP,$TARGET_LEADER,$MONITOR_NODE,$CONVERGENCE_TIME" >> "$OUTPUT_FILE"
    echo "Resultado salvo em: $OUTPUT_FILE"
else
    echo "ERRO: O cluster não reconvergiu dentro do limite de ${TIMEOUT}s."
    exit 1
fi