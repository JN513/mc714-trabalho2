#!/usr/bin/env bash
#
# Injeção manual de falha crash-stop no líder ativo.
#
# Uso: ./experiments/inject_faults.sh <NO_LIDER> <NO_MONITOR>
#   ex: ./experiments/inject_faults.sh node3 node2
#
# Observação: usamos `docker kill` (SIGKILL) em vez de `docker stop`, pois os
# contêineres executam o Python como PID 1, que ignora SIGTERM por padrão; o
# `docker stop` aguardaria o grace period de 10 s antes do SIGKILL, inflando
# artificialmente o tempo de reconvergência.
set -e

TARGET_LEADER=${1:-"node3"}
MONITOR_NODE=${2:-"node2"}
DATA_DIR="experiments/data"
OUTPUT_FILE="$DATA_DIR/fault_injection_results.csv"

mkdir -p "$DATA_DIR"
[ -f "$OUTPUT_FILE" ] || echo "timestamp,target_node,monitor_node,convergence_time_seconds" > "$OUTPUT_FILE"

echo "=================================================="
echo "   Experimento de Injeção de Falha (crash-stop)"
echo "=================================================="
echo "Líder a ser derrubado: $TARGET_LEADER"
echo "Nó monitor de logs:     $MONITOR_NODE"
echo "--------------------------------------------------"

SINCE=$(date -u +"%Y-%m-%dT%H:%M:%S")
START=$(python3 -c 'import time; print(time.time())')

docker kill "$TARGET_LEADER" > /dev/null
echo "[$TARGET_LEADER morto (SIGKILL)]. Monitorando reconvergência..."

TIMEOUT=20
RECONVERGED=false
CONVERGENCE_TIME=0

for i in $(seq 1 $((TIMEOUT * 2))); do
    sleep 0.5
    # --since ignora logs antigos (evita casar com anúncios de inicialização).
    LOGS=$(docker logs --since "$SINCE" "$MONITOR_NODE" 2>&1 || true)
    if echo "$LOGS" | grep -qE "LEADER_ANNOUNCE|LEADER_ADOPTED"; then
        CURRENT_TIME=$(python3 -c 'import time; print(time.time())')
        CONVERGENCE_TIME=$(python3 -c "print(round($CURRENT_TIME - $START, 3))")
        RECONVERGED=true
        break
    fi
done

if [ "$RECONVERGED" = "true" ]; then
    echo "-> Reconvergência concluída em ~${CONVERGENCE_TIME}s"
    TIMESTAMP=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
    echo "$TIMESTAMP,$TARGET_LEADER,$MONITOR_NODE,$CONVERGENCE_TIME" >> "$OUTPUT_FILE"
    echo "Resultado salvo em: $OUTPUT_FILE"
else
    echo "ERRO: o cluster não reconvergiu dentro de ${TIMEOUT}s."
    exit 1
fi
