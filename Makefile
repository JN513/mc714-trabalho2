.PHONY: up down logs test experiments report clean

# Sobe o cluster padrão de 3 nós
up:
	docker compose up -d --build

# Derruba o cluster e a rede
down:
	docker compose down -v

logs:
	docker compose logs -f

# Testes unitários e de integração
test:
	pytest tests/ -v

# Bateria experimental completa (requer Docker)
experiments:
	bash experiments/run_benchmarks.sh

# Compila o relatório e copia o PDF para a raiz
report:
	cd relatorio && latexmk -pdf -interaction=nonstopmode relatorio.tex
	cp relatorio/relatorio.pdf report.pdf

clean:
	cd relatorio && latexmk -C
	rm -f report.pdf
