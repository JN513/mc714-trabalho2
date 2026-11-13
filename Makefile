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
	@if command -v pytest >/dev/null 2>&1; then pytest tests/ -v; else python3 -m unittest discover -s tests -v; fi

# Bateria experimental completa (requer Docker)
experiments:
	bash experiments/run_benchmarks.sh

# Compila o relatório e copia o PDF para a raiz
report:
	@if command -v latexmk >/dev/null 2>&1; then \
		(cd relatorio && latexmk -pdf -interaction=nonstopmode relatorio.tex); \
	else \
		(cd relatorio && pdflatex -interaction=nonstopmode relatorio.tex && pdflatex -interaction=nonstopmode relatorio.tex); \
	fi
	cp relatorio/relatorio.pdf relatorio.pdf

clean:
	cd relatorio && latexmk -C
	rm -f relatorio.pdf
