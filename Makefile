PYTHON = 3.12
BUMP = micro

clean_build: veryclean all

all: format build docs tests 

veryclean: clean
	uv cache clean && rm -rfv ./.venv

format: 
	( \
		uv run isort . && \
		uv run ruff format . \
	)

requirements.txt: requirements.in
	( \
		echo "Compiling requirements.txt..." && \
		uv pip compile --output-file=requirements.txt requirements.in \
	)

install: build
	( \
		echo "Installing package as editable project..." && \
		uv pip install --upgrade -e . \
	)

tests: install 
	( \
		echo "Running tests..." && \
		uv run pytest -vv tests/ \
	)

coverage: install
	( \
		echo "Running tests with coverage..." && \
		uv run pytest --cov=./src/pycypher --cov-report=html:coverage_report \
	)

clean:
	( \
		rm -rfv ./venv && \
		rm -rfv ./sphinx_docs/_build/html/* \
		rm -rfv ./sphinx_docs/_build/doctrees/* \
		rm -rfv ./docs/* \
		rm -fv ./requirements.txt \
		rm -rfv ./dist/* \
		rm -rfv ./coverage_report \
	)

build: 
	( \
		echo "Formatting code and building package..." && \
		uv run hatch build -t wheel \
	)

docs: install
	( \
		echo "Building documentation..." && \
		cd sphinx_docs && \
		uv run make html && \
		uv run make singlehtml && \
		cp -rfv _build/singlehtml/* ../docs/ \
	)

publish: build
	( \
		uv run python ./release.py --increment=$(BUMP) \
	)

.PHONY: clean clean_build tests deps install build docs grammar