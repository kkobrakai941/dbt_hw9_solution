.PHONY: deps seed run test docs full-cycle

deps:
	 dbt deps

seed:
	 dbt seed

run:
	 dbt run

test:
	 dbt test

docs:
	 dbt docs generate

full-cycle: deps seed run test docs
