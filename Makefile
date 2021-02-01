check:
	poetry run pre-commit run --all

test:
	poetry run pytest
