nox:
	pyenv global 3.9.16 3.10.12 3.11.4
	nox -r

nox-full:
	pyenv global 3.9.16 3.10.12 3.11.4
	nox
