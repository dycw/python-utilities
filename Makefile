nox:
	pyenv global 3.9.16 3.10.10 3.11.2
	nox -r

nox-full:
	pyenv global 3.9.16 3.10.10 3.11.2
	nox
