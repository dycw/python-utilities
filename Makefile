nox:
	pyenv global 3.9.16 3.10.6 3.11.1
	nox -r

nox-full:
	pyenv global 3.9.16 3.10.6 3.11.1
	nox
