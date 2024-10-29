nox:
	pyenv global 3.11.10 3.12.7 3.13.0rc3
	nox -r

nox-full:
	pyenv global 3.11.10 3.12.7 3.13.0rc3
	nox
