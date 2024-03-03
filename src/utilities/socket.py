from socket import gethostname

HOSTNAME = gethostname()


__all__ = ["HOSTNAME"]
