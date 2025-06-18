from __future__ import annotations

from random import Random, SystemRandom
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Iterable

    from utilities.types import Seed


SYSTEM_RANDOM = SystemRandom()


##


def bernoulli(*, true: float = 0.5, seed: Seed | None = None) -> bool:
    """Return a Bernoulli random variate."""
    seed = get_state(seed=seed)
    return bool(seed.binomialvariate(p=true))


##


def get_docker_name(*, seed: Seed | None = None) -> str:
    """Get a docker name."""
    state = get_state(seed=seed)
    prefix = state.choice(_DOCKER_PREFIXES)
    suffix = state.choice(_DOCKER_SUFFIXES)
    digit = state.randint(0, 9)
    return f"{prefix}_{suffix}{digit}"


# fmt: off
# https://github.com/moby/moby/blob/master/pkg/namesgenerator/names-generator.go
_DOCKER_PREFIXES = [
    "admiring", "adoring", "affectionate", "agitated", "amazing", "angry", "awesome", "blissful", "boring", "brave", "clever", "cocky", "compassionate", "competent", "condescending", "confident", "cranky", "dazzling", "determined", "distracted", "dreamy", "eager", "ecstatic", "elastic", "elated", "elegant", "eloquent", "epic", "fervent", "festive", "flamboyant", "focused", "friendly", "frosty", "gallant", "gifted", "goofy", "gracious", "happy", "hardcore", "heuristic", "hopeful", "hungry", "infallible", "inspiring", "jolly", "jovial", "keen", "kind", "laughing", "loving", "lucid", "modest", "musing", "mystifying", "naughty", "nervous", "nifty", "nostalgic", "objective", "optimistic", "peaceful", "pedantic", "pensive", "practical", "priceless", "quirky", "quizzical", "relaxed", "reverent", "romantic", "sad", "serene", "sharp", "silly", "sleepy", "stoic", "stupefied", "suspicious", "tender", "thirsty", "trusting", "unruffled", "upbeat", "vibrant", "vigilant", "vigorous", "wizardly", "wonderful", "xenodochial", "youthful", "zealous", "zen"
]
_DOCKER_SUFFIXES = [
    "agnesi", "albattani", "allen", "almeida", "archimedes", "ardinghelli", "aryabhata", "austin", "babbage", "banach", "bardeen", "bartik", "bassi", "beaver", "bell", "benz", "bhabha", "bhaskara", "blackwell", "bohr", "booth", "borg", "bose", "bose", "bose", "boyd", "brahmagupta", "brattain", "brown", "carson", "chandrasekhar", "clarke", "colden", "cori", "cray", "curie", "curran", "darwin", "davinci", "dijkstra", "dubinsky", "easley", "edison", "einstein", "elion", "engelbart", "euclid", "euler", "fermat", "fermi", "feynman", "franklin", "galileo", "gates", "goldberg", "goldstine", "goldwasser", "golick", "goodall", "haibt", "hamilton", "hawking", "heisenberg", "hermann", "heyrovsky", "hodgkin", "hoover", "hopper", "hugle", "hypatia", "jackson", "jang", "jennings", "jepsen", "johnson", "joliot", "jones", "kalam", "kare", "keller", "kepler", "khorana", "kilby", "kirch", "knuth", "kowalevski", "lalande", "lamarr", "lamport", "leakey", "leavitt", "lewin", "lichterman", "liskov", "lovelace", "lumiere", "mahavira", "mayer", "mccarthy", "mcclintock", "mclean", "mcnulty", "meitner", "meninsky", "mestorf", "minsky", "mirzakhani", "montalcini", "morse", "murdock", "neumann", "newton", "nightingale", "nobel", "noether", "northcutt", "noyce", "panini", "pare", "pasteur", "payne", "perlman", "pike", "poincare", "poitras", "ptolemy", "raman", "ramanujan", "ride", "ritchie", "roentgen", "rosalind", "saha", "sammet", "shannon", "shaw", "shirley", "shockley", "sinoussi", "snyder", "spence", "stallman", "stonebraker", "swanson", "swartz", "swirles", "tesla", "thompson", "torvalds", "turing", "varahamihira", "visvesvaraya", "volhard", "wescoff", "wiles", "williams", "wilson", "wing", "wozniak", "wright", "yalow", "yonath"
]
# fmt: on


##


def get_state(*, seed: Seed | None = None) -> Random:
    """Get a random state."""
    return seed if isinstance(seed, Random) else Random(x=seed)


##
def shuffle[T](iterable: Iterable[T], /, *, seed: Seed | None = None) -> list[T]:
    """Shuffle an iterable."""
    copy = list(iterable).copy()
    state = get_state(seed=seed)
    state.shuffle(copy)
    return copy


__all__ = ["SYSTEM_RANDOM", "bernoulli", "get_docker_name", "get_state", "shuffle"]
