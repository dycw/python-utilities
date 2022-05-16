from os import getenv

from hypothesis import settings


settings.register_profile(
    "default",
    max_examples=100,
    deadline=None,
    print_blob=True,
    report_multiple_bugs=False,
)
settings.load_profile(getenv("HYPOTHESIS_PROFILE", "default"))
