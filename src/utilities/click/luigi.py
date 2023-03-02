from click import option

(local_scheduler_option_default_local, local_scheduler_option_default_central) = (
    option(
        "-ls/-nls",
        "--local-scheduler/--no-local-scheduler",
        is_flag=True,
        default=default,
        show_default=True,
        help=f"Pass {flag!r} to use the {desc} scheduler",
    )
    for default, flag, desc in [(True, "-nls", "central"), (False, "-ls", "local")]
)
workers_option = option(
    "-w",
    "--workers",
    type=int,
    default=None,
    show_default=True,
    help="The number of workers to use",
)
