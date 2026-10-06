from typer._click import Command, Context
from typer.core import TyperGroup


class DefaultCmdTyperGroup(TyperGroup):
    """Route unknown or absent subcommands to a default command.

    Configured via `default_cmd_name` and `insert_default_if_no_args` (a bare
    invocation runs the default command).
    """

    default_cmd_name = "q"
    insert_default_if_no_args = True
    # leading unknown options (`ftmq -s Person`) must reach the default command
    ignore_unknown_options = True

    def parse_args(self, ctx: Context, args: list[str]) -> list[str]:
        if not args and self.insert_default_if_no_args:
            args = [self.default_cmd_name]
        return super().parse_args(ctx, args)

    def get_command(self, ctx: Context, cmd_name: str) -> Command | None:
        if cmd_name not in self.commands:
            # re-inserted into the default command's args in `resolve_command`
            ctx._default_cmd_arg0 = cmd_name  # type: ignore[attr-defined]
            cmd_name = self.default_cmd_name
        return super().get_command(ctx, cmd_name)

    def resolve_command(
        self, ctx: Context, args: list[str]
    ) -> tuple[str | None, Command | None, list[str]]:
        cmd_name, cmd, args = super().resolve_command(ctx, args)
        arg0 = getattr(ctx, "_default_cmd_arg0", None)
        if arg0 is not None and cmd is not None:
            args = [arg0, *args]
            cmd_name = cmd.name
        return cmd_name, cmd, args
