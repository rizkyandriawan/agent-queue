class AgentQueueError(Exception):
    exit_code = 70


class CliError(AgentQueueError):
    def __init__(self, exit_code: int, message: str) -> None:
        self.exit_code = exit_code
        self.message = message
        super().__init__(message)


class ConfigError(CliError):
    def __init__(self, message: str) -> None:
        super().__init__(78, message)


class DaemonUnavailable(CliError):
    def __init__(self, message: str) -> None:
        super().__init__(69, message)


class Conflict(CliError):
    def __init__(self, message: str) -> None:
        super().__init__(75, message)
