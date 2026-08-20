import logging


def filter_log_messages(logging_watcher, level: int) -> list[str]:
    return [r.message for r in logging_watcher.records if r.levelno == level]


def get_error_messages(logging_watcher) -> list[str]:
    return filter_log_messages(logging_watcher, logging.ERROR)


def get_warning_messages(logging_watcher) -> list[str]:
    return filter_log_messages(logging_watcher, logging.WARNING)


def get_info_messages(logging_watcher) -> list[str]:
    return filter_log_messages(logging_watcher, logging.INFO)


def get_debug_messages(logging_watcher) -> list[str]:
    return filter_log_messages(logging_watcher, logging.DEBUG)
