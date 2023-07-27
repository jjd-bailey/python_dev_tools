from datetime import datetime

def log_message(log_type: str, message: str, start: str = '', end: str = ''):
    """
    Log message
    \n
    Parameters:
    -----------
    type : str
        The type of log to create (info|warn|error)
            info = Informational
            warn = Warning
            error = Error
    message : str
        The message to log.
    start : str
        String to include before log. Usually a newline character (\\n).
    start : str
        String to include after log. Usually a newline character (\\n).
    \n
    Returns:
    --------
    `None`
    """
    if log_type.lower() == 'info':
        _log_type = 'INFO'
    elif log_type.lower() == 'warn':
        _log_type = 'WARNING'
    elif log_type.lower() == 'error':
        _log_type = 'ERROR'
    else:
        _log_type = 'NO LOG TYPE'

    now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S.%f")
    print(f'{start}{now} - {_log_type.upper()} - {message}{end}')