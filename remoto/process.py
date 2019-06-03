import traceback
from .util import admin_command, RemoteError


class StopCallback(Exception):
    pass


def extend_env(conn, arguments):
    """
    get the remote environment's env so we can explicitly add the path without
    wiping out everything
    """
    # retrieve the remote environment variables for the host
    try:
        result = conn.gateway.remote_exec("import os; channel.send(os.environ.copy())")
        env = result.receive()
    except Exception:
        conn.logger.exception('failed to retrieve the remote environment variables')
        env = {}

    # get the $PATH and extend it (do not overwrite)
    path = env.get('PATH', '')
    env['PATH'] = path + '/usr/local/bin:/bin:/usr/bin:/usr/local/sbin:/usr/sbin:/sbin'
    arguments['env'] = env
    if arguments.get('extend_env'):
        for key, value in arguments['extend_env'].items():
            arguments['env'][key] = value
        arguments.pop('extend_env')
    return arguments


def reporting(conn, result, timeout=None):
    timeout = timeout or conn.global_timeout  # -1 a.k.a. wait for ever

    while True:
        try:
            conn.report(result, timeout)
        except StopCallback:
            break


def run(conn, command, exit=False, timeout=None, **kw):
    """
    A real-time-logging implementation of a remote subprocess.Popen call where
    a command is just executed on the remote end and no other handling is done.

    :param conn: A connection oject
    :param command: The command to pass in to the remote subprocess.Popen
    :param exit: If this call should close the connection at the end
    :param timeout: How many seconds to wait after no remote data is received
                    (defaults to wait for ever)
    """
    stop_on_error = kw.pop('stop_on_error', True)
    if not kw.get('env'):
        # get the remote environment's env so we can explicitly add
        # the path without wiping out everything
        kw = extend_env(conn, kw)

    command = conn.cmd(command)

    timeout = timeout or conn.global_timeout
    conn.logger.info('Running command: %s' % ' '.join(admin_command(conn.sudo, command)))
    result = conn.run(cmd=command, **kw)
    try:
        reporting(conn, result, timeout)
    except Exception:
        remote_trace = traceback.format_exc()
        remote_error = RemoteError(remote_trace)
        if remote_error.exception_name == 'RuntimeError':
            conn.logger.error(remote_error.exception_line)
        else:
            for tb_line in remote_trace.split('\n'):
                conn.logger.error(tb_line)
        if stop_on_error:
            raise RuntimeError(
                'Failed to execute command: %s' % ' '.join(command)
            )
    if exit:
        conn.exit()


def check(conn, command, exit=False, timeout=None, **kw):
    """
    Execute a remote command with ``subprocess.Popen`` but report back the
    results in a tuple with three items: stdout, stderr, and exit status.

    This helper function *does not* provide any logging as it is the caller's
    responsibility to do so.
    """
    command = conn.cmd(command)

    stop_on_error = kw.pop('stop_on_error', True)
    timeout = timeout or conn.global_timeout
    if not kw.get('env'):
        # get the remote environment's env so we can explicitly add
        # the path without wiping out everything
        kw = extend_env(conn, kw)

    conn.logger.info('Running command: %s' % ' '.join(admin_command(conn.sudo, command)))
    result = conn.check(cmd=command, **kw)
    response = None
    try:
        response = result.receive(timeout)
    except Exception as err:
        # the things we need to do here :(
        # because execnet magic, we cannot catch this as
        # `except TimeoutError`
        if err.__class__.__name__ == 'TimeoutError':
            msg = 'No data was received after %s seconds, disconnecting...' % timeout
            conn.logger.warning(msg)
            # there is no stdout, stderr, or exit code but make the exit code
            # an error condition (non-zero) regardless
            return [], [], -1
        else:
            remote_trace = traceback.format_exc()
            remote_error = RemoteError(remote_trace)
            if remote_error.exception_name == 'RuntimeError':
                conn.logger.error(remote_error.exception_line)
            else:
                for tb_line in remote_trace.split('\n'):
                    conn.logger.error(tb_line)
            if stop_on_error:
                raise RuntimeError(
                    'Failed to execute command: %s' % ' '.join(command)
                )
    if exit:
        conn.exit()
    return response
