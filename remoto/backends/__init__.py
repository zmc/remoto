import inspect
import json
import socket
import sys
import execnet
import logging
from remoto.process import check, StopCallback


def _remote_run(channel, cmd, **kw):
    import subprocess
    import sys
    from select import select
    stop_on_nonzero = kw.pop('stop_on_nonzero', True)

    process = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        close_fds=True,
        **kw
    )

    while True:
        reads, _, _ = select(
            [process.stdout.fileno(), process.stderr.fileno()],
            [], []
        )

        for descriptor in reads:
            if descriptor == process.stdout.fileno():
                read = process.stdout.readline()
                if read:
                    channel.send({'debug': read})
                    sys.stdout.flush()

            if descriptor == process.stderr.fileno():
                read = process.stderr.readline()
                if read:
                    channel.send({'warning': read})
                    sys.stderr.flush()

        if process.poll() is not None:
            # ensure we do not have anything pending in stdout or stderr
            # unfortunately, we cannot abstract this repetitive loop into its
            # own function because execnet does not allow for non-global (or
            # even nested functions). This must be repeated here.
            while True:
                err_read = out_read = None
                for descriptor in reads:
                    if descriptor == process.stdout.fileno():
                        out_read = process.stdout.readline()
                        if out_read:
                            channel.send({'debug': out_read})
                            sys.stdout.flush()

                    if descriptor == process.stderr.fileno():
                        err_read = process.stderr.readline()
                        if err_read:
                            channel.send({'warning': err_read})
                            sys.stderr.flush()
                # At this point we have gone through all the possible
                # descriptors and `read` was empty, so we now can break out of
                # this since all stdout/stderr has been properly flushed to
                # logging
                if not err_read and not out_read:
                    break

            break

    returncode = process.wait()
    if returncode != 0:
        if stop_on_nonzero:
            raise RuntimeError(
                "command returned non-zero exit status: %s" % returncode
            )
        else:
            channel.send({'warning': "command returned non-zero exit status: %s" % returncode})


def _remote_check(channel, cmd, **kw):
    import subprocess
    stdin = kw.pop('stdin', None)
    process = subprocess.Popen(
        cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, stdin=subprocess.PIPE, **kw
    )

    if stdin:
        if not isinstance(stdin, bytes):
            stdin.encode('utf-8', errors='ignore')
        stdout_stream, stderr_stream = process.communicate(stdin)
    else:
        stdout_stream = process.stdout.read()
        stderr_stream = process.stderr.read()

    try:
        stdout_stream = stdout_stream.decode('utf-8')
        stderr_stream = stderr_stream.decode('utf-8')
    except AttributeError:
        pass

    stdout = stdout_stream.splitlines()
    stderr = stderr_stream.splitlines()
    channel.send((stdout, stderr, process.wait()))


class BaseConnection(object):
    """
    Base class for Connection objects. Provides a generic interface to execnet
    for setting up the connection
    """
    executable = ''
    remote_import_system = 'legacy'

    def __init__(self, hostname, logger=None, sudo=False, threads=1, eager=True,
                 detect_sudo=False, interpreter=None, ssh_options=None):
        self.sudo = sudo
        self.hostname = hostname
        self.ssh_options = ssh_options
        self.logger = logger or basic_remote_logger()
        self.remote_module = None
        self.channel = None
        self.global_timeout = None  # wait for ever
        self.log_map = {
            'debug': self.logger.debug,
            'error': self.logger.error,
            'warning': self.logger.warning
        }

        self.interpreter = interpreter or 'python%s' % sys.version_info[0]

        if eager:
            try:
                if detect_sudo:
                    self.sudo = self._detect_sudo()
                self.gateway = self._make_gateway(hostname)
            except OSError:
                self.logger.error(
                    "Can't communicate with remote host, possibly because "
                    "%s is not installed there" % self.interpreter
                )
                raise

    def _make_gateway(self, hostname):
        gateway = execnet.makegateway(
            self._make_connection_string(hostname)
        )
        gateway.reconfigure(py2str_as_py3str=False, py3str_as_py2str=False)
        return gateway

    def _detect_sudo(self, _execnet=None):
        """
        ``sudo`` detection has to create a different connection to the remote
        host so that we can reliably ensure that ``getuser()`` will return the
        right information.

        After getting the user info it closes the connection and returns
        a boolean
        """
        exc = _execnet or execnet
        gw = exc.makegateway(
            self._make_connection_string(self.hostname, use_sudo=False)
        )

        channel = gw.remote_exec(
            'import getpass; channel.send(getpass.getuser())'
        )

        result = channel.receive()
        gw.exit()

        if result == 'root':
            return False
        self.logger.debug('connection detected need for sudo')
        return True

    def _make_connection_string(self, hostname, _needs_ssh=None, use_sudo=None):
        _needs_ssh = _needs_ssh or needs_ssh
        interpreter = self.interpreter
        if use_sudo is not None:
            if use_sudo:
                interpreter = 'sudo ' + interpreter
        elif self.sudo:
            interpreter = 'sudo ' + interpreter
        if _needs_ssh(hostname):
            if self.ssh_options:
                return 'ssh=%s %s//python=%s' % (
                    self.ssh_options, hostname, interpreter
                )
            else:
                return 'ssh=%s//python=%s' % (hostname, interpreter)
        return 'popen//python=%s' % interpreter

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.exit()
        return False

    def cmd(self, cmd):
        """
        In the base connection class, this method just returns the ``cmd``
        as-is. Other implementations will end up doing transformations to the
        command by prefixing it with other flags needed. See
        :class:`KubernetesConnection` for an example
        """
        return cmd

    def execute(self, function, **kw):
        return self.gateway.remote_exec(function, **kw)

    def run(self, **kw):
        return self.execute(_remote_run, **kw)

    def check(self, **kw):
        return self.execute(_remote_check, **kw)

    def exit(self):
        self.gateway.exit()

    def import_module(self, module):
        """
        Allows remote execution of a local module. Depending on the
        ``remote_import_system`` attribute it may use execnet's implementation
        or remoto's own based on JSON.

        .. note:: It is not possible to use execnet's remote execution model on
                  connections that aren't SSH or Local.
        """
        if self.remote_import_system is not None:
            if self.remote_import_system == 'json':
                self.remote_module = JsonModuleExecute(self, module, self.logger)
            else:
                self.remote_module = LegacyModuleExecute(self.gateway, module, self.logger)
        else:
            self.remote_module = LegacyModuleExecute(self.gateway, module, self.logger)
        return self.remote_module

    def report(self, result, timeout):
        try:
            received = result.receive(timeout)
            level_received, message = list(received.items())[0]
            if not isinstance(message, str):
                message = message.decode('utf-8')
            self.log_map[level_received](message.strip('\r\n'))
        except EOFError:
            raise StopCallback
        except Exception as err:
            # the things we need to do here :(
            # because execnet magic, we cannot catch this as
            # `except TimeoutError`
            if err.__class__.__name__ == 'TimeoutError':
                msg = 'No data was received after %s seconds, disconnecting...' % timeout
                self.logger.warning(msg)
                raise StopCallback
            raise


class LegacyModuleExecute(object):
    """
    This (now legacy) class, is the way ``execnet`` does its remote module
    execution: it sends it over a channel, and does a send/receive for
    exchanging information. This only works when there is native support in
    execnet for a given connection. This currently means it would only work for
    ssh and local (Popen) connections, and will not work for anything like
    kubernetes or containers.
    """

    def __init__(self, gateway, module, logger=None):
        self.channel = gateway.remote_exec(module)
        self.module = module
        self.logger = logger

    def __getattr__(self, name):
        if not hasattr(self.module, name):
            msg = "module %s does not have attribute %s" % (str(self.module), name)
            raise AttributeError(msg)
        docstring = self._get_func_doc(getattr(self.module, name))

        def wrapper(*args):
            arguments = self._convert_args(args)
            if docstring:
                self.logger.debug(docstring)
            self.channel.send("%s(%s)" % (name, arguments))
            try:
                return self.channel.receive()
            except Exception as error:
                # Error will come as a string of a traceback, remove everything
                # up to the actual exception since we do get garbage otherwise
                # that points to non-existent lines in the compiled code
                exc_line = str(error)
                for tb_line in reversed(str(error).split('\n')):
                    if tb_line:
                        exc_line = tb_line
                        break
                raise RuntimeError(exc_line)

        return wrapper

    def _get_func_doc(self, func):
        try:
            return getattr(func, 'func_doc').strip()
        except AttributeError:
            return ''

    def _convert_args(self, args):
        if args:
            if len(args) > 1:
                arguments = str(args).rstrip(')').lstrip('(')
            else:
                arguments = str(args).rstrip(',)').lstrip('(')
        else:
            arguments = ''
        return arguments


dump_template = """
if __name__ == '__main__':
    import json, traceback
    obj = {'return': None, 'exception': None}
    try:
        obj['return'] = %s%s
    except Exception:
        obj['exception'] = traceback.format_exc()
    try:
        print(json.dumps(obj).decode('utf-8'))
    except AttributeError:
        print(json.dumps(obj))
"""


class JsonModuleExecute(object):
    """
    This remote execution class allows to ship Python code over to the remote
    node, load it via ``stdin`` and call any function with arguments. The
    resulting response is dumped over JSON so that it can get printed to
    ``stdout``, then captured locally, loaded into regular Python and returned.

    If the remote end generates an exception with a traceback, that is captured
    as well and raised accordingly.
    """

    def __init__(self, conn, module, logger=None):
        self.conn = conn
        self.module = module
        self._module_source = inspect.getsource(module)
        self.logger = logger
        self.python_executable = None

    def __getattr__(self, name):
        if not hasattr(self.module, name):
            msg = "module %s does not have attribute %s" % (str(self.module), name)
            raise AttributeError(msg)
        docstring = self._get_func_doc(getattr(self.module, name))

        def wrapper(*args):
            if docstring:
                self.logger.debug(docstring)
            if len(args):
                source = self._module_source + dump_template % (name, repr(args))
            else:
                source = self._module_source + dump_template % (name, '()')

            # check python interpreter
            if self.python_executable is None:
                self.python_executable = get_python_executable(self.conn)

            out, err, code = check(self.conn, [self.python_executable], stdin=source.encode('utf-8'))
            if not out:
                if not err:
                    err = [
                        'Traceback (most recent call last):',
                        '    File "<stdin>", in <module>',
                        'Exception: error calling "%s"' % name
                    ]
                if code:
                    raise Exception('Unexpected remote exception: \n%s\n%s' % ('\n'.join(out), '\n'.join(err)))
                # at this point, there was no stdout, and the exit code was 0,
                # we must return so that we don't fail trying to serialize back
                # the JSON
                return
            response = json.loads(out[0])
            if response['exception']:
                raise Exception(response['exception'])
            return response['return']

        return wrapper

    def _get_func_doc(self, func):
        try:
            return getattr(func, 'func_doc').strip()
        except AttributeError:
            return ''


def basic_remote_logger():
    logging.basicConfig()
    logger = logging.getLogger(socket.gethostname())
    logger.setLevel(logging.DEBUG)
    return logger


def needs_ssh(hostname, _socket=None):
    """
    Obtains remote hostname of the socket and cuts off the domain part
    of its FQDN.
    """
    if hostname.lower() in ['localhost', '127.0.0.1', '127.0.1.1']:
        return False
    _socket = _socket or socket
    fqdn = _socket.getfqdn()
    if hostname == fqdn:
        return False
    local_hostname = _socket.gethostname()
    local_short_hostname = local_hostname.split('.')[0]
    if local_hostname == hostname or local_short_hostname == hostname:
        return False
    return True


def get_python_executable(conn):
    """
    Try to determine the remote Python version so that it can be used
    when executing. Avoids the problem of different Python versions, or distros
    that do not use ``python`` but do ``python3``
    """
    # executables in order of preference:
    executables = ['python3', 'python', 'python2.7']
    script = '; '.join([
        'for name in %s' % ' '.join(executables),
        'do if $name --version 2>/dev/null',
        'then echo $name',
        'break',
        'fi',
        'done',
    ])
    cmd = ['bash', '-c', script]
    out, err, code = check(conn, cmd)
    if code:
        conn.logger.warning(
            'could not find any executable in path: %s' % ' '.join(executables)
        )
    else:
        try:
            return out[0].strip()
        except IndexError:
            conn.logger.warning('could not parse stdout: %s' % out)

    # if all fails, we just return whatever the main connection had
    conn.logger.info('Falling back to using interpreter: %s' % conn.interpreter)
    return conn.interpreter
