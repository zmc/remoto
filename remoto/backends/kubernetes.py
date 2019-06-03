import json
import kubernetes

from remoto.process import StopCallback

from . import BaseConnection


kubernetes.config.load_kube_config()
k8s_api = kubernetes.client.CoreV1Api()


class KubernetesResult(object):
    """
    A thin shim to provide an execnet-like interface to callers of
    KubernetesConnection.check()
    """
    def __init__(self, result):
        self.result = result

    def receive(self, *args, **kwargs):
        return self.result


class KubernetesConnection(BaseConnection):

    executable = 'kubectl'
    remote_import_system = 'json'

    def __init__(self, pod_name, namespace=None, context=None, **kw):
        self.namespace = namespace
        self.context = context
        self.pod_name = pod_name
        self.pod = None
        super(KubernetesConnection, self).__init__(hostname='localhost', **kw)

    def cmd(self, cmd):
        return cmd

    def get_namespace(self):
        if not self.namespace:
            all_pods = k8s_api.list_pod_for_all_namespaces()
            self.pod = list(filter(
                lambda p: p.metadata.name == self.pod_name,
                all_pods.items
            ))[0]
            self.namespace = self.pod.metadata.namespace
        return self.namespace

    def get_pod(self, refresh=False):
        if self.pod and not refresh:
            return self.pod
        self.pod = k8s_api.read_namespaced_pod(
            self.pod_name, self.get_namespace())
        return self.pod

    def execute(self, cmd, **kw):
        args = [
            k8s_api.connect_get_namespaced_pod_exec,
            self.pod_name, self.get_namespace(),
        ]
        kwargs = dict(
            command=cmd,
            stderr=True, stdin=False, stdout=True, tty=False,
            _preload_content=False
        )
        if kw.get('stdin'):
            kwargs['stdin'] = True
        try:
            stream = kubernetes.stream.stream(*args, **kwargs)
        except kubernetes.client.rest.ApiException:
            # If the initial call fails, it is probably because there are
            # multiple containers in the pod. We can get around this, but it
            # involves another API call (or possibly two).
            kwargs['container'] = self.get_pod().spec.containers[0].name
            stream = kubernetes.stream.stream(*args, **kwargs)
        return stream

    def run(self, cmd, **kw):
        return self.execute(cmd, **kw)

    def check(self, cmd, **kw):
        self.logger.debug("cmd: %s", cmd)
        stdin = kw.get('stdin')
        if stdin:
            self.logger.debug("stdin: %s" % stdin)
            if isinstance(stdin, bytes):
                stdin = stdin.decode('utf-8', errors='ignore')
            cmd = ['/bin/sh', '-c'] + cmd
            cmd[-1] = "{lastcmd} << EOF\n {stdin}\nEOF".format(
                lastcmd=cmd[-1], stdin=stdin)
        resp = self.execute(cmd, **kw)
        stdout = []
        stderr = []
        code = -1
        code_channel = {}
        peek_time = 0.01
        read_time = 0.01
        while resp.is_open() and resp.sock.connected:
            if resp.peek_stdout(peek_time):
                new_stdout = self._to_lines(resp.read_stdout(read_time))
                for line in new_stdout:
                    self.logger.debug("stdout: %s", line)
                stdout.extend(new_stdout)
            if resp.peek_stderr(peek_time):
                stderr.extend(self._to_lines(resp.read_stderr(read_time)))
            if resp.peek_channel(3, peek_time):
                code_channel = json.loads(resp.read_channel(3, read_time))
        if code_channel.get('status') == 'Success':
            code = 0
        else:
            details = code_channel.get('details')
            if details:
                causes = details.get('causes', [])
                for cause in causes:
                    if cause.get('reason') == 'exitcode':
                        code = int(cause.get('message'))
                        break

        return KubernetesResult((stdout, stderr, code))

    def report(self, result, timeout):
        if not result.is_open():
            raise StopCallback

        def do_log(data, logfunc):
            for line in data.split('\n'):
                if line:
                    logfunc(line.strip('\r\n'))

        if result.peek_stdout():
            self._to_lines(result.read_stdout(), self.logger.debug)
        if result.peek_stderr():
            self._to_lines(result.read_stderr(), self.logger.warning)

    @staticmethod
    def _to_lines(data, callback=None):
        lines = []
        for line in data.split('\n'):
            if line:
                lines.append(line)
                if callback:
                    callback(line)
        return lines
