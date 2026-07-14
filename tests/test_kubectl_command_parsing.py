"""Tests for kubectl command parsing/execution (KubectlApiProcessor.execute_command).

Bug: the pipeline was split with a naive `command.split('|')`, shattering pipes INSIDE quotes
(`grep -E 'a|b'`, `grep "x\\|y"`, `jq '... | ... | ...'`) into invalid fragments that then hit
`/bin/sh: Syntax error: Unterminated quoted string` / `")" unexpected`. Also `command.replace(
'kubectl','')` stripped the word anywhere, and a trailing `2>&1` was passed to kubectl as a bogus
arg.

NOTE: this toolkit runs in BOTH the cloud backend and the VPC agent. The VPC agent uses
`native_connection_mode` (no --server/--token). The execution MODEL is unchanged here — kubectl as
an arg-list (no shell, token never enters a shell string), then downstream segments via the shell —
only the PARSING is fixed. These tests lock in both modes.

Run: python -m pytest tests/test_kubectl_command_parsing.py -v
"""
import os
import subprocess
import sys
from unittest.mock import patch

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'drdroid_debug_toolkit'))
from core.integrations.source_api_processors.kubectl_api_processor import KubectlApiProcessor  # noqa: E402

split = KubectlApiProcessor._split_pipeline
strip = KubectlApiProcessor._strip_leading_kubectl

# The exact commands that failed in prod (account 39992)
CMD_GREP_ALT = "get namespaces | grep -E 'candidate|employer|event'"
CMD_JQ = ("get cronjobs --all-namespaces -o json | jq -r '.items[] | "
          "select(.metadata.name | contains(\"verification\")) | "
          "\"\\(.metadata.namespace)/\\(.metadata.name)\"'")
CMD_GREP_ESC = 'kubectl describe pod -n ns mypod 2>&1 | grep -A 20 "Events\\|Warning\\|Error\\|OOMKilled"'


# ── _split_pipeline: pipes inside quotes must NOT split ──
def test_split_keeps_pipe_inside_single_quotes():
    assert split(CMD_GREP_ALT) == ["get namespaces", "grep -E 'candidate|employer|event'"]


def test_split_keeps_escaped_pipe_inside_double_quotes():
    segs = split(CMD_GREP_ESC.replace("kubectl ", ""))
    assert segs == ['describe pod -n ns mypod 2>&1',
                    'grep -A 20 "Events\\|Warning\\|Error\\|OOMKilled"']


def test_split_keeps_jq_internal_pipes():
    segs = split(CMD_JQ)
    assert len(segs) == 2
    assert segs[0] == "get cronjobs --all-namespaces -o json"
    assert segs[1].startswith("jq -r '") and segs[1].count("|") == 3  # all 3 internal jq pipes intact


def test_split_no_pipe_single_segment():
    assert split("get pods -n foo -o wide") == ["get pods -n foo -o wide"]


def test_split_double_quoted_pipe():
    assert split('get pods | grep "a|b"') == ["get pods", 'grep "a|b"']


def test_split_multiple_top_level_pipes():
    assert split("get pods | grep foo | wc -l") == ["get pods", "grep foo", "wc -l"]


def test_split_empty_segments_dropped():
    assert split("get pods |") == ["get pods"]


# ── _strip_leading_kubectl: only a leading token, never mid-command ──
def test_strip_leading_kubectl():
    assert strip("kubectl get pods") == "get pods"


def test_strip_bare_kubectl():
    assert strip("kubectl") == ""


def test_strip_keeps_kubectl_inside_command():
    # 'kubectl' appearing in a grep pattern must be preserved
    assert strip("get pods | grep kubectl") == "get pods | grep kubectl"


def test_strip_does_not_touch_kubectl_prefix_word():
    assert strip("kubectlfoo get") == "kubectlfoo get"  # not a leading 'kubectl ' token


# ── execute_command: build correct args + shell segments (subprocess mocked) ──
class _FakeProc:
    def __init__(self):
        self.returncode = 0

    def communicate(self, input=None):
        return ("OUTPUT", "")


def _mk(native=False, ca_cert=None, api="https://srv:6443", token="tok"):
    p = object.__new__(KubectlApiProcessor)
    p.native_connection_mode = native
    p._KubectlApiProcessor__api_server = api
    p._KubectlApiProcessor__token = token
    p._KubectlApiProcessor__ca_cert = ca_cert
    return p


def _run(proc, command):
    calls = []

    def _popen(cmd, *a, **kw):
        calls.append({"cmd": cmd, "shell": kw.get("shell", False), "stderr": kw.get("stderr")})
        return _FakeProc()

    with patch.object(subprocess, "Popen", side_effect=_popen):
        out = proc.execute_command(command)
    return out, calls


def test_exec_native_mode_no_auth_flags():
    # VPC-agent path: kubectl gets NO --server/--token, and the quoted grep stays one shell segment
    out, calls = _run(_mk(native=True), CMD_GREP_ALT)
    assert out == "OUTPUT"
    assert calls[0]["cmd"] == ["kubectl", "get", "namespaces"]
    assert calls[0]["shell"] is False                    # kubectl never via shell
    assert calls[1]["cmd"] == "grep -E 'candidate|employer|event'"
    assert calls[1]["shell"] is True


def test_exec_non_native_ca_cert_injects_flags():
    out, calls = _run(_mk(ca_cert="/tmp/ca.crt"), "get pods")
    assert calls[0]["cmd"][:1] == ["kubectl"]
    assert "--server=https://srv:6443" in calls[0]["cmd"]
    assert "--token=tok" in calls[0]["cmd"]
    assert "--certificate-authority=/tmp/ca.crt" in calls[0]["cmd"]
    assert calls[0]["cmd"][-2:] == ["get", "pods"]


def test_exec_non_native_insecure_flags():
    out, calls = _run(_mk(ca_cert=None), "get pods")
    assert "--insecure-skip-tls-verify=true" in calls[0]["cmd"]


def test_exec_stderr_redirect_maps_to_stdout():
    out, calls = _run(_mk(native=True), CMD_GREP_ESC)
    # 2>&1 removed from kubectl args, mapped to Popen stderr=STDOUT
    assert "2>&1" not in calls[0]["cmd"]
    assert calls[0]["cmd"] == ["kubectl", "describe", "pod", "-n", "ns", "mypod"]
    assert calls[0]["stderr"] == subprocess.STDOUT
    # downstream grep stays intact and runs via the shell
    assert calls[1]["cmd"] == 'grep -A 20 "Events\\|Warning\\|Error\\|OOMKilled"'
    assert calls[1]["shell"] is True


def test_exec_shlex_preserves_quoted_kubectl_args():
    # -o jsonpath='{...}' must reach kubectl as a single arg (quotes stripped by shlex)
    out, calls = _run(_mk(native=True), "get pods -o jsonpath='{.items[*].metadata.name}'")
    assert calls[0]["cmd"] == ["kubectl", "get", "pods", "-o", "jsonpath={.items[*].metadata.name}"]
    assert len(calls) == 1                                # no pipe → single Popen


def test_exec_jq_pipeline_single_downstream_segment():
    out, calls = _run(_mk(native=True), CMD_JQ)
    assert calls[0]["cmd"] == ["kubectl", "get", "cronjobs", "--all-namespaces", "-o", "json"]
    assert len(calls) == 2 and calls[1]["shell"] is True  # the whole jq program is ONE segment


def test_exec_unbalanced_quote_raises_clear_error():
    with pytest.raises(Exception) as e:
        _run(_mk(native=True), "get pods -l 'app=")
    assert "could not parse" in str(e.value)


def test_exec_leading_kubectl_stripped_before_flags():
    out, calls = _run(_mk(native=True), "kubectl get nodes")
    assert calls[0]["cmd"] == ["kubectl", "get", "nodes"]


# ── real-shell validation (no cluster needed): segments must be valid shell syntax ──
_HAS_SH = os.path.exists("/bin/sh")


@pytest.mark.skipif(not _HAS_SH, reason="no /bin/sh")
@pytest.mark.parametrize("cmd", [CMD_GREP_ALT, CMD_JQ, CMD_GREP_ESC.replace("kubectl ", "")])
def test_fixed_segments_are_valid_shell_syntax(cmd):
    # `sh -n` = parse-only. Every downstream segment the fixed splitter emits must parse cleanly
    # (this is exactly the "Unterminated quoted string" / '")" unexpected' class of failure).
    for seg in split(cmd)[1:]:
        r = subprocess.run(["sh", "-n", "-c", seg], capture_output=True, text=True)
        assert r.returncode == 0, f"invalid shell syntax in segment {seg!r}: {r.stderr}"


@pytest.mark.skipif(not _HAS_SH, reason="no /bin/sh")
def test_old_naive_split_would_have_produced_invalid_shell():
    # regression proof: the OLD command.split('|') shatters the quoted grep into fragments that
    # /bin/sh rejects — which is precisely what produced the prod errors.
    naive = [c.strip() for c in CMD_GREP_ALT.split("|")][1:]
    invalid = [s for s in naive
               if subprocess.run(["sh", "-n", "-c", s], capture_output=True, text=True).returncode != 0]
    assert invalid, "expected the naive split to yield invalid-shell fragments"
