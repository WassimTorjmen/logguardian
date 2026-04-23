import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../src"))

import pytest
from parsers.linux_parser import LinuxParser
from parsers.ssh_parser import SSHParser
from parsers.hadoop_parser import HadoopParser
from parsers.supercomputer_parser import SupercomputerParser

DATA_DIR = os.path.join(os.path.dirname(__file__), "../../../data")


def test_linux_parser_yields_entries():
    parser = LinuxParser()
    entries = list(parser.parse(DATA_DIR))
    assert len(entries) > 0
    e = entries[0]
    assert e.source == "linux"
    assert e.level in {"INFO", "WARN", "ERROR", "FATAL", "DEBUG"}
    assert e.message


def test_ssh_parser_yields_entries():
    parser = SSHParser()
    entries = list(parser.parse(DATA_DIR))
    assert len(entries) > 0
    e = entries[0]
    assert e.source == "ssh"
    assert "sshd" in e.component


def test_ssh_parser_detects_warn_level():
    parser = SSHParser()
    entries = list(parser.parse(DATA_DIR))
    warn_entries = [e for e in entries if e.level == "WARN"]
    assert len(warn_entries) > 0, "Expected WARN entries for invalid user / failed password"


def test_hadoop_parser_yields_entries():
    parser = HadoopParser()
    entries = list(parser.parse(DATA_DIR))
    assert len(entries) > 0
    e = entries[0]
    assert e.source == "hadoop"
    assert e.level in {"INFO", "WARN", "ERROR", "FATAL", "DEBUG"}


def test_supercomputer_parser_yields_entries():
    parser = SupercomputerParser()
    entries = list(parser.parse(DATA_DIR))
    assert len(entries) > 0
    e = entries[0]
    assert e.source == "supercomputer"


def test_log_entry_to_json():
    import json
    parser = LinuxParser()
    entries = list(parser.parse(DATA_DIR))
    assert entries
    payload = json.loads(entries[0].to_json())
    assert set(payload.keys()) == {"timestamp", "source", "host", "level", "component", "message", "raw"}
