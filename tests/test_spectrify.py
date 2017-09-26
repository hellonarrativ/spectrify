#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Tests for `spectrify` package."""

from click.testing import CliRunner

from spectrify import main


def test_command_line_interface():
    """Test the CLI."""
    runner = CliRunner()
    result = runner.invoke(main.cli)
    assert result.exit_code == 0
    assert 'Main entry point for spectrify.' in result.output
    help_result = runner.invoke(main.cli, ['--help'])
    assert help_result.exit_code == 0
    assert 'Show this message and exit.' in help_result.output
