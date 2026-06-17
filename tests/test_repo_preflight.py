"""Tests for the migration repo-access pre-flight (issue #16).

Verify that invalid/expired git credentials are surfaced before a destructive
migration. An injected checker is used so no real git/network call runs.
"""

from __future__ import annotations

import pytest

from migration import _summarize_git_error, check_repo_urls_reachable


def _fake_checker(results: dict[str, tuple[int, str]]):
    async def checker(url: str) -> tuple[int, str]:
        return results[url]

    return checker


async def test_all_reachable_returns_no_failures() -> None:
    urls = {"app1": "https://git/app1", "app2": "https://git/app2"}
    checker = _fake_checker({u: (0, "") for u in urls.values()})
    assert await check_repo_urls_reachable(urls, checker=checker) == {}


async def test_reports_auth_failure_per_app() -> None:
    urls = {"good": "https://git/good", "bad": "https://git/bad"}
    checker = _fake_checker(
        {
            "https://git/good": (0, ""),
            "https://git/bad": (
                128,
                "fatal: Authentication failed for 'https://git/bad'",
            ),
        }
    )
    assert await check_repo_urls_reachable(urls, checker=checker) == {
        "bad": "authentication failed"
    }


async def test_reports_multiple_failures() -> None:
    urls = {"a": "u-a", "b": "u-b", "c": "u-c"}
    checker = _fake_checker(
        {
            "u-a": (128, "remote: Repository not found"),
            "u-b": (0, ""),
            "u-c": (128, "fatal: could not resolve host: github.com"),
        }
    )
    assert await check_repo_urls_reachable(urls, checker=checker) == {
        "a": "repository not found",
        "c": "network error",
    }


async def test_empty_input_makes_no_calls() -> None:
    # No checker is supplied; an empty mapping must not invoke git at all.
    assert await check_repo_urls_reachable({}) == {}


@pytest.mark.parametrize(
    "stderr, expected",
    [
        (
            "fatal: Authentication failed for 'https://github.com/o/r'",
            "authentication failed",
        ),
        ("could not read Username for 'https://github.com'", "authentication failed"),
        ("remote: Repository not found.", "repository not found"),
        ("fatal: could not resolve host: github.com", "network error"),
        ("some unexpected git explosion", "could not access repository"),
        ("", "could not access repository"),
    ],
)
def test_summarize_git_error_classifies(stderr: str, expected: str) -> None:
    assert _summarize_git_error(stderr) == expected
