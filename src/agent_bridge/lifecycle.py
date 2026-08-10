from __future__ import annotations

import os
import shlex
import signal
import subprocess
import time
from dataclasses import replace
from pathlib import Path
from typing import Any

from .store import Store, utc_now
from .teams import TeamManifest
from .tmux import TmuxError, TmuxTransport


class TeamLifecycle:
    """Bounded lifecycle operations for the durable members of one team.

    Manifest members without a recorded tmux session are launched as a direct
    child process with ``shell=False`` and a private process group.  The PID
    and Linux start token are persisted, so stop/restart only targets a
    process this instance started and never guesses at unrelated processes.
    External registrations without a PID are reported, not started or killed.
    """

    def __init__(self, store: Store, tmux: TmuxTransport | None = None) -> None:
        self.store = store
        self.tmux = tmux or TmuxTransport()
        self._children: dict[int, subprocess.Popen[bytes]] = {}

    def create_from_manifest(self, manifest: TeamManifest):
        team = self.store.create_team(name=manifest.name, metadata=manifest.metadata)
        try:
            for member in manifest.members:
                cwd = str(Path(member.cwd).expanduser().resolve())
                run = self.store.create_run(
                    name=member.name,
                    agent=member.agent,
                    mode=member.mode,
                    command=member.command,
                    cwd=cwd,
                    inbox_path=member.inbox,
                    team=team.name,
                    team_id=team.id,
                    role=member.role,
                    is_lead=member.lead,
                    readiness_required=member.readiness_required,
                    readiness_timeout=member.startup_timeout,
                    restart_policy=member.restart_policy,
                )
                self.store.add_team_member(team.id, run.id, role=member.role, is_lead=member.lead)
        except Exception:
            # Durable creation is deliberately not rolled back: the caller can
            # inspect the failed team and remove/fix the offending member.
            self.store.update_team(team.id, "failed")
            raise
        return team

    @staticmethod
    def _argv(command: str) -> list[str]:
        try:
            argv = shlex.split(command)
        except ValueError as exc:
            raise TmuxError(f"invalid member command: {exc}") from exc
        if not argv:
            raise TmuxError("member command is empty")
        return argv

    def _environment(self, run) -> dict[str, str]:
        environment = os.environ.copy()
        environment.update(
            {
                "AGENT_BRIDGE_RUN_ID": run.id,
                "AGENT_BRIDGE_RUN_NAME": run.name,
                "AGENT_BRIDGE_DB": str(self.store.path),
                "AGENT_BRIDGE_INBOX": run.inbox_path or "",
                "AGENT_BRIDGE_INBOUND_POLICY": run.inbound_policy,
            }
        )
        return environment

    def _start_member(self, run):
        cwd = Path(run.cwd).expanduser().resolve()
        if not cwd.is_dir():
            return self.store.update_run(
                run.id,
                status="failed",
                ended_at=utc_now(),
                lifecycle_state="failed",
                failure_reason=f"working directory does not exist: {cwd}",
            )
        if run.tmux_session:
            if self.tmux.has_session(run.tmux_session):
                # The session may belong to an external supervisor. Presence
                # is observable, not proof that this lifecycle object created
                # it, so leave ownership/lifecycle state unchanged.
                return run
            try:
                self.tmux.start_session(
                    session=run.tmux_session,
                    cwd=str(cwd),
                    argv=self._argv(run.command),
                    environment=self._environment(run),
                    log_path=run.log_path,
                )
            except (TmuxError, OSError) as exc:
                return self.store.update_run(
                    run.id,
                    status="failed",
                    ended_at=utc_now(),
                    lifecycle_state="failed",
                    failure_reason=str(exc),
                )
            return self.store.update_run(run.id, status="running", lifecycle_state="running", ended_at=None)

        # A run registered by another supervisor has no process ownership
        # record and must remain external.  A manifest-created member does not
        # have a PID yet, so it is safe for this lifecycle object to launch it.
        if run.process_pid is not None:
            if self.store.process_owned_and_running(run):
                return self.store.update_run(run.id, status="running", lifecycle_state="running", ended_at=None)
            self.store.clear_process(run.id)
        if run.lifecycle_state not in {"registered", "restarting", "stopped", "failed"} and run.status == "running":
            return run
        process = None
        try:
            log_handle = None
            if run.log_path:
                Path(run.log_path).expanduser().parent.mkdir(parents=True, exist_ok=True)
                log_handle = Path(run.log_path).expanduser().open("ab")
            process = subprocess.Popen(
                self._argv(run.command),
                cwd=str(cwd),
                env=self._environment(run),
                stdin=subprocess.DEVNULL,
                stdout=log_handle or subprocess.DEVNULL,
                stderr=subprocess.STDOUT,
                start_new_session=True,
                close_fds=True,
            )
            if log_handle is not None:
                log_handle.close()
            self.store.set_process(run.id, process.pid)
            self._children[process.pid] = process
        except (OSError, ValueError, TmuxError) as exc:
            if "log_handle" in locals() and log_handle is not None and not log_handle.closed:
                log_handle.close()
            if process is not None:
                try:
                    process.kill()
                    process.wait(timeout=1.0)
                except (OSError, subprocess.TimeoutExpired):
                    pass
            return self.store.update_run(
                run.id,
                status="failed",
                ended_at=utc_now(),
                lifecycle_state="failed",
                failure_reason=str(exc),
            )
        return self.store.update_run(run.id, status="running", lifecycle_state="running", ended_at=None)

    def _refresh_member(self, run):
        if run.process_pid is not None and run.process_pid in self._children:
            child = self._children[run.process_pid]
            if child.poll() is not None:
                self._children.pop(run.process_pid, None)
                return self.store.update_run(
                    run.id,
                    status="missing",
                    lifecycle_state="offline",
                    failure_reason="managed process exited",
                )
        if run.process_pid is not None and not self.store.process_owned_and_running(run):
            return self.store.update_run(
                run.id,
                status="missing",
                lifecycle_state="offline",
                failure_reason="managed process is no longer running",
            )
        if run.tmux_session and run.status in {"running", "starting"} and not self.tmux.has_session(run.tmux_session):
            return self.store.update_run(
                run.id,
                status="missing",
                lifecycle_state="offline",
                failure_reason="tmux session is no longer present",
            )
        return run

    def start(self, team_reference: str, *, wait: bool = True):
        team = self.store.get_team(team_reference)
        self.store.update_team(team.id, "starting")
        started = [self._start_member(member.run) for member in self.store.list_team_members(team.id) if member.run]
        if wait:
            for run in started:
                if not run.readiness_required or run.readiness_timeout <= 0:
                    continue
                deadline = time.monotonic() + run.readiness_timeout
                while time.monotonic() < deadline:
                    current = self.store.get_run(run.id)
                    if self.store.adapter_ready(current):
                        break
                    time.sleep(min(0.05, max(0.0, deadline - time.monotonic())))
                current = self.store.get_run(run.id)
                if not self.store.adapter_ready(current):
                    self.store.set_readiness(run.id, "offline", error="startup readiness timeout")
        current = [self._refresh_member(member.run) for member in self.store.list_team_members(team.id) if member.run]
        failed = any(run.status == "failed" for run in current)
        offline_required = any(run.readiness_required and not self.store.adapter_ready(run) for run in current)
        offline_member = any(run.status == "missing" for run in current)
        self.store.update_team(team.id, "failed" if failed else "partial" if offline_required or offline_member else "running")
        return self.status(team.id)

    def _stop_direct(self, run) -> bool:
        if not self.store.process_owned_and_running(run):
            return False
        assert run.process_pid is not None
        try:
            os.killpg(run.process_pid, signal.SIGTERM)
        except ProcessLookupError:
            child = self._children.pop(run.process_pid, None)
            if child is not None:
                child.wait(timeout=1.0)
            return False
        deadline = time.monotonic() + 5.0
        while time.monotonic() < deadline and self.store.process_owned_and_running(run):
            time.sleep(0.05)
        if self.store.process_owned_and_running(run):
            try:
                os.killpg(run.process_pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
        child = self._children.pop(run.process_pid, None)
        if child is not None:
            try:
                child.wait(timeout=1.0)
            except subprocess.TimeoutExpired:
                pass
        return True

    def stop(self, team_reference: str):
        team = self.store.get_team(team_reference)
        self.store.update_team(team.id, "stopping")
        for member in self.store.list_team_members(team.id):
            run = member.run
            if run is None:
                continue
            controlled = False
            if run.tmux_session and run.lifecycle_state in {"running", "restarting"}:
                self.tmux.stop(run.tmux_session)
                controlled = True
            elif run.process_pid is not None:
                controlled = self._stop_direct(run)
                if controlled:
                    self.store.clear_process(run.id)
            # No PID means an externally supervised or already missing run.
            # Record it as offline rather than claiming to have killed it.
            status = "killed" if controlled else ("missing" if run.status == "running" else run.status)
            self.store.update_run(run.id, status=status, ended_at=utc_now(), lifecycle_state="stopped")
        self.store.update_team(team.id, "stopped")
        return self.status(team.id)

    def restart(self, team_reference: str):
        team = self.store.get_team(team_reference)
        self.stop(team.id)
        for member in self.store.list_team_members(team.id):
            run = member.run
            if run is None:
                continue
            with self.store.connect() as connection:
                connection.execute(
                    "UPDATE runs SET restart_count = restart_count + 1, status = 'starting', ended_at = NULL, lifecycle_state = 'restarting', readiness = 'offline', adapter_session_id = NULL, adapter_heartbeat_at = NULL WHERE id = ?",
                    (run.id,),
                )
            self.store.audit("member.restarted", None, run.id, {"team_id": team.id})
        return self.start(team.id)

    def status(self, team_reference: str) -> dict[str, Any]:
        team = self.store.get_team(team_reference)
        members = []
        for member in self.store.list_team_members(team.id):
            run = self._refresh_member(member.run) if member.run else None
            members.append(replace(member, run=run))
        return {"team": self.store.get_team(team.id), "members": members}

    def watch(self, team_reference: str, *, interval: float = 1.0, iterations: int = 1):
        if interval < 0 or iterations < 1 or iterations > 1000:
            raise ValueError("watch limits are invalid")
        results = []
        for index in range(iterations):
            results.append(self.status(team_reference))
            if index + 1 < iterations:
                time.sleep(interval)
        return results
