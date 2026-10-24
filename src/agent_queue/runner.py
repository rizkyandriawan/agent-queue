"""AgentScope execution for initialization and message jobs."""

from __future__ import annotations

import asyncio
import base64
import json
import os
import signal
import sys
from pathlib import Path
from typing import Any

from pydantic import BaseModel

from .config import Config
from .registry import SkillPackage

MAX_TOOL_OUTPUT = 64 * 1024


class AgentFailure(RuntimeError):
    """The agent completed but did not perform the requested work."""


class Outcome(BaseModel):
    success: bool
    summary: str


class AgentScopeRunner:
    def __init__(self, config: Config) -> None:
        self.config = config

    def run_init(
        self,
        skill: SkillPackage,
        workspace: Path,
        artifact_dir: Path,
        env: dict[str, str],
        *,
        timeout: float,
        max_iterations: int,
    ) -> str:
        instructions = skill.read_init()
        task = (
            "Initialize this automation environment now. Read the preparation "
            "instructions below and perform them. Supporting files from extras/ "
            "and scripts/ are already copied into your workspace. Use the tools "
            "to create and test reusable artifacts. Do not perform the per-message "
            "business action during initialization.\n\n" + instructions
        )
        return self._run(
            "init", skill, workspace, artifact_dir, env, task, timeout, max_iterations
        )

    def run_message(
        self,
        skill: SkillPackage,
        workspace: Path,
        artifact_dir: Path,
        env: dict[str, str],
        raw: bytes,
        *,
        timeout: float,
        max_iterations: int,
        metadata: dict[str, Any],
    ) -> str:
        try:
            body = raw.decode("utf-8")
        except UnicodeDecodeError:
            body = "Binary AMQP body (base64):\n" + base64.b64encode(raw).decode()
        details = "\n".join(
            f"{key}: {value}" for key, value in metadata.items() if value
        )
        task = (
            "An AMQP message has arrived. Apply the skill instructions to it and "
            "actually perform the work.\n\n"
            f"Message metadata:\n{details or '(none)'}\n\n"
            f"Message body:\n{body}"
        )
        return self._run(
            "message",
            skill,
            workspace,
            artifact_dir,
            env,
            task,
            timeout,
            max_iterations,
        )

    def _run(
        self,
        phase: str,
        skill: SkillPackage,
        workspace: Path,
        artifact_dir: Path,
        env: dict[str, str],
        task: str,
        timeout: float,
        max_iterations: int,
    ) -> str:
        workspace = workspace.resolve()
        artifact_dir = artifact_dir.resolve()
        artifact_dir.mkdir(parents=True, exist_ok=True)
        instructions = skill.read_skill() if phase == "message" else skill.read_init()
        try:
            return asyncio.run(
                self._execute(
                    phase,
                    skill,
                    workspace,
                    artifact_dir,
                    env,
                    instructions,
                    task,
                    timeout,
                    max_iterations,
                )
            )
        except AgentFailure:
            raise
        except TimeoutError as error:
            raise TimeoutError(f"{phase} timed out after {timeout:.0f}s") from error

    async def _execute(
        self,
        phase: str,
        skill: SkillPackage,
        workspace: Path,
        artifact_dir: Path,
        env: dict[str, str],
        instructions: str,
        task: str,
        timeout: float,
        max_iterations: int,
    ) -> str:
        from agentscope.agent import Agent, ReActConfig
        from agentscope.credential import OpenAICredential
        from agentscope.event import ReplyEndEvent
        from agentscope.message import Msg, UserMsg
        from agentscope.model import OpenAIChatModel
        from agentscope.permission import PermissionBehavior, PermissionDecision
        from agentscope.tool import FunctionTool, Toolkit
        from agentscope.types import ReplyFinishedReason

        model_name, api_key, base_url = self.config.model_settings(env)
        roots = (workspace, artifact_dir)

        def resolve(path: str) -> Path:
            candidate = Path(path).expanduser()
            if not candidate.is_absolute():
                candidate = workspace / candidate
            candidate = candidate.resolve()
            if not any(
                candidate == root or root in candidate.parents for root in roots
            ):
                raise ValueError(
                    f"path is outside the workspace and artifact directory: {path}"
                )
            return candidate

        async def read_file(path: str) -> str:
            """Read a UTF-8 file from the workspace or artifact directory.

            Args:
                path: Relative path in the workspace, or an absolute path inside it.
            """
            return await asyncio.to_thread(resolve(path).read_text)

        async def write_file(path: str, content: str) -> str:
            """Create or replace a UTF-8 file in the workspace or artifact directory.

            Args:
                path: Relative path in the workspace, or an absolute path inside it.
                content: Complete new file contents.
            """
            target = resolve(path)
            await asyncio.to_thread(target.parent.mkdir, parents=True, exist_ok=True)
            await asyncio.to_thread(target.write_text, content)
            return str(target)

        async def run_shell(command: str) -> str:
            """Run a shell command in the workspace; the job environment is inherited.

            Args:
                command: Shell command, for example python scripts/call_api.py.
            """
            process = await asyncio.create_subprocess_exec(
                "/bin/bash",
                "-c",
                command,
                cwd=workspace,
                env={
                    **env,
                    "PATH": str(Path(sys.executable).parent)
                    + os.pathsep
                    + env.get("PATH", os.defpath),
                    "AGENTQUEUE_WORKSPACE": str(workspace),
                    "AGENTQUEUE_ARTIFACTS": str(artifact_dir),
                    "AGENTQUEUE_PHASE": phase,
                    "AGENTQUEUE_SKILL": skill.name,
                    "AGENTQUEUE_REVISION": skill.revision,
                },
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.STDOUT,
                start_new_session=True,
            )
            collector = asyncio.create_task(_drain(process.stdout))
            try:
                await asyncio.wait_for(process.wait(), timeout)
            except BaseException:
                await _terminate(process)
                collector.cancel()
                raise
            output = await collector
            return json.dumps({"exit_code": process.returncode, "output": output})

        allow = PermissionDecision(
            behavior=PermissionBehavior.ALLOW,
            message="Enabled by the AgentQueue skill configuration",
        )
        toolkit = Toolkit(
            tools=[
                FunctionTool(tool, permission=allow, is_concurrency_safe=False)
                for tool in (read_file, write_file, run_shell)
            ]
        )
        model = OpenAIChatModel(
            credential=OpenAICredential(api_key=api_key, base_url=base_url),
            model=model_name,
            stream=False,
            max_retries=0,
            client_kwargs={"timeout": timeout, "max_retries": 0},
        )
        system = (
            f"You execute the {phase} phase of an unattended AgentQueue automation. "
            f"Skill: {skill.name}, revision {skill.revision[:12]}. "
            f"Workspace: {workspace}. Artifact directory: {artifact_dir}. "
            f"Python interpreter: {sys.executable}. "
            "Relative tool paths resolve inside the workspace. You may write only "
            "inside the workspace and artifact directory. Use the tools to perform "
            "the work, not merely describe it. Credentials are available to shell "
            "commands as environment variables; never print or persist secret values. "
            "Report success=false if required work is incomplete, inputs are missing, "
            "or an operation failed. Finish with the required structured outcome.\n\n"
            "Your operating instructions:\n" + instructions
        )
        agent = Agent(
            name=f"agentqueue_{phase}",
            model=model,
            toolkit=toolkit,
            system_prompt=system,
            react_config=ReActConfig(max_iters=max_iterations),
        )

        async def reply() -> str:
            reason = None
            final = None
            async for item in agent.reply_stream(
                UserMsg("agentqueue", task),
                structured_schema=Outcome,
                yield_final_msg=True,
            ):
                if isinstance(item, ReplyEndEvent):
                    reason = item.finished_reason
                if isinstance(item, Msg):
                    final = item
            if reason != ReplyFinishedReason.COMPLETED or final is None:
                raise AgentFailure(
                    f"agent did not complete: {reason or 'awaiting interaction'}"
                )
            output = getattr(final, "structured_output", None)
            if not output:
                raise AgentFailure(
                    "agent did not produce the required structured outcome"
                )
            outcome = Outcome.model_validate(output)
            if not outcome.success:
                raise AgentFailure(outcome.summary)
            return outcome.summary

        try:
            return await asyncio.wait_for(reply(), timeout)
        finally:
            client = getattr(model, "client", None)
            if client is not None:
                try:
                    await client.close()
                except Exception:
                    pass


async def _drain(stream: asyncio.StreamReader | None) -> str:
    if stream is None:
        return ""
    data = bytearray()
    truncated = False
    while True:
        chunk = await stream.read(8192)
        if not chunk:
            break
        remaining = MAX_TOOL_OUTPUT - len(data)
        if remaining > 0:
            data.extend(chunk[:remaining])
        if len(chunk) > max(remaining, 0):
            truncated = True
    text = data.decode("utf-8", "replace")
    return text + ("\n[output truncated]" if truncated else "")


async def _terminate(process: asyncio.subprocess.Process) -> None:
    if process.returncode is not None:
        return
    try:
        os.killpg(process.pid, signal.SIGTERM)
    except ProcessLookupError:
        return
    try:
        await asyncio.wait_for(process.wait(), 3)
    except asyncio.TimeoutError:
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        await process.wait()
