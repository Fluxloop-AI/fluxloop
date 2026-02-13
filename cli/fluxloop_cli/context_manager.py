"""
Local context management for FluxLoop CLI.

Architecture:
- .fluxloop/project.json: Web Project connection (workspace level)
- .fluxloop/context.json: Current scenario pointer (workspace level)
- .fluxloop/scenarios/{name}/.state/sync.json: Scenario sync state

Design principle: Local context is a "pointer" to server resources.
Server is the Single Source of Truth.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, List

from .constants import (
    FLUXLOOP_DIR_NAME,
    SCENARIOS_DIR_NAME,
    STATE_DIR_NAME,
    PROJECT_JSON_FILENAME,
    CONTEXT_JSON_FILENAME,
)


# =============================================================================
# Web Project Connection (workspace level)
# =============================================================================

@dataclass
class WebProjectConnection:
    """
    Connection to a Web Project (stored in .fluxloop/project.json).
    
    This links the local workspace to a Web Project.
    """
    project_id: str
    project_name: str
    api_url: str = ""
    connected_at: Optional[str] = None
    
    def to_dict(self) -> dict:
        return {
            "project_id": self.project_id,
            "project_name": self.project_name,
            "api_url": self.api_url,
            "connected_at": self.connected_at or datetime.now(timezone.utc).isoformat(),
        }
    
    @classmethod
    def from_dict(cls, data: dict) -> WebProjectConnection:
        return cls(
            project_id=data["project_id"],
            project_name=data["project_name"],
            api_url=data.get("api_url", ""),
            connected_at=data.get("connected_at"),
        )


# =============================================================================
# Context Data Classes
# =============================================================================

@dataclass
class ScenarioContext:
    """Current scenario reference (maps to Web Scenario)."""
    id: str
    name: str
    local_path: Optional[str] = None  # e.g., "scenarios/order-cancel"
    
    def to_dict(self) -> dict:
        data = {"id": self.id, "name": self.name}
        if self.local_path:
            data["local_path"] = self.local_path
        return data
    
    @classmethod
    def from_dict(cls, data: dict) -> ScenarioContext:
        return cls(
            id=data["id"], 
            name=data["name"],
            local_path=data.get("local_path"),
        )


@dataclass
class BundleContext:
    """Current bundle reference."""
    id: str
    version: str
    
    def to_dict(self) -> dict:
        return {"id": self.id, "version": self.version}
    
    @classmethod
    def from_dict(cls, data: dict) -> BundleContext:
        return cls(id=data["id"], version=data["version"])


@dataclass
class WorkflowState:
    """
    Tracks workflow progress for agent context awareness.
    
    Phases: init → setup → data → ready
    """
    phase: str = "init"  # init, setup, data, ready
    completed_steps: List[str] = field(default_factory=list)
    last_action: Optional[str] = None
    last_action_at: Optional[str] = None
    
    def to_dict(self) -> dict:
        return {
            "phase": self.phase,
            "completed_steps": self.completed_steps,
            "last_action": self.last_action,
            "last_action_at": self.last_action_at,
        }
    
    @classmethod
    def from_dict(cls, data: dict) -> WorkflowState:
        return cls(
            phase=data.get("phase", "init"),
            completed_steps=data.get("completed_steps", []),
            last_action=data.get("last_action"),
            last_action_at=data.get("last_action_at"),
        )
    
    def record_action(self, action: str, step: Optional[str] = None) -> None:
        """Record an action and optionally mark a step as completed."""
        self.last_action = action
        self.last_action_at = datetime.now(timezone.utc).isoformat()
        if step and step not in self.completed_steps:
            self.completed_steps.append(step)


@dataclass
class ResourceInfo:
    """Generic resource info with optional metadata."""
    id: str
    name: str
    description: Optional[str] = None
    tag: Optional[str] = None
    count: Optional[int] = None  # For input_set
    
    def to_dict(self) -> dict:
        data = {"id": self.id, "name": self.name}
        if self.description:
            data["description"] = self.description
        if self.tag:
            data["tag"] = self.tag
        if self.count is not None:
            data["count"] = self.count
        return data
    
    @classmethod
    def from_dict(cls, data: dict) -> ResourceInfo:
        return cls(
            id=data["id"],
            name=data["name"],
            description=data.get("description"),
            tag=data.get("tag"),
            count=data.get("count"),
        )


@dataclass
class Resources:
    """Collection of resource references with metadata."""
    project: Optional[ResourceInfo] = None
    scenario: Optional[ResourceInfo] = None
    input_set: Optional[ResourceInfo] = None
    bundle: Optional[ResourceInfo] = None
    
    def to_dict(self) -> dict:
        data = {}
        if self.project:
            data["project"] = self.project.to_dict()
        if self.scenario:
            data["scenario"] = self.scenario.to_dict()
        if self.input_set:
            data["input_set"] = self.input_set.to_dict()
        if self.bundle:
            data["bundle"] = self.bundle.to_dict()
        return data
    
    @classmethod
    def from_dict(cls, data: dict) -> Resources:
        return cls(
            project=ResourceInfo.from_dict(data["project"]) if data.get("project") else None,
            scenario=ResourceInfo.from_dict(data["scenario"]) if data.get("scenario") else None,
            input_set=ResourceInfo.from_dict(data["input_set"]) if data.get("input_set") else None,
            bundle=ResourceInfo.from_dict(data["bundle"]) if data.get("bundle") else None,
        )


@dataclass
class LocalContext:
    """
    Local context state stored in .fluxloop/context.json.
    
    This is a pointer to server resources, not a cache.
    Extended with workflow_state and resources for agent context awareness.
    """
    current_scenario: Optional[ScenarioContext] = None
    current_bundle: Optional[BundleContext] = None
    workflow_state: Optional[WorkflowState] = None
    resources: Optional[Resources] = None
    last_updated: Optional[str] = None
    
    def to_dict(self) -> dict:
        data = {}
        if self.current_scenario:
            data["current_scenario"] = self.current_scenario.to_dict()
        if self.current_bundle:
            data["current_bundle"] = self.current_bundle.to_dict()
        if self.workflow_state:
            data["workflow_state"] = self.workflow_state.to_dict()
        if self.resources:
            data["resources"] = self.resources.to_dict()
        data["last_updated"] = self.last_updated or datetime.now(timezone.utc).isoformat()
        return data
    
    @classmethod
    def from_dict(cls, data: dict) -> LocalContext:
        return cls(
            current_scenario=ScenarioContext.from_dict(data["current_scenario"]) 
                if data.get("current_scenario") else None,
            current_bundle=BundleContext.from_dict(data["current_bundle"]) 
                if data.get("current_bundle") else None,
            workflow_state=WorkflowState.from_dict(data["workflow_state"])
                if data.get("workflow_state") else None,
            resources=Resources.from_dict(data["resources"])
                if data.get("resources") else None,
            last_updated=data.get("last_updated"),
        )
    
    def ensure_workflow_state(self) -> WorkflowState:
        """Get or create workflow state."""
        if not self.workflow_state:
            self.workflow_state = WorkflowState()
        return self.workflow_state
    
    def ensure_resources(self) -> Resources:
        """Get or create resources."""
        if not self.resources:
            self.resources = Resources()
        return self.resources


# =============================================================================
# Workspace Directory Functions
# =============================================================================

def get_fluxloop_dir(base_dir: Optional[Path] = None) -> Path:
    """Get the .fluxloop workspace directory path."""
    base = base_dir or Path.cwd()
    workspace_root = find_workspace_root(base)
    if workspace_root:
        return workspace_root / FLUXLOOP_DIR_NAME
    return base / FLUXLOOP_DIR_NAME


def ensure_fluxloop_dir(base_dir: Optional[Path] = None) -> Path:
    """Ensure .fluxloop workspace directory exists."""
    fluxloop_dir = get_fluxloop_dir(base_dir)
    fluxloop_dir.mkdir(parents=True, exist_ok=True)
    return fluxloop_dir


def get_scenarios_dir(base_dir: Optional[Path] = None) -> Path:
    """Get the .fluxloop/scenarios directory path."""
    return get_fluxloop_dir(base_dir) / SCENARIOS_DIR_NAME


def ensure_scenarios_dir(base_dir: Optional[Path] = None) -> Path:
    """Ensure .fluxloop/scenarios directory exists."""
    scenarios_dir = get_scenarios_dir(base_dir)
    scenarios_dir.mkdir(parents=True, exist_ok=True)
    return scenarios_dir


def get_scenario_dir(scenario_name: str, base_dir: Optional[Path] = None) -> Path:
    """Get the directory for a specific scenario."""
    return get_scenarios_dir(base_dir) / scenario_name


def get_state_dir(scenario_dir: Path) -> Path:
    """Get the .state directory inside a scenario."""
    return scenario_dir / STATE_DIR_NAME


def ensure_state_dir(scenario_dir: Path) -> Path:
    """Ensure .state directory exists inside a scenario."""
    state_dir = get_state_dir(scenario_dir)
    state_dir.mkdir(parents=True, exist_ok=True)
    return state_dir


# =============================================================================
# Project Connection (workspace ↔ Web Project)
# =============================================================================

def get_project_json_path(base_dir: Optional[Path] = None) -> Path:
    """Get the path to .fluxloop/project.json."""
    return get_fluxloop_dir(base_dir) / PROJECT_JSON_FILENAME


def load_project_connection(base_dir: Optional[Path] = None) -> Optional[WebProjectConnection]:
    """Load Web Project connection from .fluxloop/project.json."""
    project_json_path = get_project_json_path(base_dir)
    
    if not project_json_path.exists():
        return None
    
    try:
        data = json.loads(project_json_path.read_text())
        return WebProjectConnection.from_dict(data)
    except (json.JSONDecodeError, KeyError, ValueError) as e:
        print(f"Warning: Failed to load project.json: {e}")
        return None


def save_project_connection(connection: WebProjectConnection, base_dir: Optional[Path] = None) -> Path:
    """Save Web Project connection to .fluxloop/project.json."""
    ensure_fluxloop_dir(base_dir)
    project_json_path = get_project_json_path(base_dir)
    project_json_path.write_text(json.dumps(connection.to_dict(), indent=2))
    return project_json_path


def select_web_project(
    project_id: str, 
    project_name: str, 
    api_url: str = "",
    base_dir: Optional[Path] = None
) -> WebProjectConnection:
    """Select a Web Project for this workspace."""
    connection = WebProjectConnection(
        project_id=project_id,
        project_name=project_name,
        api_url=api_url,
        connected_at=datetime.now(timezone.utc).isoformat(),
    )
    save_project_connection(connection, base_dir)
    return connection


def get_current_web_project_id(base_dir: Optional[Path] = None) -> Optional[str]:
    """Get current Web Project ID from project.json."""
    connection = load_project_connection(base_dir)
    return connection.project_id if connection else None


# =============================================================================
# Context Functions (current scenario)
# =============================================================================

def get_context_file_path(base_dir: Optional[Path] = None) -> Path:
    """Get the full path to context.json."""
    return get_fluxloop_dir(base_dir) / CONTEXT_JSON_FILENAME


def load_context(base_dir: Optional[Path] = None) -> Optional[LocalContext]:
    """Load local context from .fluxloop/context.json."""
    context_path = get_context_file_path(base_dir)
    
    if not context_path.exists():
        return None
    
    try:
        data = json.loads(context_path.read_text())
        return LocalContext.from_dict(data)
    except (json.JSONDecodeError, KeyError, ValueError) as e:
        print(f"Warning: Failed to load context: {e}")
        return None


def save_context(context: LocalContext, base_dir: Optional[Path] = None) -> Path:
    """Save local context to .fluxloop/context.json."""
    ensure_fluxloop_dir(base_dir)
    context_path = get_context_file_path(base_dir)
    context.last_updated = datetime.now(timezone.utc).isoformat()
    context_path.write_text(json.dumps(context.to_dict(), indent=2))
    return context_path


def clear_context(base_dir: Optional[Path] = None) -> None:
    """Delete the context file."""
    context_path = get_context_file_path(base_dir)
    if context_path.exists():
        context_path.unlink()


def set_scenario(
    scenario_id: str, 
    scenario_name: str, 
    local_path: Optional[str] = None,
    base_dir: Optional[Path] = None
) -> LocalContext:
    """Set the current scenario in local context."""
    context = load_context(base_dir) or LocalContext()
    
    # If scenario changes, clear bundle
    if context.current_scenario and context.current_scenario.id != scenario_id:
        context.current_bundle = None
    
    context.current_scenario = ScenarioContext(
        id=scenario_id, 
        name=scenario_name,
        local_path=local_path or f"{SCENARIOS_DIR_NAME}/{scenario_name}",
    )
    save_context(context, base_dir)
    return context


def set_bundle(bundle_id: str, bundle_version: str, base_dir: Optional[Path] = None) -> LocalContext:
    """Set the current bundle in local context."""
    context = load_context(base_dir) or LocalContext()
    context.current_bundle = BundleContext(id=bundle_id, version=bundle_version)
    save_context(context, base_dir)
    return context


def get_current_scenario_id(base_dir: Optional[Path] = None) -> Optional[str]:
    """Get current scenario ID from context."""
    context = load_context(base_dir)
    return context.current_scenario.id if context and context.current_scenario else None


def get_current_scenario(base_dir: Optional[Path] = None) -> Optional[ScenarioContext]:
    """Get current scenario from context."""
    context = load_context(base_dir)
    return context.current_scenario if context else None


# =============================================================================
# Workflow & Resource Helpers
# =============================================================================

def record_action(
    action: str,
    step: Optional[str] = None,
    phase: Optional[str] = None,
    base_dir: Optional[Path] = None
) -> LocalContext:
    """Record an action in workflow state."""
    context = load_context(base_dir) or LocalContext()
    workflow = context.ensure_workflow_state()
    workflow.record_action(action, step)
    if phase:
        workflow.phase = phase
    save_context(context, base_dir)
    return context


def set_resource_project(
    project_id: str,
    name: str,
    description: Optional[str] = None,
    base_dir: Optional[Path] = None
) -> LocalContext:
    """Set project in resources."""
    context = load_context(base_dir) or LocalContext()
    resources = context.ensure_resources()
    resources.project = ResourceInfo(id=project_id, name=name, description=description)
    save_context(context, base_dir)
    return context


def set_resource_scenario(
    scenario_id: str,
    name: str,
    description: Optional[str] = None,
    base_dir: Optional[Path] = None
) -> LocalContext:
    """Set scenario in resources."""
    context = load_context(base_dir) or LocalContext()
    resources = context.ensure_resources()
    resources.scenario = ResourceInfo(id=scenario_id, name=name, description=description)
    save_context(context, base_dir)
    return context


def set_resource_input_set(
    input_set_id: str,
    name: str,
    count: Optional[int] = None,
    tag: Optional[str] = None,
    base_dir: Optional[Path] = None
) -> LocalContext:
    """Set input_set in resources."""
    context = load_context(base_dir) or LocalContext()
    resources = context.ensure_resources()
    resources.input_set = ResourceInfo(
        id=input_set_id, name=name, count=count, tag=tag
    )
    save_context(context, base_dir)
    return context


def set_resource_bundle(
    bundle_id: str,
    version: str,
    description: Optional[str] = None,
    base_dir: Optional[Path] = None
) -> LocalContext:
    """Set bundle in resources."""
    context = load_context(base_dir) or LocalContext()
    resources = context.ensure_resources()
    resources.bundle = ResourceInfo(id=bundle_id, name=version, description=description)
    save_context(context, base_dir)
    return context


def get_workflow_phase(base_dir: Optional[Path] = None) -> str:
    """Get current workflow phase."""
    context = load_context(base_dir)
    if context and context.workflow_state:
        return context.workflow_state.phase
    return "init"


# =============================================================================
# Workspace Detection
# =============================================================================

def find_workspace_root(start_dir: Optional[Path] = None) -> Optional[Path]:
    """
    Find the workspace root by looking for .fluxloop directory.
    
    Searches from start_dir upward until finding .fluxloop/.
    
    Note: Home directory (~) is excluded from workspace detection.
    The global ~/.fluxloop/ is for auth/config only, not a project workspace.
    """
    current = (start_dir or Path.cwd()).resolve()
    home_dir = Path.home().resolve()
    
    for parent in [current] + list(current.parents):
        # Skip home directory - ~/.fluxloop is for global config, not workspace
        if parent == home_dir:
            continue
        if (parent / FLUXLOOP_DIR_NAME).exists():
            return parent
        if parent == parent.parent:
            break
    
    return None


def find_scenario_root(start_dir: Optional[Path] = None) -> Optional[Path]:
    """
    Find the scenario root by looking for .state directory.
    """
    current = (start_dir or Path.cwd()).resolve()
    
    for parent in [current] + list(current.parents):
        if (parent / STATE_DIR_NAME).exists():
            return parent
        if parent == parent.parent:
            break
    
    return None


# =============================================================================
# Backward Compatibility Aliases
# =============================================================================

# For commands that still use project terminology
def get_current_project_id(base_dir: Optional[Path] = None) -> Optional[str]:
    """Get current Web Project ID (alias for get_current_web_project_id)."""
    return get_current_web_project_id(base_dir)


def set_project(project_id: str, project_name: str, base_dir: Optional[Path] = None):
    """Set current project (alias for select_web_project)."""
    return select_web_project(project_id, project_name, base_dir=base_dir)
