"""
Templates for generating configuration and code files.
"""

from textwrap import dedent




def create_project_config(project_name: str) -> str:
    """Create default project-level configuration content."""

    return dedent(
        f"""
        # FluxLoop Project Configuration
        # ------------------------------------------------------------
        # Describes global metadata and defaults shared across the project.
        # Update name/description/tags to suit your workspace.
        name: {project_name}
        description: AI agent simulation project
        version: 1.0.0

        # FluxLoop VSCode extension will prompt to set this path
        source_root: ""

        # Optional collector settings (leave null if using offline mode only)
        collector_url: null
        collector_api_key: null

        # Tags and metadata help downstream tooling categorize experiments
        tags:
          - simulation
          - testing

        metadata:
          team: development
          environment: local
          service_context: ""
          # Describe the overall service scenario (used by multi-turn supervisor)
          # Add any custom fields used by dashboards or automation tools.
        """
    ).strip() + "\n"


def create_input_config() -> str:
    """Create default input configuration content."""

    return dedent(
        """
        # ===================================================================
        # Quick Configuration
        # - Define What would you ask to your agent? -
        # ===================================================================
        # Quick start: Fill in 1. BASE USER INPUT (required)
        # Optional fields use defaults if not specified
        # ===================================================================


        # -------------------------------------------------------------------
        # 1. BASE USER INPUT (REQUIRED)
        # -------------------------------------------------------------------
        # Start here: Write the main questions your users would ask.
        # This is your baseline - variations will be generated from this.
        # Example: "How do I get started?" or "Reset my password"

        base_inputs:
          - input: "How do I get started?"
            expected_intent: help


        # -------------------------------------------------------------------
        # 2. INPUT VARIATION STRATEGIES (OPTIONAL)
        # -------------------------------------------------------------------
        # How should the base input be transformed?
        # - rephrase: Same intent, different wording
        # - verbose: More detailed, longer questions
        # - typo: Common spelling mistakes
        # - concise: Shorter, more direct
        # Tip: Mix strategies to cover realistic user behavior patterns.

        variation_strategies:
          - rephrase
          - verbose
          - error_prone

        variation_count: 2           # How many variations per strategy
        variation_temperature: 0.7   # 0.0 = consistent, 1.0 = creative


        # -------------------------------------------------------------------
        # 3. PERSONA SETTING (OPTIONAL)
        # -------------------------------------------------------------------
        # 

        personas:
          - name: novice_user
            description: A user new to the system
            characteristics:
              - Asks basic questions
              - May use incorrect terminology
              - Needs detailed explanations
            language: en
            expertise_level: novice
            goals:
              - Understand system capabilities
              - Complete basic tasks
            # Tip: Add persona-specific context that can be injected into prompts.

          - name: expert_user
            description: An experienced power user
            characteristics:
              - Uses technical terminology
              - Asks complex questions
              - Expects efficient responses
            language: en
            expertise_level: expert
            goals:
              - Optimize workflows
              - Access advanced features
            # Tip: Include any tone/style expectations in characteristics.

        # ------------------------------------------------------------
        # Input generation settings
        # ------------------------------------------------------------

        inputs_file: inputs/generated.yaml

        input_generation:
          mode: llm
          llm:
            enabled: true
            provider: openai
            model: gpt-5-mini
            api_key: null
            # Replace provider/model/api_key according to your LLM setup.
        """
    ).strip() + "\n"


def create_simulation_config(project_name: str) -> str:
    """Create default simulation configuration content."""

    return dedent(
        f"""
        # FluxLoop Simulation Configuration
        # ------------------------------------------------------------
        # Controls how experiments execute (iterations, runner target, output paths).
        # Adjust runner module/function to point at your agent entry point.
        name: {project_name}_experiment
        description: AI agent simulation experiment

        iterations: 1           # Number of times to cycle through inputs/personas
        parallel_runs: 1          # Increase for concurrent execution (ensure thread safety)
        run_delay_seconds: 0      # Optional delay between runs to avoid rate limits
        seed: 42                  # Set for reproducibility; remove for randomness

        runner:
          module_path: "examples.simple_agent"
          function_name: "run"
          target: "examples.simple_agent:run"
          working_directory: .    # Relative to project root; adjust if agent lives elsewhere
          python_path:            # Optional custom PYTHONPATH entries
          timeout_seconds: 120   # Abort long-running traces
          max_retries: 3         # Automatic retry attempts on error

        replay_args:
          enabled: false
          recording_file: recordings/args_recording.jsonl
          override_param_path: data.content

        multi_turn:
          enabled: false              # Enable to drive conversations via supervisor
          max_turns: 8                # Safety cap on total turns per conversation
          auto_approve_tools: true    # Automatically approve tool calls when supported
          persona_override: null      # Force a specific persona id (optional)
          supervisor:
            provider: openai          # openai (LLM generated) | mock (scripted playback)
            model: gpt-5-mini
            system_prompt: |
              You supervise an AI assistant supporting customers.
              Review the entire transcript and decide whether to continue.
              When continuing, craft the next user message consistent with the persona.
              When terminating, explain the reason and provide any closing notes.
            metadata:
              scripted_questions: []  # Array of user utterances for sequential playback (e.g., ["First question", "Second question", ...])
              mock_decision: terminate        # Fallback when no scripted questions remain
              mock_reason: script_complete    # Termination reason for scripted runs
              mock_closing: "Thanks for the help. I have no further questions."

        output_directory: experiments
        save_traces: true
        save_aggregated_metrics: true
        """
    ).strip() + "\n"


def create_pytest_bridge_template(config_relative_path: str) -> str:
    """Return a ready-to-run pytest smoke test referencing the bridge fixtures."""

    return dedent(
        f"""
        \"\"\"FluxLoop Pytest smoke test generated via `fluxloop init pytest-template`.\"\"\"

        from pathlib import Path


        PROJECT_ROOT = Path(__file__).resolve().parents[1]
        SIMULATION_CONFIG = PROJECT_ROOT / "{config_relative_path}"


        def test_fluxloop_smoke(fluxloop_runner):
            result = fluxloop_runner(
                project_root=PROJECT_ROOT,
                simulation_config=SIMULATION_CONFIG,
                env={{"PYTHONPATH": str(PROJECT_ROOT)}},
            )
            result.require_success(label="fluxloop smoke")
        """
    ).strip() + "\n"


def create_sample_agent() -> str:
    """Create a sample agent implementation."""

    return dedent(
        '''
        """Sample agent implementation for FluxLoop testing."""

        import random
        import time
        from typing import Any, Dict

        import fluxloop


        @fluxloop.agent(name="SimpleAgent")
        def run(input_text: str) -> str:
            """Main agent entry point."""
            processed = process_input(input_text)
            response = generate_response(processed)
            time.sleep(random.uniform(0.1, 0.5))
            return response


        @fluxloop.prompt(model="simple-model")
        def generate_response(processed_input: Dict[str, Any]) -> str:
            intent = processed_input.get("intent", "unknown")
            responses = {
                "greeting": "Hello! How can I help you today?",
                "help": "I can assist you with various tasks. What would you like to know?",
                "capabilities": "I can answer questions, provide information, and help with tasks.",
                "demo": "Here's an example: You can ask me about any topic and I'll try to help.",
                "unknown": "I'm not sure I understand. Could you please rephrase?",
            }
            return responses.get(intent, responses["unknown"])


        @fluxloop.tool(description="Process and analyze input text")
        def process_input(text: str) -> Dict[str, Any]:
            text_lower = text.lower()

            intent = "unknown"
            if any(word in text_lower for word in ["hello", "hi", "hey"]):
                intent = "greeting"
            elif any(word in text_lower for word in ["help", "start", "begin"]):
                intent = "help"
            elif any(word in text_lower for word in ["can you", "what can", "capabilities"]):
                intent = "capabilities"
            elif "example" in text_lower or "demo" in text_lower:
                intent = "demo"

            return {
                "original": text,
                "intent": intent,
                "word_count": len(text.split()),
                "has_question": "?" in text,
            }


        if __name__ == "__main__":
            with fluxloop.instrument("test_run"):
                result = run("Hello, what can you help me with?")
                print(f"Result: {result}")
        '''
    ).strip() + "\n"


def create_gitignore() -> str:
    """Create a .gitignore file."""

    return dedent(
        """
        # Python
        __pycache__/
        *.py[cod]
        *$py.class
        *.so
        .Python
        venv/
        env/
        ENV/
        .venv/

        # FluxLoop
        traces/
        *.trace
        *.log

        # Environment
        .env
        .env.local
        *.env

        # IDE
        .vscode/
        .idea/
        *.swp
        *.swo

        # OS
        .DS_Store
        Thumbs.db

        # Testing
        .pytest_cache/
        .coverage
        htmlcov/
        *.coverage
        """
    ).strip() + "\n"


def create_env_file() -> str:
    """Create a .env template file."""

    return dedent(
        """
        # FluxLoop Configuration
        # API URL is managed via .fluxloop/project.json (set by 'fluxloop projects select')
        FLUXLOOP_SYNC_API_KEY=your-api-key-here
        FLUXLOOP_ENABLED=true
        FLUXLOOP_DEBUG=false
        FLUXLOOP_SAMPLE_RATE=1.0
        # Argument Recording (global toggle)
        FLUXLOOP_RECORD_ARGS=false
        FLUXLOOP_RECORDING_FILE=
        # Example: recordings/args_recording.jsonl (project-relative) or absolute path

        # Service Configuration
        FLUXLOOP_SERVICE_NAME=my-agent
        FLUXLOOP_ENVIRONMENT=development

        # LLM API Keys (for input generation and supervisor)
        OPENAI_API_KEY=
        ANTHROPIC_API_KEY=
        GEMINI_API_KEY=
        """
    ).strip() + "\n"


def create_agent_wrapper_template(agent_name: str = "my_agent") -> str:
    """Create an external agent wrapper template.
    
    This template wraps an external agent (located outside the FluxLoop project)
    with a FluxLoop-compatible interface. Users should modify:
    1. ORIGINAL_AGENT_PATH - path to the original agent code
    2. Import statement for the original agent
    3. The _call_original_agent() function to match their agent's interface
    """

    return dedent(
        f'''
        """
        FluxLoop Agent Wrapper Template
        ================================
        This file wraps an external agent with a FluxLoop-compatible interface.
        
        HOW TO USE:
        1. Set ORIGINAL_AGENT_PATH to your agent's location
        2. Update the import statement for your agent
        3. Modify _call_original_agent() to match your agent's interface
        4. Update configs/simulation.yaml:
           runner:
             module_path: "agents.{agent_name}"
             function_name: "run"
        """

        from __future__ import annotations

        import asyncio
        import sys
        from pathlib import Path
        from typing import Any, Optional

        import fluxloop


        # ============================================================
        # STEP 1: Configure path to your original agent
        # ============================================================
        # Adjust the path to point to your agent's root directory.
        # Example: If your agent is at ../my-bot/backend/, set:
        #   ORIGINAL_AGENT_PATH = FLUXLOOP_PROJECT_ROOT.parent / "my-bot" / "backend"

        FLUXLOOP_PROJECT_ROOT = Path(__file__).resolve().parents[1]
        ORIGINAL_AGENT_PATH = FLUXLOOP_PROJECT_ROOT.parent / "path" / "to" / "your" / "agent"

        if str(ORIGINAL_AGENT_PATH) not in sys.path:
            sys.path.insert(0, str(ORIGINAL_AGENT_PATH))


        # ============================================================
        # STEP 2: Import your original agent
        # ============================================================
        # Uncomment and modify based on your agent's structure:
        #
        # from your_package.agent import YourAgent
        # from your_package.core import get_agent_manager
        # from your_agent import run as original_run


        # ============================================================
        # STEP 3: Implement the bridge to your agent
        # ============================================================

        def _extract_prompt(
            *,
            input_text: Optional[str] = None,
            prompt: Optional[str] = None,
            data: Optional[dict[str, Any]] = None,
            **kwargs: Any,
        ) -> str:
            """Extract prompt from various input formats supported by FluxLoop."""
            candidates: list[Optional[str]] = [
                input_text,
                prompt,
                kwargs.get("input"),
                kwargs.get("text"),
                kwargs.get("content"),
            ]
            if data:
                candidates.extend([
                    data.get("content"),
                    data.get("input"),
                    data.get("prompt"),
                    data.get("text"),
                ])

            for candidate in candidates:
                if candidate is not None:
                    value = str(candidate).strip()
                    if value:
                        return value

            raise ValueError(
                "No prompt provided. Expected input_text, prompt, or data.content."
            )


        def _call_original_agent(prompt: str) -> str:
            """
            Call your original agent and return the response.
            
            MODIFY THIS FUNCTION to match your agent's interface.
            
            Examples:
            
            # Simple function call:
            return original_run(prompt)
            
            # Class-based agent:
            agent = YourAgent()
            return agent.respond(prompt)
            
            # Async agent (wrap with asyncio.run):
            async def _async_call():
                agent = YourAgent()
                return await agent.arespond(prompt)
            return asyncio.run(_async_call())
            
            # Agent manager pattern:
            manager = get_agent_manager()
            conversation_id, run_id = manager.start_run(prompt)
            result = asyncio.run(manager.get_result(run_id))
            return result.get("answer", "")
            """
            # TODO: Replace with your agent call
            raise NotImplementedError(
                "Please implement _call_original_agent() to call your agent. "
                "See the examples in the docstring above."
            )


        # ============================================================
        # FluxLoop Entry Point (usually no changes needed)
        # ============================================================

        @fluxloop.agent(name="{agent_name.title().replace('_', '')}")
        def run(
            input_text: Optional[str] = None,
            *,
            prompt: Optional[str] = None,
            data: Optional[dict[str, Any]] = None,
            **kwargs: Any,
        ) -> str:
            """FluxLoop-compatible entry point for the agent."""
            final_prompt = _extract_prompt(
                input_text=input_text,
                prompt=prompt,
                data=data,
                **kwargs,
            )
            return _call_original_agent(final_prompt)


        # ============================================================
        # Manual Testing
        # ============================================================

        if __name__ == "__main__":
            # Quick smoke test without FluxLoop CLI
            test_prompt = "Hello, can you help me?"
            print(f"Testing with: {{test_prompt}}")
            try:
                result = run(input_text=test_prompt)
                print(f"Result: {{result}}")
            except NotImplementedError as e:
                print(f"Setup needed: {{e}}")
        '''
    ).strip() + "\n"


def create_agents_readme() -> str:
    """Create README for agents directory."""

    return dedent(
        """
        # Agent Wrappers

        This directory contains FluxLoop-compatible agent wrappers.

        ## Purpose

        Agent wrappers bridge your original agent code with FluxLoop's testing framework.
        This keeps your original code untouched while enabling simulation and testing.

        ## Files

        - `_template_wrapper.py` - Template for creating new agent wrappers
        - `<your_agent>.py` - Your customized wrapper (copy from template)

        ## Quick Start

        1. Copy the template:
           ```bash
           cp _template_wrapper.py my_agent.py
           ```

        2. Edit `my_agent.py`:
           - Set `ORIGINAL_AGENT_PATH` to your agent's location
           - Update the import statement
           - Implement `_call_original_agent()` function

        3. Update `configs/simulation.yaml`:
           ```yaml
           runner:
             module_path: "agents.my_agent"
             function_name: "run"
           ```

        4. Test manually:
           ```bash
           python agents/my_agent.py
           ```

        5. Run FluxLoop test:
           ```bash
           fluxloop test
           ```

        ## Example

        See `_template_wrapper.py` for detailed instructions and examples.
        """
    ).strip() + "\n"