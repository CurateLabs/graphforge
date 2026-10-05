# Work with an agent

Start by asking a coding agent to build a small graph and answer a question.
The agent needs permission and tools to run Python in your environment. A chat
window without local execution cannot run GraphForge for you.

This path uses the public Python API. It does not require an agent framework,
model account supplied by GraphForge, or research workspace.

**Basic · You bring the question and check the evidence; the agent runs the code.**

## Get your first answer

Choose a coding assistant that can work in a folder on your computer and run
Python. If it is not already set up, ask a technical colleague or computer
support person to help. GraphForge does not provide that assistant or its account.

Give the assistant [Installation](installation.md) and
[Your first graph](quickstart.md), then use this prompt:

> Help me run the first-graph example. Check the installation instructions and
> use an environment with the selected GraphForge version. Save the example as
> first_graph.py and run it. Show the actual returned rows, and explain why
> Methods has two citations and Survey is absent. Tell me what disappears when
> the program closes. If setup fails, report the error and the help I need.
> Stop after this graph task.

You should see Methods with **two** citations and Replication with **one**.
Compare that answer with the table in the lesson. An explanation alone does not
show that the agent ran the query; ask to see its execution result. Keep
`first_graph.py`, the file of instructions, to repeat the example. Its temporary
graph disappears when the program exits.

<details>
<summary>Setup instructions for the agent or a technical helper</summary>

Use an empty working folder and Python 3.10 or newer. Create an environment:

```bash
python3 -m venv .venv
```

On Windows use `py -3 -m venv .venv`. Activate it with
`source .venv/bin/activate` in a POSIX shell or
`.venv\Scripts\Activate.ps1` in PowerShell. If creation fails, install the
Python distribution's venv and pip support. Then run:

```bash
python -m pip install graphforge==0.6.0
python -c "import sys, graphforge; print(sys.executable); print(graphforge.__version__)"
```

The final-version command requires v0.6.0 to be published. Check
[Installation](installation.md) first; for a published candidate substitute its
exact PyPI version. Give the agent the printed interpreter path: a new shell
may not keep the environment activation from the previous command.

</details>

## Continue with your research

[Your first mixed-methods project](first-research-project.md) connects survey
responses to interview excerpts, challenges an explanation, and saves a finding
you can retrieve later. You decide whether the evidence supports the conclusion.
An agent can help operate GraphForge; its explanation is also open to challenge.

## Optional Advanced tools

The following integrations assume basic terminal and development-tool skills.
You do not need them to complete the Basic lessons. See [Advanced](advanced.md)
when you want to operate the tools yourself.

## Work in VS Code

The optional [GraphForge extension](vscode-extension/) provides project and
result views. Follow its [runtime setup guide](vscode-extension/install.md).
A coding agent can also use the Python path from the editor's terminal.

Structured extension commands require an agent integration that can call
VS Code's command API. Some commands still need a human to select an item or
complete a dialog. Consult [agent interop](vscode-extension/agent-interop.md)
for that boundary; installing the extension does not connect every chat agent
automatically.

Packaged editor and agent first-use qualification for v0.6.0 is tracked in
[#1209](https://github.com/CurateLabs/graphforge/issues/1209). This shell-based
Python route does not establish that any branded agent integration is qualified.

## Add repository guidance when useful

For work in a Git repository on supported durable storage, the Python CLI
launcher can initialize the project and install repository-local skills:

```bash
graphforge init
graphforge skills status
```

Initialization creates GraphForge repository definitions and manages data
exclusions. It is optional for the in-memory quickstart. Existing projects can
use `graphforge skills install`. See [repository integration](repository-integration.md)
for managed files and updates.

The installed skills teach repository setup and knowledge construction. Their
version compatibility must match the engine; release qualification includes
updating and testing that compatibility for v0.6.0. Skills are instructions,
not a connection or permission system for an agent.

## Continue with a specific task

Ask the agent to [save and reopen the graph](tutorial.md) when you want durable
work. Later, ask it to [record an inquiry](record-an-inquiry.md): state the
hypothesis, challenge it, show the evidence, record the bounded conclusion,
and retrieve it before a related inquiry. You can use these steps without
learning the collaboration features.
