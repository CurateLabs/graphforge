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
python -m pip install graphforge
python -c "import sys, graphforge; print(sys.executable); print(graphforge.__version__)"
```

This lesson installs the published `graphforge` Python package; the engine
prints its version, such as `0.6.0`. Give the agent the printed interpreter
path: a new shell may not keep the environment activation from the previous
command. [Installation](installation.md#choose-a-runtime-for-your-task) lists
the other runtimes.

</details>

## Continue with your research

[Your first mixed-methods project](first-research-project.md) connects survey
responses to interview excerpts, challenges an explanation, and saves a finding
you can retrieve later. You decide whether the evidence supports the conclusion.
An agent can help operate GraphForge; its explanation is also open to challenge.

## Save work when you need it

Ask the agent to follow [save and reopen](tutorial.md) when you want a durable
graph. Ask it to close the program, reopen the saved project, and show the
returned rows again. Keep the source instructions as well as the project.

You can stop here. When your task needs editor commands, repository skills,
Hub automation, credentials, or error handling, continue with
[Automate GraphForge with an agent](agent-automation.md) in the Advanced path.
Those tools assume basic terminal and development skills.
