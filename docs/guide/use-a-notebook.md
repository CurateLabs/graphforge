# Use a notebook

**Advanced · Assumes basic Python, database, and terminal skills.**
Start with the [Advanced introduction](advanced.md) for GraphForge terminology,
or [Basic](overview.md#basic) for a guided first graph.

Use a local Jupyter notebook to build a graph, run queries, and inspect tables.
The first task is the same [basic citation graph](quickstart.md) used by scripts
and agents. This guide uses the published `0.6.0rc7` Python package selected in
[Installation](installation.md#choose-a-runtime-for-your-task).

## Start a local notebook

You need Python 3.10 or newer, a terminal, and a web browser. Open a terminal
in the folder where you want to keep your notebook, then create an environment:

```bash
python3 -m venv .venv
```

On Windows, use `py -3 -m venv .venv` instead. If environment creation fails,
install your Python distribution's venv and pip support before continuing.

Activate it with `source .venv/bin/activate` in a POSIX shell or
`.venv\Scripts\Activate.ps1` in PowerShell. In that activated terminal, install
the notebook tools and GraphForge:

```bash
python -m pip install jupyterlab ipykernel
python -m pip install "graphforge==0.6.0rc7"
python -m ipykernel install --sys-prefix --name graphforge --display-name "Python (GraphForge)"
jupyter lab
```

Keep this package version for the lesson so a moving prerelease cannot change
the engine underneath your notebook.

JupyterLab opens in your browser; if it does not, open the local URL printed
in the terminal. Keep that terminal running. In the launcher, create a notebook
using **Python (GraphForge)**. This kernel runs code with the environment you
just prepared. The [JupyterLab installation guide](https://jupyterlab.readthedocs.io/en/stable/getting_started/installation.html)
and [IPython kernel guide](https://ipython.readthedocs.io/en/stable/install/kernel_install.html)
cover other setups.

Run this first cell with **Shift+Enter**:

```python
import sys
import graphforge
print(sys.executable)
print(graphforge.__version__)
```

Check that the executable is inside your `.venv` and the package version is
the one you installed. If you already use another notebook environment, select
its intended kernel and run `%pip install "graphforge==0.6.0rc7"` in a cell.
This selects the lesson's exact package, replacing a different version if
needed. Restart the kernel after changing the native package, then check it again.

## Get your first graph answer

Copy each Python block from [Your first graph](quickstart.md) into a separate
cell and run them in order with **Shift+Enter**. The query cell prints Methods
with two citations and Replication with one. Inspect the Arrow table with
`result.to_pylist()` or display `result` in a cell. No pandas installation is
needed. Save the notebook with **File → Save Notebook**.

## Know what survives

`GraphForge()` keeps data in the running process. A kernel restart or session
reset discards that graph. Saving the notebook saves code and displayed output;
it does not save the live GraphForge instance.

To retain the graph, follow [Query, analyze, and save a graph](tutorial.md).
`GraphForge(path)` requires an existing directory on
[supported durable storage](installation.md#durable-storage). In a later kernel,
open the same path and run the query again. Keep the Python source and any
original input files needed to reproduce your work.

## Hosted notebooks

Kaggle and Colab remain runtime-unqualified. In the
[recorded access check](https://github.com/CurateLabs/graphforge/issues/1209#issuecomment-6639323044),
each available session was signed out and no authorized kernel was available.
That records an access limitation, not an unsupported-platform result. Native
installation, execution, filesystem admission, session reset and retention/export
were not tested there. A successful local Jupyter kernel does not establish any
of those hosted outcomes. Use the local notebook path above for the qualified
workflow; assess each hosted environment separately before relying on it.

If a hosted environment supports the native package, memory-only work remains
ephemeral. Do not bypass a `GF_UNSUPPORTED_FILESYSTEM` error to claim durable
support. Retain source inputs and explicitly export the results you need before
the session ends; a result table is not a complete project backup.

## Continue

- [Analytics integration](analytics-integration.md) for pandas, Polars, and algorithms.
- [Record and revisit an inquiry](record-an-inquiry.md) when a question needs a
  retained hypothesis, challenge, evidence, and conclusion.
