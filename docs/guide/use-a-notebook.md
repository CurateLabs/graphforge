# Use a notebook

**Advanced · Assumes basic Python, database, and terminal skills.**
Start with the [Advanced introduction](advanced.md) for GraphForge terminology,
or [Basic](overview.md#basic) for a guided first graph.

Use a local Jupyter notebook to build a graph, run queries, and inspect tables.
The first task is the same [basic citation graph](quickstart.md) used by scripts
and agents. The lesson uses the published `graphforge` Python package from
[Installation](installation.md#choose-a-runtime-for-your-task), locally or in a
[hosted notebook](#hosted-notebooks).

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
python -m pip install graphforge
python -m ipykernel install --sys-prefix --name graphforge --display-name "Python (GraphForge)"
jupyter lab
```

Keep this environment for the lesson; upgrading GraphForge mid-lesson changes
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
its intended kernel and run `%pip install --upgrade graphforge` in a cell.
This installs the latest release, replacing a different version if needed. Restart the kernel after changing the native package, then check it again.

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

### Select the hosted runtime

Install the published package into the notebook's Python environment:

```python
%pip install graphforge
```

Restart the kernel after installing the native package. Verify that
`graphforge.__version__` reports the release you installed and that
`from graphforge import FilesystemDurabilityWarning` succeeds before continuing.
The managed Colab lifecycle was qualified with a `0.6.0-rc.8` build
([receipt](https://github.com/CurateLabs/graphforge/issues/2332#issuecomment-6711013172));
Kaggle execution remains unqualified.

### Open session storage

Kaggle and Colab are supported with an unverified-durability warning when their
native package and filesystem primitive checks succeed. For example, use
`GraphForge("/content/project")` on Colab. The overlayfs or tmpfs classification
emits one `graphforge.FilesystemDurabilityWarning` per open, with
`code="GF_UNVERIFIED_FILESYSTEM"`. Closing and reopening emits a new warning;
writes emit none. `forge.open_warnings` retains its structured fields.

```python
import warnings
from graphforge import FilesystemDurabilityWarning, GraphForge

with warnings.catch_warnings(record=True) as observed:
    warnings.simplefilter("always", FilesystemDurabilityWarning)
    forge = GraphForge("/content/project")
    forge.execute("CREATE (:NotebookRun)")
    receipt = {"open_warnings": forge.open_warnings}
    forge.close()
```

The generation records the last writer's unverified qualification. Portable
export receipts and verification reports include `source_unverified_durability`
when that source generation is stamped. Keep that receipt alongside benchmark
results. Set `GRAPHFORGE_FILESYSTEM_DURABILITY=strict` before opening to retain
refusal of unproven classes; broken primitives and integrity checks still fail
in default warning mode.

This is session storage, not a persistence guarantee: tmpfs loses data on
reboot, and Colab/Kaggle VM disks lose data when a runtime is recycled. Retain
source inputs and export a complete portable package to persistent storage
before the session ends; a result table is not a project backup. Network/FUSE
mounts such as mounted cloud drives may still fail the primitive probe. Use
session disk for the project and copy the completed portable package out.
For the connected Colab write–close–reopen procedure and qualification receipts,
see [#2332](https://github.com/CurateLabs/graphforge/issues/2332). Local tmpfs and
split-device overlay proofs do not establish a hosted runtime result.

## Continue

- [Analytics integration](analytics-integration.md) for pandas, Polars, and algorithms.
- [Record and revisit an inquiry](record-an-inquiry.md) when a question needs a
  retained hypothesis, challenge, evidence, and conclusion.
