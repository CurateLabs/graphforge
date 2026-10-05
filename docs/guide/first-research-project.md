# Your first mixed-methods project

**Question: what might help students attend a study group?**

**Basic · No programming prerequisite.** This lesson is for a first social sciences project. You will read a small survey,
connect it to interview excerpts, challenge an explanation, and save a conclusion
you can find again. Every person, response, and quotation here is **invented for
teaching**. You can understand the worked example before running any code.

Mixed methods brings numerical and qualitative evidence together to answer a
question. Putting two files in the same folder is only preparation; the important
step is explaining what the two kinds of evidence mean together. The
[UK Data Service introduction](https://ukdataservice.ac.uk/help/data-types/qualitative-mixed-methods/)
shows examples of linked survey and interview data.

GraphForge helps keep those connections inspectable. It does not choose your
research design, interpret interviews for you, or establish that a claim is true.
For a small table of totals, a spreadsheet may be sufficient. GraphForge becomes
useful when you want to follow a result back to the people, excerpts, and coding
decisions that informed it.

## Choose how to follow along

First try [Your first graph](quickstart.md) to see three items connected by links.
A **graph** here is a collection of items and their connections, not a bar chart.
Then return to this lesson. You do not need research Branches or an ontology.

- **With an agent:** use [Work with an agent](work-with-an-agent.md). An agent is
  a coding assistant that can run commands, not just answer questions in chat.
- **In a notebook:** use [Use a notebook](use-a-notebook.md). A notebook is a file
  containing short pieces of code, their results, and your notes.
- **Just reading:** work through the tables below and the four check questions.
  Reading a printed example does not mean you have executed it.

The setup guides explain how to select a Python environment and supported
storage location. The current engine requires a Python/Node environment; this
guide is not a point-and-click research application.

## 1. Define what you are comparing

Our fictional survey asks whether each student attended a study group **last
week**. For this exercise, a “long commute” means at least 40 minutes one way.
That threshold is a teaching choice, not a universal definition.

| Participant | Commute group | Reported attending last week? |
| ----------- | ------------- | ----------------------------- |
| P01         | Long          | Yes                           |
| P02         | Long          | No                            |
| P03         | Long          | No                            |
| P04         | Short         | Yes                           |
| P05         | Short         | Yes                           |
| P06         | Short         | Yes                           |

One row represents one person. P01 is an identifier used to link that person's
survey response and interview, not their name. In a real project, decide how
people are recruited, what questions mean, which records belong, and how missing
answers are recorded before calculating results. **No answer is different from
“No.”** All six responses are present in this example.

## 2. Read and label the interview evidence

Four of those six people also have a fictional interview excerpt. A qualitative
**code** is a short label you assign to part of a text. It is an interpretation
used to organize your reading, not computer code and not a conclusion by itself.

| Participant | Fictional excerpt                                 | Assigned code and reason                                    |
| ----------- | ------------------------------------------------- | ----------------------------------------------------------- |
| P01         | “I can join when the session follows my lecture.” | `schedule_fit`: the meeting fits an existing campus visit   |
| P02         | “The last bus leaves before the group ends.”      | `travel_timing`: transport timing conflicts with attendance |
| P03         | “My paid shift starts when the group meets.”      | `work_schedule`: paid work overlaps the meeting             |
| P04         | “I stayed because a friend invited me.”           | `peer_invitation`: an invitation influenced participation   |

The example author assigned these codes; GraphForge did not discover them.
A real project needs a documented coding approach: retain the original text,
explain why a label applies, note who assigned it, and record changes. A different
reading may be reasonable. Read surrounding transcript context before adopting
a label. Four short excerpts do not constitute a complete thematic analysis.

P05 and P06 have **no interview here**. That does not tell us they experienced
no barriers. Also, three quotations from one person would still be one survey
respondent. Count the unit your question asks about.

GraphForge represents these links as:

```text
Participant P02 → said → excerpt about the last bus → coded as → travel_timing
```

A **node** is one item, such as a participant or excerpt. A **relationship** is
a named link, such as “said.” A **property** is a recorded detail, such as an
excerpt's text. A link records what you have connected; it does not establish
cause and effect.

## 3. Bring the two kinds of evidence together

The survey counts give **1 of 3** long-commute respondents and **3 of 3**
short-commute respondents reporting attendance. The denominator is the number
of survey respondents in each group, not the number interviewed.

The following combined table is a **joint display**: it places the numerical
pattern beside the qualitative evidence so you can consider agreement,
differences, and possible explanations.

| Numerical observation                     | Interview evidence                                            | Interpretation to investigate                                                                                |
| ----------------------------------------- | ------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------ |
| 1 of 3 long-commute respondents attended  | P02 mentions the last bus; P03 mentions a work shift          | Timing may matter in different ways; commute length alone may be an incomplete explanation                   |
| One long-commute respondent did attend    | P01 says joining is possible when a session follows a lecture | Existing campus visits may make attendance easier; this challenges “long commutes always prevent attendance” |
| 3 of 3 short-commute respondents attended | P04 mentions a friend's invitation; P05/P06 have no excerpts  | Social invitation is another possible explanation; we cannot assign P04's reason to everyone                 |

This is an illustration of integration, not a statistical test. These six
invented observations support no population estimate or causal claim. In an
actual project, your sampling, measurement, and analysis plan determine what
you can conclude. GraphForge can organize evidence for that work; choose any
statistical testing appropriate to your research design.

## 4. Challenge the explanation before writing the conclusion

Try the claim: **“No long-commute student in this dataset attended.”** P01 is
a counterexample, so the claim is false within this dataset. A verified
counterexample is enough to reject that absolute claim. Finding none would
require checking data coverage before concluding an absence.

An exploratory mixed-methods project need not begin with a statistical null
hypothesis. If your analysis plan requires one, specify its population, measures, and
test separately. Failure to reject a statistical null is not proof of no effect.

The example saves this authored **summary**:

> In these six fictional responses, 1 of 3 long-commute participants and
> 3 of 3 short-commute participants reported attending. Interview excerpts
> suggest timing, work, and invitation as explanations to investigate.
> P01 shows that a long commute does not always prevent attendance.

It records three separate fields beside that summary:

- **Scope:** “week 1; six survey respondents; four interviewed”
- **Limitation:** “Six invented survey responses and four invented excerpts teach a method; they establish no population pattern or causal effect.”
- **Next question:** “Would a different meeting time help, and for whom?”

The numbers, excerpts, codes, interpretation, and limitation should remain
separate and connected. Keeping them together lets someone challenge your
reasoning instead of only reading its final sentence.

## 5. Run, save, and revisit the example

Give your coding agent this page and the [example file](../../examples/mixed-methods/first_project.py), then ask:

> Use my selected Python environment to run this fictional project. Show the
> actual counts and every interview excerpt. Explain the denominator and P01's
> counterexample. Reopen the saved project and retrieve its scope, limitation,
> and next question. Keep the author's interpretation distinct from the
> engine's output, and report any failed step.

You should see the same counts and interpretation when the saved project is
opened again. Ask to see the source excerpt and coding reason behind each label,
and the limitation attached to the conclusion.

<details>
<summary>Execution instructions for the agent or an Advanced reader</summary>

The [complete Python example](../../examples/mixed-methods/first_project.py)
contains exactly the fictional records above. Download its raw contents and
save them as `first_project.py` in your working folder. Use the Python environment
from your selected setup guide and a folder on
[supported durable storage](installation.md#durable-storage).

In a terminal, with that environment active:

```bash
python first_project.py create study-project
```

This creates a new folder named `study-project`, prints the counts and linked
excerpts, and saves the finding. If that folder already exists, use the `review`
command below or choose a different new folder; creation refuses to overwrite
or duplicate your work. The first output is:

```text
Survey summary (people, not quotations):
long: 1 attended / 3 respondents
short: 3 attended / 3 respondents
```

Next it prints P01–P04's excerpts, codes, coding reasons, source labels, and who assigned the codes, then the saved finding's summary,
scope, limitation, and next question. The interpretation is explicitly authored
in the script; the engine does not generate or approve it.

The saved finding output is:

```text
Saved finding:
summary: In these six fictional responses, 1 of 3 long-commute participants and 3 of 3 short-commute participants reported attending. Interview excerpts suggest timing, work, and invitation as explanations to investigate. P01 shows that a long commute does not always prevent attendance.
scope: week 1; six survey respondents; four interviewed
limitation: Six invented survey responses and four invented excerpts teach a method; they establish no population pattern or causal effect.
next_question: Would a different meeting time help, and for whom?
```

Close the terminal or notebook. Later, return to the same working folder,
activate the same Python environment, and run:

```bash
python first_project.py review study-project
```

You should see the same results. `study-project` holds the saved graph;
`first_project.py` holds the instructions and fictional source records. Keep
both. A saved chat transcript is not a substitute for the project.

In a notebook using the prepared kernel, run `%run first_project.py create study-project`
and, in a later session, `%run first_project.py review study-project` instead.

</details>

## Check your understanding

Before adapting the example, explain these in your own words:

1. Why is the long-commute denominator three when we have only four interviews overall?
2. What evidence challenges the claim that long commutes always prevent attendance?
3. Why can't P04's explanation be assigned to P05 and P06?
4. Where would you find the original excerpt, coding decision, and limitation after reopening?

For real participant data, follow your project's consent and storage requirements.
Only pass material to an external agent service when permitted; a local graph
engine does not make an external chat service local.

You have completed this Basic lesson. The following optional [Advanced](advanced.md) tasks
assume introductory Python, database, and terminal skills. Continue with [recording and finding inquiries](record-an-inquiry.md),
[keeping an exact research state](research-journey.md), or [moving a saved project](portable-projects.md)
only when your next task needs them. This guide teaches an inspectable example;
actual first-use usability remains part of [first-use qualification](https://github.com/CurateLabs/graphforge/issues/1209).
