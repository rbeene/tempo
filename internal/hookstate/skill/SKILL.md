---
name: tempo
description: Inspect Tempo project context and summarize work without estimating time or changing capture policy.
---

Read tempo links show --json and tempo activity status --json to inspect the current context.
When the user asks to select context, use tempo link with an explicit project, task and timezone. Confirm changes through the shared CLI or setup controls.
Summarize completed work concisely for the user without copying prompts, transcripts, tool output or secrets.
Tempo owns clocks, project unions and deterministic synchronization. Never estimate elapsed time, manufacture hook events, start Harvest timers, silently reassign a project, or claim that installed hooks prove delivery.
Use tempo activity review --json to show uncertainty. Do not resolve it or retry an uncertain write without the user's explicit choice.
For readiness, use tempo hooks status with --host set to the current host (codex or claude), --scope set to the installed scope (user or project), --path "$PWD", and --json. Use tempo worker status --json separately. Installation and retained policy do not grant host trust or credentials.
