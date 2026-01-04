You are my multi channel writer, you are executing a controlled, multi-phase content generation process.

Source of truth:
- The currently open Markdown file is the canonical amplification draft.
- Do not invent new claims.
- Do not contradict the claims registry.
- Respect stated limitations and tradeoffs.

You will execute the following agents IN ORDER.
Each agent must explicitly state when it becomes active.
Each agent must write its output into a clearly separated Markdown section.

--------------------------------------------------
PHASE 0 — INPUT CONFIRMATION
--------------------------------------------------
State:
"Canonical draft loaded. Publishing Cycle v2 active."

Briefly summarize (5 bullets max):
- Core problem
- Primary angle
- Intended audience
- Tutorial scope
- Explicit non-goals

--------------------------------------------------
PHASE 1 — LINKEDIN DRAFT AGENT v1
--------------------------------------------------
Act as LinkedIn Draft Agent v1.
Instructions are defined in:
examples/agents/linkedin-draft-agent.md

Output requirements:
- One LinkedIn-ready post
- Clear hook in first 2 lines
- Explicit CTA to tutorial
- Emphasize: "No operations overhead when adding new Kafka applications"
- No hashtags beyond 5

Write output under:
## LinkedIn Draft

--------------------------------------------------
PHASE 2 — BLOG / MEDIUM DRAFT AGENT v1
--------------------------------------------------
Act as Blog Draft Agent v1.
Instructions are defined in:
examples/agents/blog-draft-agent.md

Output requirements:
- Full long-form article
- Use headings
- Reference exercises E10–E40
- Explicit tradeoff section
- Neutral, technical tone

Write output under:
## Blog / Medium Draft

--------------------------------------------------
PHASE 3 — NEWSLETTER DRAFT AGENT v1
--------------------------------------------------
Act as Newsletter Draft Agent v1.
Instructions are defined in:
examples/agents/newsletter-draft-agent.md

Output requirements:
- First-person voice
- Reflective tone
- Emphasize architectural insight
- End with “Why this matters now”

Write output under:
## Newsletter Draft

--------------------------------------------------
PHASE 4 — TALK ABSTRACT AGENT v1
--------------------------------------------------
Act as Talk Abstract Agent v1.
Instructions are defined in:
examples/agents/talk-abstract-agent.md

Output requirements:
- Conference-ready abstract
- Explicit audience definition
- Clear learning outcomes

Write output under:
## Talk Abstract

--------------------------------------------------
PHASE 5 — CLAIMS DRIFT SENTINEL v1
--------------------------------------------------
Act as Claims Drift Sentinel v1.
Instructions are defined in:
examples/agents/claims-drift-sentinel.md

Verify:
- No new claims introduced
- All KS-* references are valid
- No overpromising language

Write output under:
## Claims Drift Report

--------------------------------------------------
PHASE 6 — EDITORIAL READINESS REVIEW v1
--------------------------------------------------
Act as Editorial Coherence Reviewer v1.
Instructions are defined in:
examples/agents/editorial-coherence-reviewer.md

Assess:
- Consistency across channels
- Tone alignment
- Message clarity

Write output under:
## Editorial Readiness

--------------------------------------------------
PHASE 7 — DISTRIBUTION READINESS AGENT v1
--------------------------------------------------
Act as Distribution Readiness Agent v1.
Instructions are defined in:
examples/agents/distribution-readiness-agent.md

Produce:
- Channel checklist
- Suggested publishing order
- Reuse strategy

Write output under:
## Distribution Plan

--------------------------------------------------
PHASE 8 — FILE GENERATION
--------------------------------------------------
After completing all phases, save the following files to:
examples/publication-drafts/

Files to create:
1. linkedin-draft__[tutorial-name].md
   - Include LinkedIn post content
   - Add publishing notes (character count, UTM tracking, checklist)

2. newsletter-draft__[tutorial-name].md
   - Include newsletter content
   - Add subject line, word count, publishing notes

3. talk-abstract__[tutorial-name].md
   - Include talk abstract, title, key takeaways
   - Add target audience, technical level, CFP checklist

4. distribution-checklist__[YYYY-MM-DD].md
   - Include complete distribution plan
   - Pre-publication verification
   - Channel-specific checklists
   - Publishing schedule
   - Success metrics

Note: The blog draft should already exist from Blog Draft Agent execution.
If not, create blog-draft__[tutorial-name].md as well.

--------------------------------------------------
FINAL STEP
--------------------------------------------------
After saving all files, list the generated file paths and end with:

"Publishing Cycle v2 complete. Drafts saved to examples/publication-drafts/

Generated files:
- linkedin-draft__[name].md
- newsletter-draft__[name].md
- talk-abstract__[name].md
- distribution-checklist__[date].md
- blog-draft__[name].md (if created)

All drafts ready for human review."

Do NOT continue beyond this point.