# ==========================================
# Documentation Review Makefile
# ==========================================
#
# Usage:
#   make help
#   make review-eaat TUTORIAL=path/to/tutorial
#   make review-didactics TUTORIAL=path/to/tutorial
#   make review-tech TUTORIAL=path/to/tutorial
#   make review-publish TUTORIAL=path/to/tutorial
#   make review TUTORIAL=path/to/tutorial
#
# Optional:
#   LLM_CMD=command (default: cat)
#

.PHONY: help review review-eaat review-didactics review-tech review-publish claims-check generate-publish

LLM_CMD ?= cat
DRAFT_DIR := /Users/kamir/GITHUB.kamir/kafscale-publications/PLAN/backlog/drafts
DATE := $(shell date +%Y-%m-%d)
TUTORIAL_ID := $(notdir $(TUTORIAL))
# DRAFT_FILE := $(DRAFT_DIR)/$(DATE)__$(TUTORIAL_ID)__draft.md
DRAFT_FILE := 2026-01-04__101_kafscale-dev-guide__draft

generate-publish:
	@echo ""
	@echo "=========================================="
	@echo " GENERATE PRE-CHANNEL DRAFT"
	@echo "=========================================="
	@echo ""
	@echo "Draft will be written to:"
	@echo "  $(DRAFT_FILE)"
	@echo ""
	@mkdir -p $(DRAFT_DIR)
	@echo "# Draft — $(TUTORIAL_ID)" > $(DRAFT_FILE)
	@echo "" >> $(DRAFT_FILE)
	@cat agents/tutorial-amplifier-agent.md >> $(DRAFT_FILE)
	@echo "" >> $(DRAFT_FILE)
	@echo "## Context" >> $(DRAFT_FILE)
	@echo "" >> $(DRAFT_FILE)
	@echo "Tutorial path: $(TUTORIAL)" >> $(DRAFT_FILE)
	@echo "" >> $(DRAFT_FILE)
	@echo "### Instructions" >> $(DRAFT_FILE)
	@echo "" >> $(DRAFT_FILE)
	@echo "Generate the draft content below this line." >> $(DRAFT_FILE)
	@echo "" >> $(DRAFT_FILE)
	@echo "---" >> $(DRAFT_FILE)
	@echo ""
	@echo "Next step:"
	@echo "Open this file in Claude Code and generate content."

# ------------------------------------------
# Help
# ------------------------------------------

help:
	@echo ""
	@echo "Documentation Review Targets"
	@echo "============================"
	@echo ""
	@echo "Required variable:"
	@echo "  TUTORIAL=path/to/tutorial-folder"
	@echo ""
	@echo "Optional:"
	@echo "  LLM_CMD=command (default: cat)"
	@echo ""
	@echo "Targets:"
	@echo ""
	@echo "  make review-eaat TUTORIAL=path"
	@echo "      Run EAAT (Experience, Authority, Accuracy, Trust)"
	@echo ""
	@echo "  make review-didactics TUTORIAL=path"
	@echo "      Run didactical / learning-flow review"
	@echo ""
	@echo "  make review-tech TUTORIAL=path"
	@echo "      Run technical accuracy & claims verification"
	@echo ""
	@echo "  make review-publish TUTORIAL=path"
	@echo "      Run publishing/promotion draft generator"
	@echo ""
	@echo "  make review TUTORIAL=path"
	@echo "      Run ALL reviews (EAAT + Technical + Didactics + Publishing)"
	@echo ""

# ------------------------------------------
# EAAT Review
# ------------------------------------------

review-eaat:
	@echo ""
	@echo "=========================================="
	@echo " EAAT REVIEW"
	@echo "=========================================="
	@echo ""
	@echo "EAAT Review Agent v1 active"
	@echo ""
	@cat examples/agents/eaat-review.md | \
	$(LLM_CMD) --context "$(TUTORIAL)" || true

# ------------------------------------------
# Didactical Review
# ------------------------------------------

review-didactics:
	@echo ""
	@echo "=========================================="
	@echo " DIDACTICAL REVIEW"
	@echo "=========================================="
	@echo ""
	@echo "Didactical Review Agent active"
	@echo ""
	@cat examples/agents/didactical-review.md | \
	$(LLM_CMD) --context "$(TUTORIAL)" || true

# ------------------------------------------
# Technical Accuracy / Claims Review
# ------------------------------------------

review-tech:
	@echo ""
	@echo "=========================================="
	@echo " TECHNICAL ACCURACY / CLAIMS REVIEW"
	@echo "=========================================="
	@echo ""
	@echo "Technical Accuracy Review Agent v1 active"
	@echo ""
	@cat examples/agents/technical-accuracy-review.md | \
	$(LLM_CMD) --context "$(TUTORIAL)" || true

# ------------------------------------------
# Publishing / Promotion Drafts
# ------------------------------------------

review-publish:
	@echo ""
	@echo "=========================================="
	@echo " PUBLISHING / PROMOTION DRAFTS"
	@echo "=========================================="
	@echo ""
	@echo "Publishing Channel Agent v1 active"
	@echo ""
	@echo "Goal: Produce channel-ready drafts promoting this tutorial"
	@echo "and its exercise set (E10/E20/E30/E40) with clear CTA."
	@echo ""

	@mkdir -p review-output
	@mkdir -p /Users/kamir/GITHUB.kamir/kafscale-publications/PLAN/backlog/drafts

	@echo "# Publishing Drafts" > review-output/publishing_drafts.md
	@echo "" >> review-output/publishing_drafts.md
	@echo "**Tutorial:** $(TUTORIAL)" >> review-output/publishing_drafts.md
	@echo "" >> review-output/publishing_drafts.md

	@echo "## Agent Instructions" >> review-output/publishing_drafts.md
	@cat agents/tutorial-amplifier-agent.md >> review-output/publishing_drafts.md

	@echo "" >> review-output/publishing_drafts.md
	@echo "## Tutorial Context" >> review-output/publishing_drafts.md
	@echo "_Source path: $(TUTORIAL)_" >> review-output/publishing_drafts.md

	@cp review-output/publishing_drafts.md \
		/Users/kamir/GITHUB.kamir/kafscale-publications/PLAN/backlog/drafts/
	@echo ""
	@echo "Draft copied to WritingOS backlog:"
	@echo "→ /Users/kamir/GITHUB.kamir/kafscale-publications/PLAN/backlog/drafts/publishing_drafts.md"

# ------------------------------------------
# Claims Reference Scan
# ------------------------------------------

claims-check:
	@echo ""
	@echo "Checking claim references (KS-*)"
	@echo "------------------------------------------"
	@grep -R "KS-" ./$(TUTORIAL) | grep -v claims || true

# ------------------------------------------
# Full Review (All Agents)
# ------------------------------------------

review:
	@echo ""
	@echo "=========================================="
	@echo " FULL REVIEW: EAAT + TECH + DIDACTICS + PUBLISHING"
	@echo "=========================================="
	@echo ""
	@$(MAKE) review-eaat TUTORIAL=$(TUTORIAL)
	@$(MAKE) review-tech TUTORIAL=$(TUTORIAL)
	@$(MAKE) review-didactics TUTORIAL=$(TUTORIAL)
	@$(MAKE) review-publish TUTORIAL=$(TUTORIAL)


# ------------------------------------------
# Publishing Cycle Planning
# ------------------------------------------

publish-plan:
	@echo ""
	@echo "=========================================="
	@echo " PUBLISHING CYCLE PLANNING"
	@echo "=========================================="
	@echo ""
	@echo "Preparing publishing cycle planning artifact"
	@echo ""

	@mkdir -p review-output

	@echo "# Publishing Cycle Plan" > review-output/publish_plan.md
	@echo "" >> review-output/publish_plan.md
	@echo "> Publishing Cycle Planner v1 active" >> review-output/publish_plan.md
	@echo "" >> review-output/publish_plan.md

	@echo "## Canonical Draft Context" >> review-output/publish_plan.md
	@echo "" >> review-output/publish_plan.md
	@echo "Draft file:" >> review-output/publish_plan.md
	@echo "\`\`\`" >> review-output/publish_plan.md
	@echo "$(DRAFT_FILE)" >> review-output/publish_plan.md
	@echo "\`\`\`" >> review-output/publish_plan.md
	@echo "" >> review-output/publish_plan.md

	@echo "## Agent Instructions" >> review-output/publish_plan.md
	@echo "" >> review-output/publish_plan.md
	@cat agents/publishing-cycle-planner.md >> review-output/publish_plan.md

	@echo "" >> review-output/publish_plan.md
	@echo "---" >> review-output/publish_plan.md
	@echo "" >> review-output/publish_plan.md
	@echo "### Action" >> review-output/publish_plan.md
	@echo "Open this file in Claude Code and generate the publishing plan below." >> review-output/publish_plan.md

	@echo ""
	@echo "Publishing plan prepared:"
	@echo "→ review-output/publish_plan.md"



publish-generate:
	@echo ""
	@echo "=========================================="
	@echo " CHANNEL DRAFT GENERATION"
	@echo "=========================================="
	@echo ""
	@echo "Preparing channel draft generation artifact"
	@echo ""

	@mkdir -p review-output

	@echo "# Channel Draft Generation" > review-output/channel_drafts.md
	@echo "" >> review-output/channel_drafts.md
	@echo "> Channel Draft Generator v1 active" >> review-output/channel_drafts.md
	@echo "" >> review-output/channel_drafts.md

	@echo "## Canonical Draft Context" >> review-output/channel_drafts.md
	@echo "" >> review-output/channel_drafts.md
	@echo "Source draft:" >> review-output/channel_drafts.md
	@echo "\`\`\`" >> review-output/channel_drafts.md
	@echo "$(DRAFT_FILE)" >> review-output/channel_drafts.md
	@echo "\`\`\`" >> review-output/channel_drafts.md
	@echo "" >> review-output/channel_drafts.md

	@echo "## Agent Instructions" >> review-output/channel_drafts.md
	@echo "" >> review-output/channel_drafts.md
	@cat agents/channel-draft-generator.md >> review-output/channel_drafts.md

	@echo "" >> review-output/channel_drafts.md
	@echo "---" >> review-output/channel_drafts.md
	@echo "" >> review-output/channel_drafts.md
	@echo "### Action" >> review-output/channel_drafts.md
	@echo "Generate channel-specific drafts below this line." >> review-output/channel_drafts.md

	@echo ""
	@echo "Prepared:"
	@echo "→ review-output/channel_drafts.md"


publish-verify:
	@echo ""
	@echo "=========================================="
	@echo " CLAIMS DRIFT VERIFICATION"
	@echo "=========================================="
	@echo ""
	@echo "Preparing claims drift verification artifact"
	@echo ""

	@mkdir -p review-output

	@echo "# Claims Drift Verification" > review-output/claims_drift_report.md
	@echo "" >> review-output/claims_drift_report.md
	@echo "> Claims Drift Sentinel v1 active" >> review-output/claims_drift_report.md
	@echo "" >> review-output/claims_drift_report.md

	@echo "## Canonical Draft Context" >> review-output/claims_drift_report.md
	@echo "" >> review-output/claims_drift_report.md
	@echo "Source draft:" >> review-output/claims_drift_report.md
	@echo "\`\`\`" >> review-output/claims_drift_report.md
	@echo "$(DRAFT_FILE)" >> review-output/claims_drift_report.md
	@echo "\`\`\`" >> review-output/claims_drift_report.md
	@echo "" >> review-output/claims_drift_report.md

	@echo "## Agent Instructions" >> review-output/claims_drift_report.md
	@echo "" >> review-output/claims_drift_report.md
	@cat agents/claims-drift-sentinel.md >> review-output/claims_drift_report.md

	@echo "" >> review-output/claims_drift_report.md
	@echo "---" >> review-output/claims_drift_report.md
	@echo "" >> review-output/claims_drift_report.md
	@echo "### Action" >> review-output/claims_drift_report.md
	@echo "Verify claim consistency and report drift below this line." >> review-output/claims_drift_report.md

	@echo ""
	@echo "Prepared:"
	@echo "→ review-output/claims_drift_report.md"



publish-ready:
	@echo ""
	@echo "=========================================="
	@echo " EDITORIAL & DISTRIBUTION READINESS"
	@echo "=========================================="
	@echo ""
	@echo "Preparing editorial readiness artifact"
	@echo ""

	@mkdir -p review-output

	@echo "# Editorial & Distribution Readiness Review" > review-output/editorial_readiness.md
	@echo "" >> review-output/editorial_readiness.md
	@echo "> Editorial Coherence Reviewer v1 active" >> review-output/editorial_readiness.md
	@echo "" >> review-output/editorial_readiness.md

	@echo "## Canonical Draft Context" >> review-output/editorial_readiness.md
	@echo "" >> review-output/editorial_readiness.md
	@echo "Source draft:" >> review-output/editorial_readiness.md
	@echo "\`\`\`" >> review-output/editorial_readiness.md
	@echo "$(DRAFT_FILE)" >> review-output/editorial_readiness.md
	@echo "\`\`\`" >> review-output/editorial_readiness.md
	@echo "" >> review-output/editorial_readiness.md

	@echo "## Editorial Coherence Review" >> review-output/editorial_readiness.md
	@echo "" >> review-output/editorial_readiness.md
	@cat agents/editorial-coherence-reviewer.md >> review-output/editorial_readiness.md

	@echo "" >> review-output/editorial_readiness.md
	@echo "## Distribution Readiness Review" >> review-output/editorial_readiness.md
	@echo "" >> review-output/editorial_readiness.md
	@cat agents/distribution-readiness-agent.md >> review-output/editorial_readiness.md

	@echo "" >> review-output/editorial_readiness.md
	@echo "---" >> review-output/editorial_readiness.md
	@echo "" >> review-output/editorial_readiness.md
	@echo "### Action" >> review-output/editorial_readiness.md
	@echo "Assess readiness and provide final publish/no-publish decision below." >> review-output/editorial_readiness.md

	@echo ""
	@echo "Prepared:"
	@echo "→ review-output/editorial_readiness.md"

	

# ------------------------------------------
# Full Publishing Cycle
# ------------------------------------------

publish-cycle:
	@$(MAKE) publish-plan
	@$(MAKE) publish-generate
	@$(MAKE) publish-verify
	@$(MAKE) publish-ready
