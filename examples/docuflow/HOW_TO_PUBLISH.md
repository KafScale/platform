# How to Publish: Multi-Channel Content Distribution

This guide covers the complete workflow for transforming tutorial content into publication-ready drafts across multiple channels.

---

## Overview

The publishing workflow consists of three phases:

1. **Content Production** - Create the tutorial content
2. **Content Amplification** - Generate canonical publication draft
3. **Multi-Channel Drafting** - Transform canonical draft into channel-specific content

This document focuses on **Phase 3: Multi-Channel Drafting**.

---

## Prerequisites

Before starting the multi-channel drafting process, you must have:

1. ✅ **Tutorial content completed** - All markdown files in `examples/[tutorial-name]/`
2. ✅ **Tutorial reviewed** - EAAT, Technical Accuracy, and Didactical reviews passed
3. ✅ **Claims registry updated** - All technical claims have verified registry entries
4. ✅ **Canonical draft created** - File exists in `examples/publication-drafts/[date]__[tutorial-name]__draft.md`

---

## Phase 3: Multi-Channel Drafting

### Step-by-Step Procedure

#### Step 1: Prepare Your Environment

**Action**: Open the canonical draft in your IDE

```bash
# Navigate to publication drafts directory
cd examples/publication-drafts/

# List available canonical drafts
ls -l *__draft.md

# Open the most recent canonical draft in your IDE
# Example: 2026-01-04__101_kafscale-dev-guide__draft.md
```

**Verification**:
- [ ] Canonical draft file is open in IDE
- [ ] Draft contains Core Story Summary, LinkedIn outline, Blog outline, etc.
- [ ] Claims registry references are present (KS-COMP-001, KS-ARCH-001, etc.)

---

#### Step 2: Execute the Publishing Cycle

**Action**: Use the DRAFT-GENERATOR-PROMPT.md to invoke the multi-channel writer

**Option A: Copy-Paste Prompt** (Manual)
1. Open `examples/docuflow/DRAFT-GENERATOR-PROMPT.md`
2. Copy the entire prompt text
3. Paste into your AI assistant with the canonical draft open
4. Wait for Publishing Cycle v2 to complete

**Option B: Direct Reference** (Preferred)
1. Send this message to your AI assistant:

```
You are my multi channel writer, executing a controlled, multi-phase content generation process.

Source of truth:
- The currently open Markdown file is the canonical amplification draft.

Execute the instructions defined in:
examples/docuflow/DRAFT-GENERATOR-PROMPT.md

Begin now.
```

**Expected Output**: The system will execute 8 phases:
- Phase 0: Input Confirmation
- Phase 1: LinkedIn Draft
- Phase 2: Blog/Medium Draft
- Phase 3: Newsletter Draft
- Phase 4: Talk Abstract
- Phase 5: Claims Drift Sentinel
- Phase 6: Editorial Readiness Review
- Phase 7: Distribution Readiness
- Phase 8: File Generation

**Duration**: 3-5 minutes (depending on content length)

---

#### Step 3: Verify Generated Files

**Action**: Check that all expected files were created

```bash
# List all generated files in publication-drafts/
ls -lh examples/publication-drafts/

# You should see:
# - linkedin-draft__[tutorial-name].md
# - newsletter-draft__[tutorial-name].md
# - talk-abstract__[tutorial-name].md
# - distribution-checklist__[YYYY-MM-DD].md
# - blog-draft__[tutorial-name].md (should already exist)
```

**Verification Checklist**:
- [ ] LinkedIn draft exists (~2-3 KB)
- [ ] Newsletter draft exists (~3-4 KB)
- [ ] Talk abstract exists (~5-6 KB)
- [ ] Distribution checklist exists (~10-12 KB)
- [ ] Blog draft exists (~12-15 KB, may be pre-existing)

---

#### Step 4: Review Claims Drift Report

**Action**: Check the Claims Drift Sentinel output (embedded in Publishing Cycle output)

**What to Look For**:
- ✅ **Claims Accuracy**: PASS (no new claims introduced)
- ✅ **Registry Compliance**: PASS (all KS-* references verified)
- ✅ **Overpromising Check**: PASS (no overpromising language)

**If FAIL Status**:
1. Review the flagged claims in the report
2. Either:
   - Add missing claims to `examples/claims/` registry
   - Remove or qualify the claim in channel drafts
3. Re-run the publishing cycle

---

#### Step 5: Review Editorial Coherence Report

**Action**: Check the Editorial Coherence Reviewer output

**What to Look For**:
- ✅ **Framing Alignment**: Consistent core message across channels
- ✅ **CTA Consistency**: All CTAs point to tutorial
- ✅ **Tradeoff Transparency**: S3 latency and transaction limitations stated in all channels
- ✅ **Tone Appropriateness**: Each channel matches its audience expectations

**If NOT READY Status**:
1. Review the flagged issues
2. Manually edit the affected channel draft files
3. Re-run editorial review only (or full cycle if significant changes)

---

#### Step 6: Review Distribution Plan

**Action**: Open `distribution-checklist__[date].md` and review the suggested schedule

**Key Sections**:
1. **Pre-Publication Verification** - Technical, legal, accessibility checks
2. **Channel Checklists** - Blog, LinkedIn, Newsletter, Talk specific steps
3. **Suggested Publishing Schedule** - Recommended order: Blog → LinkedIn → Newsletter → Talk
4. **Success Metrics** - Analytics setup and tracking

**Decision Point**: Choose publishing strategy:
- **Option A (Recommended)**: Staggered release (Blog Monday → LinkedIn Wednesday → Newsletter Friday → Talk Week 2+)
- **Option B (Alternative)**: Simultaneous launch (Blog + LinkedIn same day → Newsletter 2-3 days later)

---

#### Step 7: Complete Pre-Publication Tasks

**Action**: Work through the distribution checklist

**For Blog**:
- [ ] Choose hosting platform (Medium, Dev.to, company blog)
- [ ] Add cover image if required
- [ ] Format code blocks with syntax highlighting
- [ ] Configure tags/categories
- [ ] Set up analytics tracking

**For LinkedIn**:
- [ ] Replace `[link to tutorial]` placeholder with actual GitHub URL
- [ ] Add UTM parameters for tracking
- [ ] Verify hashtags are appropriate
- [ ] Preview post formatting

**For Newsletter**:
- [ ] Choose newsletter platform (Substack, ConvertKit, Buttondown)
- [ ] Add subject line
- [ ] Send test email to yourself
- [ ] Verify links render in email clients

**For Talk**:
- [ ] Identify target conferences/meetups
- [ ] Prepare speaker bio (150 words)
- [ ] Specify demo format (live vs recorded)

---

#### Step 8: Execute Publishing Schedule

**Action**: Publish content according to chosen schedule

**Week 1, Day 1 (Monday)**:
1. Publish blog (morning, US Eastern time)
2. Note final blog URL
3. Set up analytics tracking
4. Share internally with team

**Week 1, Day 3 (Wednesday)**:
1. Update LinkedIn draft with actual blog URL
2. Add UTM tracking parameters
3. Post on LinkedIn (afternoon)
4. Engage with comments in first 2 hours

**Week 1, Day 5 (Friday)**:
1. Update newsletter with blog reference
2. Send newsletter (afternoon)
3. Monitor open rate and click rate

**Week 2+ (Ongoing)**:
1. Submit talk to 3-5 conferences/meetups
2. Link to published blog as supporting material
3. Track acceptance rates

---

#### Step 9: Monitor and Track

**Action**: Set up tracking for each channel

**Blog Analytics**:
```bash
# Example UTM parameters for tracking referrals
?utm_source=linkedin&utm_medium=social&utm_campaign=kafscale-tutorial-2026-01
?utm_source=newsletter&utm_medium=email&utm_campaign=kafscale-tutorial-2026-01
```

**Metrics to Track**:
- Blog: Page views, time on page, scroll depth, GitHub clicks
- LinkedIn: Impressions, engagement rate, click-through rate
- Newsletter: Open rate, click rate, replies
- Talk: CFP acceptance rate, attendance, GitHub stars spike

**Review Cadence**:
- Daily (first 3 days): Monitor engagement, respond to comments
- Weekly (first month): Review analytics, note feedback themes
- Monthly: Update tutorial based on community feedback

---

#### Step 10: Archive and Document

**Action**: Create a publishing retrospective

**Create File**: `examples/publication-drafts/retrospective__[date]__[tutorial-name].md`

**Template**:
```markdown
# Publishing Retrospective — [Tutorial Name]

**Publication Date**: [YYYY-MM-DD]
**Tutorial**: examples/[tutorial-name]/

## Metrics Summary

### Blog
- Platform: [Medium/Dev.to/etc.]
- URL: [final URL]
- Week 1 views: [number]
- Week 1 time on page: [average minutes]
- GitHub clicks: [number]

### LinkedIn
- Published: [date/time]
- Impressions: [number]
- Engagement rate: [percentage]
- Comments: [number]

### Newsletter
- Platform: [platform name]
- Sent: [date/time]
- Open rate: [percentage]
- Click rate: [percentage]
- Replies: [number]

### Talk
- Submitted to: [conference names]
- Accepted: [which ones]
- Presented: [date, if applicable]

## Lessons Learned

### What Worked Well
- [List successes]

### What Could Be Improved
- [List areas for improvement]

### Process Changes for Next Time
- [Document updates to workflow]

## Community Feedback

### Common Questions
- [Theme 1]: [summary]
- [Theme 2]: [summary]

### Requested Features/Topics
- [List for future tutorial planning]

## Tutorial Updates

### Changes Made Post-Publication
- [Date]: [Description of fix/update]
- [Date]: [Description of fix/update]
```

---

## Troubleshooting

### Issue: Publishing Cycle Fails at Phase 5 (Claims Drift)

**Symptom**: Claims Drift Sentinel reports FAIL status

**Solution**:
1. Review flagged claims in the report
2. Check if claim exists in `examples/claims/`
3. If missing, create claim entry with proper evidence
4. If claim is overstated, qualify it in channel drafts
5. Re-run publishing cycle

---

### Issue: Generated Files Missing or Incomplete

**Symptom**: Not all 4 channel files created

**Solution**:
1. Check AI assistant output for errors
2. Verify canonical draft is properly formatted
3. Manually trigger Phase 8 (File Generation) again
4. If persistent, create files manually using templates from previous runs

---

### Issue: Editorial Coherence Reports Inconsistencies

**Symptom**: Different technical claims across channels

**Solution**:
1. Review Editorial Coherence Report findings
2. Standardize language across channel drafts
3. Ensure all channels reference same claims registry IDs
4. Re-run Publishing Cycle if changes are significant

---

### Issue: Low Engagement After Publishing

**Symptom**: Blog views, LinkedIn impressions below expectations

**Solution**:
1. **Timing**: Re-share content at different times/days
2. **Distribution**: Share in relevant Slack/Discord communities
3. **Paid Promotion**: Consider minimal paid boost for blog
4. **Internal Amplification**: Leverage company/team channels
5. **Derivative Content**: Create Twitter thread or infographic
6. **Community Engagement**: Proactively share in r/apachekafka, Hacker News

---

## Quick Reference Commands

```bash
# List all canonical drafts
ls -l examples/publication-drafts/*__draft.md

# List all generated channel drafts for a tutorial
ls -l examples/publication-drafts/*__101_kafscale-dev-guide.md

# View distribution checklist
cat examples/publication-drafts/distribution-checklist__2026-01-04.md

# Check file sizes (verify completeness)
ls -lh examples/publication-drafts/

# Search for placeholder URLs (should be replaced before publishing)
grep -r "\[link to tutorial\]" examples/publication-drafts/
```

---

## Summary Workflow Diagram

```
┌─────────────────────────────────────────────────────────────┐
│ PHASE 3: MULTI-CHANNEL DRAFTING                            │
└─────────────────────────────────────────────────────────────┘

Step 1: Prepare Environment
  └─> Open canonical draft in IDE

Step 2: Execute Publishing Cycle
  └─> Run DRAFT-GENERATOR-PROMPT.md
      ├─> Phase 0: Input Confirmation
      ├─> Phase 1: LinkedIn Draft
      ├─> Phase 2: Blog Draft
      ├─> Phase 3: Newsletter Draft
      ├─> Phase 4: Talk Abstract
      ├─> Phase 5: Claims Drift Sentinel ✓
      ├─> Phase 6: Editorial Coherence ✓
      ├─> Phase 7: Distribution Readiness
      └─> Phase 8: File Generation
          ├─> linkedin-draft__[name].md
          ├─> newsletter-draft__[name].md
          ├─> talk-abstract__[name].md
          └─> distribution-checklist__[date].md

Step 3: Verify Generated Files
  └─> Check all 4 files exist + blog draft

Step 4: Review Claims Drift Report
  └─> Verify PASS status, fix if FAIL

Step 5: Review Editorial Coherence
  └─> Verify READY status, fix if NOT READY

Step 6: Review Distribution Plan
  └─> Choose publishing strategy (staggered/simultaneous)

Step 7: Complete Pre-Publication Tasks
  ├─> Blog: Platform, formatting, analytics
  ├─> LinkedIn: URL replacement, UTM tracking
  ├─> Newsletter: Platform, subject line, test send
  └─> Talk: CFP targets, speaker bio

Step 8: Execute Publishing Schedule
  ├─> Monday: Blog
  ├─> Wednesday: LinkedIn
  ├─> Friday: Newsletter
  └─> Week 2+: Talk submissions

Step 9: Monitor and Track
  └─> Daily → Weekly → Monthly review cadence

Step 10: Archive and Document
  └─> Create retrospective, note lessons learned
```

---

## Files and Agents Reference

### Agent Definitions (in `examples/agents/`)
- `linkedin-draft-agent.md` - LinkedIn post generation rules
- `newsletter-draft-agent.md` - Newsletter content rules
- `blog-draft-agent.md` - Blog article generation rules
- `talk-abstract-agent.md` - Conference CFP abstract rules
- `claims-drift-sentinel.md` - Claims verification rules
- `editorial-coherence-reviewer.md` - Cross-channel consistency rules
- `distribution-readiness-agent.md` - Pre-publication checklist rules

### Master Workflow
- `examples/docuflow/DRAFT-GENERATOR-PROMPT.md` - Main publishing cycle orchestrator

### Output Directory
- `examples/publication-drafts/` - All generated draft files

---

## Next Steps

After completing this multi-channel drafting process, you should have:

✅ 4-5 publication-ready draft files
✅ Claims accuracy verified
✅ Editorial coherence confirmed
✅ Distribution plan with checklists
✅ Analytics tracking configured

**You are now ready to publish!**

Proceed with executing the publishing schedule from the distribution checklist.

---

**Last Updated**: 2026-01-04
**Version**: 2.0
**Maintained By**: KafScale Documentation Team
