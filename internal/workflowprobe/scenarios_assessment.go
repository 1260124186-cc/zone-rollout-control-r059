package workflowprobe

import (
	"fmt"
	"net/http"
	"os"
	"time"

	"solo-0014-zone-rollout-control/internal/domain"
)

func runConsistencyAssessment() error {
	harness, err := newHarness()
	if err != nil {
		return err
	}
	defer harness.close()

	if _, err := enrollNode(harness, "resolver-assess-01", "west"); err != nil {
		return err
	}
	if _, err := enrollNode(harness, "resolver-assess-02", "west"); err != nil {
		return err
	}
	plan, revision, err := createActivePlan(harness, "assess.example", 3001, 2, 1, 40)
	if err != nil {
		return err
	}
	group, err := allocate(harness, plan.ID, "assess-lease-0001", "west", 2)
	if err != nil {
		return err
	}
	for index, leaseID := range group.LeaseIDs {
		requestKey := fmt.Sprintf("assess-probe-%s", leaseID)
		if _, err := submitProbe(harness, leaseID, requestKey, true, revision.Serial, 10+index); err != nil {
			return err
		}
	}

	var consistent domain.RolloutAssessment
	if err := harness.call(http.MethodPatch, "/v1/plans/"+plan.ID+"/assessments", domain.AssessmentInput{
		RequestKey: "assess-request-0001",
	}, &consistent); err != nil {
		return err
	}
	if consistent.Verdict != domain.VerdictConsistent || consistent.ProbeCount != 2 {
		return ensure(false, "consistent assessment did not include both probes")
	}
	var replay domain.RolloutAssessment
	if err := harness.call(http.MethodPatch, "/v1/plans/"+plan.ID+"/assessments", domain.AssessmentInput{
		RequestKey: "assess-request-0001",
	}, &replay); err != nil {
		return err
	}
	if replay.ID != consistent.ID {
		return ensure(false, "assessment replay returned another result")
	}
	var latest domain.RolloutAssessment
	if err := harness.call(http.MethodGet, "/v1/plans/"+plan.ID+"/assessments/latest", nil, &latest); err != nil {
		return err
	}
	if latest.ID != consistent.ID {
		return ensure(false, "latest assessment does not match")
	}

	reviewPlan, reviewRevision, err := createActivePlan(harness, "review.example", 4001, 1, 1, 20)
	if err != nil {
		return err
	}
	reviewGroup, err := allocate(harness, reviewPlan.ID, "review-lease-0001", "west", 1)
	if err != nil {
		return err
	}
	if _, err := submitProbe(
		harness,
		reviewGroup.LeaseIDs[0],
		"review-probe-0001",
		true,
		reviewRevision.Serial+1,
		30,
	); err != nil {
		return err
	}
	var review domain.RolloutAssessment
	if err := harness.call(http.MethodPatch, "/v1/plans/"+reviewPlan.ID+"/assessments", domain.AssessmentInput{
		RequestKey: "review-request-0001",
	}, &review); err != nil {
		return err
	}
	if review.Verdict != domain.VerdictReviewRequired {
		return ensure(false, "mismatched serial did not require review")
	}
	if err := runRepeatedLeaseAssessment(); err != nil {
		return err
	}
	if err := runOutOfOrderObservationAssessment(); err != nil {
		return err
	}
	return runRestartDeterministicAssessment()
}

// runRepeatedLeaseAssessment verifies that one resolver node leasing and
// reporting twice still counts as a single participating node. The plan
// expects two successful nodes, so the second lease must not inflate coverage
// into a false pass.
func runRepeatedLeaseAssessment() error {
	harness, err := newHarness()
	if err != nil {
		return err
	}
	defer harness.close()

	if _, err := enrollNode(harness, "resolver-repeat-01", "west"); err != nil {
		return err
	}
	plan, revision, err := createActivePlan(harness, "repeat.example", 5001, 2, 1, 40)
	if err != nil {
		return err
	}

	// The plan expects two nodes but only one resolver is enrolled. After its
	// first report the node goes back to ready, so it can claim a second lease
	// and report again through a different lease.
	firstGroup, err := allocate(harness, plan.ID, "repeat-lease-0001", "west", 1)
	if err != nil {
		return err
	}
	if _, err := submitProbe(harness, firstGroup.LeaseIDs[0], "repeat-probe-0001", true, revision.Serial, 10); err != nil {
		return err
	}

	harness.clock.Advance(time.Minute)
	secondGroup, err := allocate(harness, plan.ID, "repeat-lease-0002", "west", 1)
	if err != nil {
		return err
	}
	if len(secondGroup.LeaseIDs) != 1 {
		return ensure(false, "second allocation did not re-lease the same node")
	}
	if secondGroup.LeaseIDs[0] == firstGroup.LeaseIDs[0] {
		return ensure(false, "the repeated report reused the same lease")
	}
	if _, err := submitProbe(harness, secondGroup.LeaseIDs[0], "repeat-probe-0002", true, revision.Serial, 12); err != nil {
		return err
	}

	var assessment domain.RolloutAssessment
	if err := harness.call(http.MethodPatch, "/v1/plans/"+plan.ID+"/assessments", domain.AssessmentInput{
		RequestKey: "repeat-request-0001",
	}, &assessment); err != nil {
		return err
	}
	if assessment.Verdict != domain.VerdictReviewRequired {
		return ensure(false, "repeated reports from one node inflated node coverage")
	}
	if !containsReason(assessment.Reasons, "node_not_observed") {
		return ensure(false, "missing expected node did not surface node_not_observed")
	}
	if assessment.ProbeCount != 2 || assessment.SuccessCount != 2 {
		return ensure(false, "repeated reports were not both counted in probe totals")
	}
	return nil
}

// runOutOfOrderObservationAssessment verifies that a late-arriving report with
// an earlier observed_at is not swallowed: its failure status and latency must
// feed the success ratio and maximum latency even though the report order is
// out of sequence.
func runOutOfOrderObservationAssessment() error {
	harness, err := newHarness()
	if err != nil {
		return err
	}
	defer harness.close()

	if _, err := enrollNode(harness, "resolver-order-01", "east"); err != nil {
		return err
	}
	plan, revision, err := createActivePlan(harness, "order.example", 6001, 1, 1, 40)
	if err != nil {
		return err
	}

	firstGroup, err := allocate(harness, plan.ID, "order-lease-0001", "east", 1)
	if err != nil {
		return err
	}
	base := harness.clock.Now()
	// First report: successful, with a later observation time and low latency.
	if _, err := submitProbeAt(harness, firstGroup.LeaseIDs[0], "order-probe-0001", true, revision.Serial, 10, base.Add(2*time.Minute)); err != nil {
		return err
	}

	// Same node claims another lease and supplies a late补报 whose observed_at
	// is earlier, whose probe failed, and whose latency exceeds the limit.
	harness.clock.Advance(3 * time.Minute)
	secondGroup, err := allocate(harness, plan.ID, "order-lease-0002", "east", 1)
	if err != nil {
		return err
	}
	if _, err := submitProbeAt(harness, secondGroup.LeaseIDs[0], "order-probe-0002", false, revision.Serial, 55, base.Add(time.Minute)); err != nil {
		return err
	}

	var assessment domain.RolloutAssessment
	if err := harness.call(http.MethodPatch, "/v1/plans/"+plan.ID+"/assessments", domain.AssessmentInput{
		RequestKey: "order-request-0001",
	}, &assessment); err != nil {
		return err
	}
	if assessment.ProbeCount != 2 || assessment.SuccessCount != 1 {
		return ensure(false, "the earlier-observed late report vanished from the probe totals")
	}
	if assessment.SuccessRatio != 0.5 {
		return ensure(false, "the failed late report did not lower the success ratio")
	}
	if assessment.MaximumLatencyMS != 55 {
		return ensure(false, "the late report latency was not reflected in the maximum")
	}
	if assessment.Verdict != domain.VerdictReviewRequired ||
		!containsReason(assessment.Reasons, "success_ratio_below_minimum") ||
		!containsReason(assessment.Reasons, "latency_limit_exceeded") {
		return ensure(false, "out-of-order late report did not drive the expected failure reasons")
	}
	return nil
}

// runRestartDeterministicAssessment verifies that restarting the service over
// the same snapshot reproduces the exact same coverage and verdict, while an
// assessment request replay keeps returning the first recorded result.
func runRestartDeterministicAssessment() error {
	tempDir, err := os.MkdirTemp("", "zrc-restart-*")
	if err != nil {
		return err
	}
	defer os.RemoveAll(tempDir)
	harness, err := newHarnessAt(tempDir)
	if err != nil {
		return err
	}
	defer harness.close()

	if _, err := enrollNode(harness, "resolver-restart-01", "west"); err != nil {
		return err
	}
	plan, revision, err := createActivePlan(harness, "restart.example", 7001, 1, 1, 40)
	if err != nil {
		return err
	}
	group, err := allocate(harness, plan.ID, "restart-lease-0001", "west", 1)
	if err != nil {
		return err
	}
	base := harness.clock.Now()
	if _, err := submitProbeAt(harness, group.LeaseIDs[0], "restart-probe-0001", true, revision.Serial, 15, base); err != nil {
		return err
	}

	var before domain.RolloutAssessment
	if err := harness.call(http.MethodPatch, "/v1/plans/"+plan.ID+"/assessments", domain.AssessmentInput{
		RequestKey: "restart-request-0001",
	}, &before); err != nil {
		return err
	}
	if before.Verdict != domain.VerdictConsistent {
		return ensure(false, "single successful node should pass before restart")
	}

	if err := harness.restart(); err != nil {
		return err
	}

	// Replaying the same assessment request must return the first result.
	var replay domain.RolloutAssessment
	if err := harness.call(http.MethodPatch, "/v1/plans/"+plan.ID+"/assessments", domain.AssessmentInput{
		RequestKey: "restart-request-0001",
	}, &replay); err != nil {
		return err
	}
	if replay.ID != before.ID || replay.Verdict != before.Verdict {
		return ensure(false, "assessment replay after restart did not return the first result")
	}

	// A fresh request over the same historical observations must reproduce the
	// identical coverage and conclusion even though a new assessment is made.
	var after domain.RolloutAssessment
	if err := harness.call(http.MethodPatch, "/v1/plans/"+plan.ID+"/assessments", domain.AssessmentInput{
		RequestKey: "restart-request-0002",
	}, &after); err != nil {
		return err
	}
	if after.Verdict != before.Verdict ||
		after.ProbeCount != before.ProbeCount ||
		after.SuccessCount != before.SuccessCount ||
		after.SuccessRatio != before.SuccessRatio ||
		after.MaximumLatencyMS != before.MaximumLatencyMS {
		return ensure(false, "restart produced a different verdict for the same history")
	}
	return nil
}

func containsReason(reasons []string, want string) bool {
	for _, reason := range reasons {
		if reason == want {
			return true
		}
	}
	return false
}
