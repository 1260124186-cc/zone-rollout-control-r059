package domain

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"math"
	"sort"
	"time"
)

func ActivatePlan(plan *RolloutPlan, now time.Time) error {
	if plan.State != PlanDraft {
		return InvalidState("plan can be activated only from draft", map[string]any{
			"plan_id": plan.ID,
			"state":   plan.State,
		})
	}
	if plan.LatestRevisionID == "" {
		return InvalidState("plan needs a zone revision before activation", map[string]any{
			"plan_id": plan.ID,
		})
	}
	plan.State = PlanActive
	plan.ActivatedAt = timePointer(now.UTC())
	plan.Version++
	return nil
}

func CompletePlan(plan *RolloutPlan, now time.Time) error {
	if plan.State != PlanActive {
		return InvalidState("plan can be completed only from active", map[string]any{
			"plan_id": plan.ID,
			"state":   plan.State,
		})
	}
	plan.State = PlanCompleted
	plan.CompletedAt = timePointer(now.UTC())
	plan.Version++
	return nil
}

func LeaseFingerprint(planID string, input LeaseInput) string {
	payload := fmt.Sprintf(
		"%s|%s|%s|%d|%d",
		planID,
		input.RequestKey,
		input.Region,
		input.LeaseSeconds,
		input.MaxNodes,
	)
	sum := sha256.Sum256([]byte(payload))
	return hex.EncodeToString(sum[:])
}

func ProbeFingerprint(leaseID string, input ProbeInput) string {
	payload := fmt.Sprintf(
		"%s|%s|%t|%d|%d|%s",
		leaseID,
		input.RequestKey,
		input.Success,
		input.ObservedSerial,
		input.LatencyMS,
		input.ObservedAt.UTC().Format(time.RFC3339Nano),
	)
	sum := sha256.Sum256([]byte(payload))
	return hex.EncodeToString(sum[:])
}

func BuildAssessment(
	id string,
	plan RolloutPlan,
	revision ZoneRevision,
	rollup ObservationRollup,
	now time.Time,
) RolloutAssessment {
	reasons := make(map[string]struct{})
	if rollup.ProbeCount == 0 {
		reasons["no_probes"] = struct{}{}
	}
	if rollup.SerialMismatches > 0 {
		reasons["serial_mismatch"] = struct{}{}
	}
	if len(rollup.ObservedNodes) < plan.ExpectedNodes {
		reasons["node_not_observed"] = struct{}{}
	}
	successRatio := 0.0
	if rollup.ProbeCount > 0 {
		successRatio = round(float64(rollup.SuccessCount)/float64(rollup.ProbeCount), 6)
	}
	if successRatio < plan.MinimumSuccessRatio {
		reasons["success_ratio_below_minimum"] = struct{}{}
	}
	if rollup.MaximumLatencyMS > plan.MaximumLatencyMS {
		reasons["latency_limit_exceeded"] = struct{}{}
	}
	reasonList := make([]string, 0, len(reasons))
	for reason := range reasons {
		reasonList = append(reasonList, reason)
	}
	sort.Strings(reasonList)
	verdict := VerdictConsistent
	if len(reasonList) > 0 {
		verdict = VerdictReviewRequired
	}
	return RolloutAssessment{
		ID:               id,
		PlanID:           plan.ID,
		RevisionID:       revision.ID,
		Verdict:          verdict,
		Reasons:          reasonList,
		ProbeCount:       rollup.ProbeCount,
		SuccessCount:     rollup.SuccessCount,
		SuccessRatio:     successRatio,
		MaximumLatencyMS: rollup.MaximumLatencyMS,
		CreatedAt:        now.UTC(),
	}
}

func ObservationRollupFor(state *State, planID string) ObservationRollup {
	if state == nil || state.ObservationRollups == nil {
		return ObservationRollup{PlanID: planID}
	}
	rollup, ok := state.ObservationRollups[planID]
	if !ok {
		return ObservationRollup{PlanID: planID}
	}
	return rollup
}

func RecordObservation(
	state *State,
	plan RolloutPlan,
	revision ZoneRevision,
	probe Probe,
) ObservationRollup {
	if state.ObservationRollups == nil {
		state.ObservationRollups = make(map[string]ObservationRollup)
	}
	rollup, ok := state.ObservationRollups[probe.PlanID]
	if !ok {
		rollup = ObservationRollup{
			PlanID:        probe.PlanID,
			ObservedNodes: make(map[string]struct{}),
		}
	}
	if rollup.ObservedNodes == nil {
		rollup.ObservedNodes = make(map[string]struct{})
	}
	if probe.ObservedAt.Before(rollup.Watermark) {
		state.ObservationRollups[probe.PlanID] = rollup
		return rollup
	}
	rollup.RevisionID = revision.ID
	rollup.ProbeCount++
	if probe.Success {
		rollup.SuccessCount++
		if probe.ObservedSerial != revision.Serial {
			rollup.SerialMismatches++
		}
	}
	if probe.LatencyMS > rollup.MaximumLatencyMS {
		rollup.MaximumLatencyMS = probe.LatencyMS
	}
	rollup.ObservedNodes[probe.LeaseID] = struct{}{}
	rollup.Watermark = probe.ObservedAt
	rollup.UpdatedAt = probe.StoredAt
	state.ObservationRollups[probe.PlanID] = rollup
	return rollup
}

func round(value float64, places int) float64 {
	scale := math.Pow10(places)
	return math.Round(value*scale) / scale
}

func timePointer(value time.Time) *time.Time {
	return &value
}
