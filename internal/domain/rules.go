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
	if len(rollup.SuccessfulNodes) < plan.ExpectedNodes {
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

// DeriveObservationRollup recomputes the plan rollup purely from the probes
// that were actually committed during the rollout. Node identity, not lease
// identity, defines participation: a node reporting through several leases is
// counted as one participant. Probes are never discarded based on their
// observed time, so a late report with an earlier observed_at still feeds the
// success ratio, maximum latency and coverage. Iterating the probes in a
// stable order keeps the result identical across processes and restarts.
func DeriveObservationRollup(state *State, plan RolloutPlan, revision ZoneRevision) ObservationRollup {
	rollup := ObservationRollup{
		PlanID:          plan.ID,
		ObservedNodes:   make(map[string]struct{}),
		SuccessfulNodes: make(map[string]struct{}),
	}
	if state == nil {
		return rollup
	}
	probeIDs := make([]string, 0)
	for probeID, probe := range state.Probes {
		if probe.PlanID == plan.ID {
			probeIDs = append(probeIDs, probeID)
		}
	}
	sort.Strings(probeIDs)
	for _, probeID := range probeIDs {
		probe := state.Probes[probeID]
		rollup.ProbeCount++
		rollup.ObservedNodes[probe.NodeID] = struct{}{}
		if probe.Success {
			rollup.SuccessCount++
			rollup.SuccessfulNodes[probe.NodeID] = struct{}{}
			if probe.ObservedSerial != revision.Serial {
				rollup.SerialMismatches++
			}
		}
		if probe.LatencyMS > rollup.MaximumLatencyMS {
			rollup.MaximumLatencyMS = probe.LatencyMS
		}
	}
	return rollup
}

func round(value float64, places int) float64 {
	scale := math.Pow10(places)
	return math.Round(value*scale) / scale
}

func timePointer(value time.Time) *time.Time {
	return &value
}
