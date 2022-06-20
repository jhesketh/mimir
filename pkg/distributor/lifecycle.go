// SPDX-License-Identifier: AGPL-3.0-only

package distributor

import "github.com/grafana/dskit/ring"

// OnRingInstanceRegister implements the ring.BasicLifecyclerDelegate interface
func (d *Distributor) OnRingInstanceRegister(_ *ring.BasicLifecycler, ringDesc ring.Desc, instanceExists bool, instanceID string, instanceDesc ring.InstanceDesc) (ring.InstanceState, ring.Tokens) {
	var tokens []uint32
	if instanceExists {
		tokens = instanceDesc.GetTokens()
	}

	takenTokens := ringDesc.GetTokens()
	newTokens := ring.GenerateTokens(ringNumTokens-len(tokens), takenTokens)

	// Tokens sorting will be enforced by the parent caller.
	tokens = append(tokens, newTokens...)

	return ring.ACTIVE, tokens
}

// OnRingInstanceTokens implements the ring.BasicLifecyclerDelegate interface
func (d *Distributor) OnRingInstanceTokens(_ *ring.BasicLifecycler, _ ring.Tokens) {}

// OnRingInstanceStopping implements the ring.BasicLifecyclerDelegate interface
func (d *Distributor) OnRingInstanceStopping(_ *ring.BasicLifecycler) {}

// OnRingInstanceHeartbeat implements the ring.BasicLifecyclerDelegate interface
func (d *Distributor) OnRingInstanceHeartbeat(_ *ring.BasicLifecycler, ringDesc *ring.Desc, _ *ring.InstanceDesc) {
	activeMembers := uint32(0)

	for _, instance := range ringDesc.Ingesters {
		if ring.ACTIVE == instance.State {
			activeMembers++
		}
	}

	d.healthyInstancesCount.Store(activeMembers)
}

// HealthyInstancesCount implements the ReadLifecycler interface
func (d *Distributor) HealthyInstancesCount() int {
	return int(d.healthyInstancesCount.Load())
}
