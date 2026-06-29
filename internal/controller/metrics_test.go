package controller

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	agentforgev1alpha1 "github.com/containeroo/agent-forge-operator/api/v1alpha1"
)

func TestRecordPoolCapacityMetricsExportsVMShape(t *testing.T) {
	pool := &agentforgev1alpha1.VsphereAgentPool{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "billing",
			Name:      "large-workers",
		},
		Spec: agentforgev1alpha1.VsphereAgentPoolSpec{
			Template: agentforgev1alpha1.VMTemplateSpec{
				NumCPUs:   16,
				MemoryMiB: 65536,
				DiskGiB:   512,
			},
		},
	}
	deletePoolCapacityMetrics(pool)
	defer deletePoolCapacityMetrics(pool)

	recordPoolCapacityMetrics(pool, PoolPlan{DesiredReplicas: 3})

	if got, want := testutil.ToFloat64(poolVMCPUCoresGauge.WithLabelValues(pool.Namespace, pool.Name)), float64(16); got != want {
		t.Fatalf("CPU cores metric = %f, want %f", got, want)
	}
	if got, want := testutil.ToFloat64(poolVMMemoryBytesGauge.WithLabelValues(pool.Namespace, pool.Name)), float64(65536*1024*1024); got != want {
		t.Fatalf("memory bytes metric = %f, want %f", got, want)
	}
	if got, want := testutil.ToFloat64(poolVMDiskBytesGauge.WithLabelValues(pool.Namespace, pool.Name)), float64(512*1024*1024*1024); got != want {
		t.Fatalf("disk bytes metric = %f, want %f", got, want)
	}
}

func TestRecordPoolCapacityMetricsExportsDefaultVMShape(t *testing.T) {
	pool := &agentforgev1alpha1.VsphereAgentPool{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "billing",
			Name:      "default-workers",
		},
	}
	applySpecDefaults(pool)
	deletePoolCapacityMetrics(pool)
	defer deletePoolCapacityMetrics(pool)

	recordPoolCapacityMetrics(pool, PoolPlan{})

	if got, want := testutil.ToFloat64(poolVMCPUCoresGauge.WithLabelValues(pool.Namespace, pool.Name)), float64(4); got != want {
		t.Fatalf("default CPU cores metric = %f, want %f", got, want)
	}
	if got, want := testutil.ToFloat64(poolVMMemoryBytesGauge.WithLabelValues(pool.Namespace, pool.Name)), float64(16384*1024*1024); got != want {
		t.Fatalf("default memory bytes metric = %f, want %f", got, want)
	}
	if got, want := testutil.ToFloat64(poolVMDiskBytesGauge.WithLabelValues(pool.Namespace, pool.Name)), float64(100*1024*1024*1024); got != want {
		t.Fatalf("default disk bytes metric = %f, want %f", got, want)
	}
}
