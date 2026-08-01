package stats

// GPUProviderInterface enumerates GPUs and reports live VRAM/utilization.
// Implementations live in the runtime that owns the device (e.g. NVML in
// mass-worker-llama-cpp). Devices come back as Type=DeviceTypeGPU with IDs
// from [GPUDeviceID]. Composes with [Hardware] to form a full
// [HardwareInterface] (host CPU + every detected GPU).
type GPUProviderInterface interface {
	GPUs() []Device
	GPUStats() []DeviceStats
}

// NoGPU is a GPUProviderInterface implementation reporting no GPUs.
type NoGPU struct{}

func (NoGPU) GPUs() []Device          { return nil }
func (NoGPU) GPUStats() []DeviceStats { return nil }

// Hardware implements [HardwareInterface] by combining the host CPU with the
// GPUs reported by a [GPUProviderInterface]. Pass [NoGPU]{} for CPU-only builds.
type Hardware struct {
	GPU GPUProviderInterface
}

// NewHardware returns a [HardwareInterface] for the host (CPU + the GPU
// provider's devices).
func NewHardware(gpu GPUProviderInterface) Hardware {
	if gpu == nil {
		gpu = NoGPU{}
	}
	return Hardware{GPU: gpu}
}

func (h Hardware) Devices() []Device {
	return append([]Device{CPUInfo()}, h.GPU.GPUs()...)
}

func (h Hardware) Stats() []DeviceStats {
	return append([]DeviceStats{CPUStats()}, h.GPU.GPUStats()...)
}
