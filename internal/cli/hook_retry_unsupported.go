//go:build (!darwin && !linux) || (!amd64 && !arm64)

package cli

func hookNativeRetryNode(error) (known, safe, admissionCancellation bool) {
	return false, false, false
}
