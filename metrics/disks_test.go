package metrics

import "testing"

func TestCapacityMountExcludesReadOnlyImages(t *testing.T) {
	for _, tt := range []struct {
		device, filesystem, options string
		want                        bool
	}{
		{"/dev/vda1", "ext4", "rw,relatime", true},
		{"/dev/loop1", "ext4", "rw", true},
		{"/dev/vdb1", "xfs", "rw", true},
		{"/dev/loop0", "squashfs", "ro,nodev", false},
		{"/dev/loop0", "squashfs", "rw", false},
		{"/dev/sr0", "iso9660", "ro", false},
		{"/dev/vdc1", "ext4", "ro,relatime", false},
		{"tmpfs", "tmpfs", "rw", false},
	} {
		if got := capacityMount(tt.device, tt.filesystem, tt.options); got != tt.want {
			t.Errorf("capacityMount(%q, %q, %q) = %v", tt.device, tt.filesystem, tt.options, got)
		}
	}
}
