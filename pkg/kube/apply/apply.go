package apply

import (
	"context"

	sdkerrors "github.com/bubustack/bubu-sdk-go/pkg/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// FieldManager is the stable SSA field manager for this SDK.
const FieldManager = "bubu-sdk-go"

// Apply performs Server-Side Apply with a stable field manager.
func Apply(ctx context.Context, c client.Client, obj client.Object, force bool) error {
	if ctx == nil {
		return sdkerrors.ErrNilContext
	}
	obj.SetManagedFields(nil)
	return c.Patch(ctx, obj, client.Apply, &client.PatchOptions{
		FieldManager: FieldManager,
		Force:        &force,
	})
}
