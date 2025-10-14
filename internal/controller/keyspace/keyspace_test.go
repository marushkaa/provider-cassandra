package keyspace

import (
	"context"
	"testing"

	"github.com/gocql/gocql"
	"github.com/pkg/errors"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/crossplane/crossplane-runtime/pkg/reconciler/managed"
	"github.com/crossplane/crossplane-runtime/pkg/resource"
	"github.com/crossplane/crossplane-runtime/pkg/test"
	"github.com/google/go-cmp/cmp"

	"github.com/crossplane/provider-cassandra/apis/cql/v1alpha1"
	"github.com/crossplane/provider-cassandra/internal/clients/cassandra"
)

func pointerToString(s string) *string {
	return &s
}

func pointerToInt(i int) *int {
	return &i
}

func pointerToBool(b bool) *bool {
	return &b
}

func TestConnect(t *testing.T) {
	errBoom := errors.New("boom")

	type fields struct {
		kube      resource.ClientApplicator
		usage     resource.Tracker
		newClient func(creds map[string][]byte, keyspace string, consistencyLevel string) cassandra.DB
	}

	type args struct {
		ctx context.Context
		mg  resource.Managed
	}

	type want struct {
		err error
	}

	cases := map[string]struct {
		reason string
		fields fields
		args   args
		want   want
	}{
		"ErrNotKeyspace": {
			reason: "Should return an error when the managed resource is not a *Keyspace",
			args: args{
				mg: nil,
			},
			want: want{
				err: errors.New(errNotKeyspace),
			},
		},
		"ErrTrackPCUsage": {
			reason: "Should return an error when tracking provider config usage fails",
			fields: fields{
				usage: resource.TrackerFn(func(ctx context.Context, mg resource.Managed) error { return errBoom }),
			},
			args: args{
				mg: &v1alpha1.Keyspace{},
			},
			want: want{
				err: errors.Wrap(errBoom, errTrackPCUsage),
			},
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			c := &connector{
				kube:      tc.fields.kube,
				usage:     tc.fields.usage,
				newClient: tc.fields.newClient,
			}
			_, err := c.Connect(tc.args.ctx, tc.args.mg)
			if diff := cmp.Diff(tc.want.err, err, test.EquateErrors()); diff != "" {
				t.Errorf("\n%s\nConnect(...): -want error, +got error:\n%s\n", tc.reason, diff)
			}
		})
	}
}

func TestObserve(t *testing.T) {

	type fields struct {
		db cassandra.DB
	}

	type args struct {
		ctx context.Context
		mg  resource.Managed
	}

	type want struct {
		o   managed.ExternalObservation
		err error
	}

	cases := map[string]struct {
		reason string
		fields fields
		args   args
		want   want
	}{
		"ErrNotKeyspace": {
			reason: "Should return an error if the managed resource is not a *Keyspace",
			args: args{
				mg: nil,
			},
			want: want{
				err: errors.New(errNotKeyspace),
			},
		},
		"KeyspaceNotFound": {
			reason: "Should return ResourceExists: false when the keyspace does not exist",
			fields: fields{
				db: &cassandra.MockDB{
					QueryFunc: func(ctx context.Context, query string, args ...interface{}) (*gocql.Iter, error) {
						return &gocql.Iter{}, nil
					},
					ScanFunc: func(iter *gocql.Iter, dest ...interface{}) bool { return false },
				},
			},
			args: args{
				mg: &v1alpha1.Keyspace{},
			},
			want: want{
				o: managed.ExternalObservation{
					ResourceExists: false,
				},
			},
		},
		"KeyspaceExists": {
			reason: "Should return ResourceExists: true when the keyspace exists",
			fields: fields{
				db: &cassandra.MockDB{
					QueryFunc: func(ctx context.Context, query string, args ...interface{}) (*gocql.Iter, error) {
						return &gocql.Iter{}, nil
					},
					ScanFunc: func(iter *gocql.Iter, dest ...interface{}) bool {
						if len(dest) == 1 {
							if name, ok := dest[0].(*string); ok {
								*name = "example_keyspace"
							}
							return true
						} else if len(dest) == 2 {
							if replicationMap, ok := dest[0].(*map[string]string); ok {
								(*replicationMap)["class"] = "SimpleStrategy"
								(*replicationMap)["replication_factor"] = "2"
							}
							if durableWrites, ok := dest[1].(**bool); ok && durableWrites != nil {
								*durableWrites = pointerToBool(true)
							}
							return true
						}
						return false
					},
				},
			},
			args: args{
				mg: &v1alpha1.Keyspace{
					Spec: v1alpha1.KeyspaceSpec{
						ForProvider: v1alpha1.KeyspaceParameters{
							ReplicationClass:  pointerToString("SimpleStrategy"),
							ReplicationFactor: pointerToInt(2),
							DurableWrites:     pointerToBool(true),
						},
					},
				},
			},
			want: want{
				o: managed.ExternalObservation{
					ResourceExists:          true,
					ResourceUpToDate:        true,
					ResourceLateInitialized: false,
				},
			},
		},
		"LateInit": {
			reason: "Should return LateInit if some params need be backfield",
			fields: fields{
				db: &cassandra.MockDB{
					QueryFunc: func(ctx context.Context, query string, args ...interface{}) (*gocql.Iter, error) {
						return &gocql.Iter{}, nil
					},
					ScanFunc: func(iter *gocql.Iter, dest ...interface{}) bool {
						if len(dest) == 1 {
							if name, ok := dest[0].(*string); ok {
								*name = "example_keyspace"
							}
							return true
						} else if len(dest) == 2 {
							if replicationMap, ok := dest[0].(*map[string]string); ok {
								(*replicationMap)["class"] = "SimpleStrategy"
								(*replicationMap)["replication_factor"] = "2"
							}
							if durableWrites, ok := dest[1].(**bool); ok && durableWrites != nil {
								*durableWrites = pointerToBool(true)
							}
							return true
						}
						return false
					},
				},
			},
			args: args{
				mg: &v1alpha1.Keyspace{
					Spec: v1alpha1.KeyspaceSpec{
						ForProvider: v1alpha1.KeyspaceParameters{
							ReplicationClass: pointerToString("SimpleStrategy"),
							DurableWrites:    pointerToBool(true),
						},
					},
				},
			},
			want: want{
				o: managed.ExternalObservation{
					ResourceExists:          true,
					ResourceUpToDate:        true,
					ResourceLateInitialized: true,
				},
			},
		},
		"ResourceOutdated": {
			reason: "Should return ResourceUpToDate: false if out of date",
			fields: fields{
				db: &cassandra.MockDB{
					QueryFunc: func(ctx context.Context, query string, args ...interface{}) (*gocql.Iter, error) {
						return &gocql.Iter{}, nil
					},
					ScanFunc: func(iter *gocql.Iter, dest ...interface{}) bool {
						if len(dest) == 1 {
							if name, ok := dest[0].(*string); ok {
								*name = "example_keyspace"
							}
							return true
						} else if len(dest) == 2 {
							if replicationMap, ok := dest[0].(*map[string]string); ok {
								(*replicationMap)["class"] = "SimpleStrategy"
								(*replicationMap)["replication_factor"] = "3"
							}
							if durableWrites, ok := dest[1].(**bool); ok && durableWrites != nil {
								*durableWrites = pointerToBool(true)
							}
							return true
						}
						return false
					},
				},
			},
			args: args{
				mg: &v1alpha1.Keyspace{
					Spec: v1alpha1.KeyspaceSpec{
						ForProvider: v1alpha1.KeyspaceParameters{
							ReplicationClass:  pointerToString("SimpleStrategy"),
							ReplicationFactor: pointerToInt(2),
							DurableWrites:     pointerToBool(true),
						},
					},
				},
			},
			want: want{
				o: managed.ExternalObservation{
					ResourceExists:          true,
					ResourceUpToDate:        false,
					ResourceLateInitialized: false,
				},
			},
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			e := external{db: tc.fields.db}
			got, err := e.Observe(tc.args.ctx, tc.args.mg)
			if diff := cmp.Diff(tc.want.err, err, test.EquateErrors()); diff != "" {
				t.Errorf("\n%s\nObserve(...): -want error, +got error:\n%s\n", tc.reason, diff)
			}
			if diff := cmp.Diff(tc.want.o, got); diff != "" {
				t.Errorf("\n%s\nObserve(...): -want, +got:\n%s\n", tc.reason, diff)
			}
		})
	}
}

func TestUpdate(t *testing.T) {
	errBoom := errors.New("boom")

	type fields struct {
		db cassandra.DB
	}

	type args struct {
		ctx context.Context
		mg  resource.Managed
	}

	type want struct {
		u   managed.ExternalUpdate
		err error
	}

	cases := map[string]struct {
		reason string
		fields fields
		args   args
		want   want
	}{
		"ErrNotKeyspace": {
			reason: "Should return an error if the managed resource is not a *Keyspace",
			args: args{
				mg: nil,
			},
			want: want{
				err: errors.New(errNotKeyspace),
			},
		},
		"UpdateKeyspaceSuccess": {
			reason: "Should successfully update the keyspace if the update query succeeds",
			fields: fields{
				db: &cassandra.MockDB{
					ExecFunc: func(ctx context.Context, query string, args ...interface{}) error {
						expectedQuery := "ALTER KEYSPACE \"example_keyspace\" WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 2} AND durable_writes = true"
						if query != expectedQuery {

							return errors.New("unexpected query: " + query)
						}
						return nil
					},
				},
			},
			args: args{
				mg: &v1alpha1.Keyspace{
					ObjectMeta: metav1.ObjectMeta{
						Annotations: map[string]string{
							"crossplane.io/external-name": "example_keyspace",
						},
					},
					Spec: v1alpha1.KeyspaceSpec{
						ForProvider: v1alpha1.KeyspaceParameters{
							ReplicationClass:  pointerToString("SimpleStrategy"),
							ReplicationFactor: pointerToInt(2),
							DurableWrites:     pointerToBool(true),
						},
					},
				},
			},
			want: want{
				u:   managed.ExternalUpdate{},
				err: nil,
			},
		},
		"UpdateKeyspaceFailure": {
			reason: "Should return an error if the update query fails",
			fields: fields{
				db: &cassandra.MockDB{
					ExecFunc: func(ctx context.Context, query string, args ...interface{}) error {
						return errBoom
					},
				},
			},
			args: args{
				mg: &v1alpha1.Keyspace{
					Spec: v1alpha1.KeyspaceSpec{
						ForProvider: v1alpha1.KeyspaceParameters{
							ReplicationClass:  pointerToString("SimpleStrategy"),
							ReplicationFactor: pointerToInt(2),
							DurableWrites:     pointerToBool(true),
						},
					},
				},
			},
			want: want{
				u:   managed.ExternalUpdate{},
				err: errors.New(errUpdateKeyspace + ": " + errBoom.Error()),
			},
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			e := external{db: tc.fields.db}
			got, err := e.Update(tc.args.ctx, tc.args.mg)
			if diff := cmp.Diff(tc.want.err, err, test.EquateErrors()); diff != "" {
				t.Errorf("\n%s\nUpdate(...): -want error, +got error:\n%s\n", tc.reason, diff)
			}
			if diff := cmp.Diff(tc.want.u, got); diff != "" {
				t.Errorf("\n%s\nUpdate(...): -want, +got:\n%s\n", tc.reason, diff)
			}
		})
	}
}

func TestCreate(t *testing.T) {
	errBoom := errors.New("boom")

	type fields struct {
		db cassandra.DB
	}

	type args struct {
		ctx context.Context
		mg  resource.Managed
	}

	type want struct {
		c   managed.ExternalCreation
		err error
	}

	cases := map[string]struct {
		reason string
		fields fields
		args   args
		want   want
	}{
		"ErrNotKeyspace": {
			reason: "Should return an error if the managed resource is not a *Keyspace",
			args: args{
				mg: nil,
			},
			want: want{
				err: errors.New(errNotKeyspace),
			},
		},
		"CreateKeyspaceSuccess": {
			reason: "Should successfully create the keyspace if the create query succeeds",
			fields: fields{
				db: &cassandra.MockDB{
					ExecFunc: func(ctx context.Context, query string, args ...interface{}) error {
						expectedQuery := "CREATE KEYSPACE IF NOT EXISTS \"example_keyspace\" WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 2} AND durable_writes = true"
						if query != expectedQuery {
							return errors.New("unexpected query: " + query)
						}
						return nil
					},
				},
			},
			args: args{
				mg: &v1alpha1.Keyspace{
					ObjectMeta: metav1.ObjectMeta{
						Annotations: map[string]string{
							"crossplane.io/external-name": "example_keyspace",
						},
					},
					Spec: v1alpha1.KeyspaceSpec{
						ForProvider: v1alpha1.KeyspaceParameters{
							ReplicationClass:  pointerToString("SimpleStrategy"),
							ReplicationFactor: pointerToInt(2),
							DurableWrites:     pointerToBool(true),
						},
					},
				},
			},
			want: want{
				c:   managed.ExternalCreation{},
				err: nil,
			},
		},
		"CreateKeyspaceFailure": {
			reason: "Should return an error if the create query fails",
			fields: fields{
				db: &cassandra.MockDB{
					ExecFunc: func(ctx context.Context, query string, args ...interface{}) error {
						return errBoom
					},
				},
			},
			args: args{
				mg: &v1alpha1.Keyspace{
					ObjectMeta: metav1.ObjectMeta{
						Annotations: map[string]string{
							"crossplane.io/external-name": "example_keyspace",
						},
					},
					Spec: v1alpha1.KeyspaceSpec{
						ForProvider: v1alpha1.KeyspaceParameters{
							ReplicationClass:  pointerToString("SimpleStrategy"),
							ReplicationFactor: pointerToInt(2),
							DurableWrites:     pointerToBool(true),
						},
					},
				},
			},
			want: want{
				c:   managed.ExternalCreation{},
				err: errors.New(errCreateKeyspace + ": " + errBoom.Error()),
			},
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			e := external{db: tc.fields.db}
			got, err := e.Create(tc.args.ctx, tc.args.mg)
			if diff := cmp.Diff(tc.want.err, err, test.EquateErrors()); diff != "" {
				t.Errorf("\n%s\nCreate(...): -want error, +got error:\n%s\n", tc.reason, diff)
			}
			if diff := cmp.Diff(tc.want.c, got); diff != "" {
				t.Errorf("\n%s\nCreate(...): -want, +got:\n%s\n", tc.reason, diff)
			}
		})
	}
}

func TestDelete(t *testing.T) {
	errBoom := errors.New("boom")

	type fields struct {
		db cassandra.DB
	}

	type args struct {
		ctx context.Context
		mg  resource.Managed
	}

	type want struct {
		err error
	}

	cases := map[string]struct {
		reason string
		fields fields
		args   args
		want   want
	}{
		"ErrNotKeyspace": {
			reason: "Should return an error if the managed resource is not a *Keyspace",
			args: args{
				mg: nil,
			},
			want: want{
				err: errors.New(errNotKeyspace),
			},
		},
		"DeleteKeyspaceSuccess": {
			reason: "Should successfully delete the keyspace if the delete query succeeds",
			fields: fields{
				db: &cassandra.MockDB{
					ExecFunc: func(ctx context.Context, query string, args ...interface{}) error {
						expectedQuery := "DROP KEYSPACE IF EXISTS \"example_keyspace\""
						if query != expectedQuery {
							return errors.New("unexpected query: " + query)
						}
						return nil
					},
				},
			},
			args: args{
				mg: &v1alpha1.Keyspace{
					ObjectMeta: metav1.ObjectMeta{
						Annotations: map[string]string{
							"crossplane.io/external-name": "example_keyspace",
						},
					},
				},
			},
			want: want{
				err: nil,
			},
		},
		"DeleteKeyspaceFailure": {
			reason: "Should return an error if the delete query fails",
			fields: fields{
				db: &cassandra.MockDB{
					ExecFunc: func(ctx context.Context, query string, args ...interface{}) error {
						return errBoom
					},
				},
			},
			args: args{
				mg: &v1alpha1.Keyspace{
					ObjectMeta: metav1.ObjectMeta{
						Annotations: map[string]string{
							"crossplane.io/external-name": "example_keyspace",
						},
					},
				},
			},
			want: want{
				err: errors.New(errDropKeyspace + ": " + errBoom.Error()),
			},
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			e := external{db: tc.fields.db}
			err := e.Delete(tc.args.ctx, tc.args.mg)
			if diff := cmp.Diff(tc.want.err, err, test.EquateErrors()); diff != "" {
				t.Errorf("\n%s\nDelete(...): -want error, +got error:\n%s\n", tc.reason, diff)
			}
		})
	}
}
