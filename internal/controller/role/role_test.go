package role

import (
	"context"
	"fmt"
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

func pointerToBool(b bool) *bool {
	return &b
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
		"ErrNotRole": {
			reason: "Should return an error if the managed resource is not a *Role",
			args: args{
				mg: nil,
			},
			want: want{
				err: errors.New(errNotRole),
			},
		},
		"RoleNotFound": {
			reason: "Should return ResourceExists: false when the role does not exist",
			fields: fields{
				db: &cassandra.MockDB{
					QueryFunc: func(ctx context.Context, query string, args ...interface{}) (*gocql.Iter, error) {
						return &gocql.Iter{}, nil
					},
					ScanFunc: func(iter *gocql.Iter, dest ...interface{}) bool { return false },
				},
			},
			args: args{
				mg: &v1alpha1.Role{},
			},
			want: want{
				o: managed.ExternalObservation{
					ResourceExists: false,
				},
			},
		},
		"RoleExists": {
			reason: "Should return ResourceExists: true when the role exists",
			fields: fields{
				db: &cassandra.MockDB{
					QueryFunc: func(ctx context.Context, query string, args ...interface{}) (*gocql.Iter, error) {
						return &gocql.Iter{}, nil
					},
					ScanFunc: func(iter *gocql.Iter, dest ...interface{}) bool {
						if len(dest) > 1 {
							if isSuperuser, ok := dest[0].(*bool); ok {
								*isSuperuser = true
							}
							if canLogin, ok := dest[1].(*bool); ok {
								*canLogin = true
							}
						}
						return true
					},
				},
			},
			args: args{
				mg: &v1alpha1.Role{
					Spec: v1alpha1.RoleSpec{
						ForProvider: v1alpha1.RoleParameters{
							Privileges: v1alpha1.RolePrivilege{
								SuperUser: boolPtr(true),
								Login:     boolPtr(true),
							},
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

func TestCreate(t *testing.T) {
	errBoom := errors.New("boom")
	originalGeneratePassword := generatePassword
	defer func() { generatePassword = originalGeneratePassword }()

	generatePassword = func() (string, error) {
		return "mocked-password", nil
	}

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
		"ErrNotRole": {
			reason: "Should return an error if the managed resource is not a *Role",
			args: args{
				mg: nil,
			},
			want: want{
				err: errors.New(errNotRole),
			},
		},
		"CreateRoleSuccess": {
			reason: "Should successfully create the role if the create query succeeds",
			fields: fields{
				db: &cassandra.MockDB{
					ExecFunc: func(ctx context.Context, query string, args ...interface{}) error {
						expectedQuery := "CREATE ROLE IF NOT EXISTS \"example_role\" WITH SUPERUSER = true AND LOGIN = true AND PASSWORD = 'mocked-password'"
						if query != expectedQuery {
							return fmt.Errorf("unexpected query: %s", query)
						}
						return nil
					},
				},
			},
			args: args{
				mg: &v1alpha1.Role{
					ObjectMeta: metav1.ObjectMeta{
						Annotations: map[string]string{
							"crossplane.io/external-name": "example_role",
						},
					},
					Spec: v1alpha1.RoleSpec{
						ForProvider: v1alpha1.RoleParameters{
							Privileges: v1alpha1.RolePrivilege{
								SuperUser: pointerToBool(true),
								Login:     pointerToBool(true),
							},
						},
					},
				},
			},
			want: want{
				c: managed.ExternalCreation{
					ConnectionDetails: managed.ConnectionDetails{
						"username": []byte("example_role"),
						"password": []byte("mocked-password"),
					},
				},
				err: nil,
			},
		},
		"CreateRoleFailure": {
			reason: "Should return an error if the create query fails",
			fields: fields{
				db: &cassandra.MockDB{
					ExecFunc: func(ctx context.Context, query string, args ...interface{}) error {
						return errBoom
					},
				},
			},
			args: args{
				mg: &v1alpha1.Role{},
			},
			want: want{
				err: errors.New(errCreateRole + ": " + errBoom.Error()),
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
		"ErrNotRole": {
			reason: "Should return an error if the managed resource is not a *Role",
			args: args{
				mg: nil,
			},
			want: want{
				err: errors.New(errNotRole),
			},
		},
		"UpdateRoleSuccess": {
			reason: "Should successfully update the role if the update query succeeds",
			fields: fields{
				db: &cassandra.MockDB{
					ExecFunc: func(ctx context.Context, query string, args ...interface{}) error {
						expectedQuery := "ALTER ROLE \"example_role\" WITH SUPERUSER = true AND LOGIN = false"
						if query != expectedQuery {
							return fmt.Errorf("unexpected query: %s", query)
						}
						return nil
					},
				},
			},
			args: args{
				mg: &v1alpha1.Role{
					ObjectMeta: metav1.ObjectMeta{
						Annotations: map[string]string{
							"crossplane.io/external-name": "example_role",
						},
					},
					Spec: v1alpha1.RoleSpec{
						ForProvider: v1alpha1.RoleParameters{
							Privileges: v1alpha1.RolePrivilege{
								SuperUser: pointerToBool(true),
								Login:     pointerToBool(false),
							},
						},
					},
				},
			},
			want: want{
				u:   managed.ExternalUpdate{},
				err: nil,
			},
		},
		"UpdateRoleFailure": {
			reason: "Should return an error if the update query fails",
			fields: fields{
				db: &cassandra.MockDB{
					ExecFunc: func(ctx context.Context, query string, args ...interface{}) error {
						return errBoom
					},
				},
			},
			args: args{
				mg: &v1alpha1.Role{
					ObjectMeta: metav1.ObjectMeta{
						Annotations: map[string]string{
							"crossplane.io/external-name": "example_role",
						},
					},
				},
			},
			want: want{
				u:   managed.ExternalUpdate{},
				err: errors.New(errUpdateRole + ": " + errBoom.Error()),
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
		"ErrNotRole": {
			reason: "Should return an error if the managed resource is not a *Role",
			args: args{
				mg: nil,
			},
			want: want{
				err: errors.New(errNotRole),
			},
		},
		"DeleteRoleSuccess": {
			reason: "Should successfully delete the role if the delete query succeeds",
			fields: fields{
				db: &cassandra.MockDB{
					ExecFunc: func(ctx context.Context, query string, args ...interface{}) error {
						expectedQuery := "DROP ROLE IF EXISTS \"example_role\""
						if query != expectedQuery {
							return fmt.Errorf("unexpected query: %s", query)
						}
						return nil
					},
				},
			},
			args: args{
				mg: &v1alpha1.Role{
					ObjectMeta: metav1.ObjectMeta{
						Annotations: map[string]string{
							"crossplane.io/external-name": "example_role",
						},
					},
				},
			},
			want: want{
				err: nil,
			},
		},
		"DeleteRoleFailure": {
			reason: "Should return an error if the delete query fails",
			fields: fields{
				db: &cassandra.MockDB{
					ExecFunc: func(ctx context.Context, query string, args ...interface{}) error {
						return errBoom
					},
				},
			},
			args: args{
				mg: &v1alpha1.Role{
					ObjectMeta: metav1.ObjectMeta{
						Annotations: map[string]string{
							"crossplane.io/external-name": "example_role",
						},
					},
				},
			},
			want: want{
				err: errors.New(errDropRole + ": " + errBoom.Error()),
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

// Add similar test suites for `Update` and `Delete` following the above format.

// boolPtr returns a pointer to the given bool value
func boolPtr(b bool) *bool {
	return &b
}
