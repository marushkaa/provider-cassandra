/*
Copyright 2022 The Crossplane Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package grant

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/pkg/errors"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/crossplane/crossplane-runtime/pkg/connection"
	"github.com/crossplane/crossplane-runtime/pkg/controller"
	"github.com/crossplane/crossplane-runtime/pkg/event"
	"github.com/crossplane/crossplane-runtime/pkg/ratelimiter"
	"github.com/crossplane/crossplane-runtime/pkg/reconciler/managed"
	"github.com/crossplane/crossplane-runtime/pkg/resource"
	"github.com/crossplane/provider-cassandra/internal/clients/cassandra"

	xpv1 "github.com/crossplane/crossplane-runtime/apis/common/v1"

	"github.com/crossplane/provider-cassandra/apis/cql/v1alpha1"
	apisv1alpha1 "github.com/crossplane/provider-cassandra/apis/v1alpha1"
	"github.com/crossplane/provider-cassandra/internal/features"
)

const (
	errNotGrant     = "managed resource is not a Grant custom resource"
	errTrackPCUsage = "cannot track ProviderConfig usage"
	errGetPC        = "cannot get ProviderConfig"
	errGetCreds     = "cannot get credentials"

	errNewClient    = "cannot create new Service"
	errGrantCreate  = "cannot create grant"
	errGrantDelete  = "cannot delete grant"
	errGrantObserve = "cannot observe grant"
	maxConcurrency  = 5
)

// Setup adds a controller that reconciles Grant managed resources.
func Setup(mgr ctrl.Manager, o controller.Options) error {
	name := managed.ControllerName(v1alpha1.GrantGroupKind)

	cps := []managed.ConnectionPublisher{managed.NewAPISecretPublisher(mgr.GetClient(), mgr.GetScheme())}
	if o.Features.Enabled(features.EnableAlphaExternalSecretStores) {
		cps = append(cps, connection.NewDetailsManager(mgr.GetClient(), apisv1alpha1.StoreConfigGroupVersionKind))
	}

	r := managed.NewReconciler(mgr,
		resource.ManagedKind(v1alpha1.GrantGroupVersionKind),
		managed.WithExternalConnecter(&connector{
			kube:      mgr.GetClient(),
			usage:     resource.NewProviderConfigUsageTracker(mgr.GetClient(), &apisv1alpha1.ProviderConfigUsage{}),
			newClient: cassandra.New}),
		managed.WithLogger(o.Logger.WithValues("controller", name)),
		managed.WithPollInterval(o.PollInterval),
		managed.WithRecorder(event.NewAPIRecorder(mgr.GetEventRecorderFor(name))),
		managed.WithConnectionPublishers(cps...))

	return ctrl.NewControllerManagedBy(mgr).
		Named(name).
		WithOptions(o.ForControllerRuntime()).
		WithEventFilter(resource.DesiredStateChanged()).
		For(&v1alpha1.Grant{}).
		Complete(ratelimiter.NewReconciler(name, r, o.GlobalRateLimiter))
}

// A connector is expected to produce an ExternalClient when its Connect method
// is called.
type connector struct {
	kube      client.Client
	usage     resource.Tracker
	newClient func(creds map[string][]byte, keyspace string, consistencyLevel string) cassandra.DB
}

func (c *connector) Connect(ctx context.Context, mg resource.Managed) (managed.ExternalClient, error) {
	cr, ok := mg.(*v1alpha1.Grant)
	if !ok {
		return nil, errors.New(errNotGrant)
	}

	if err := c.usage.Track(ctx, mg); err != nil {
		return nil, errors.Wrap(err, errTrackPCUsage)
	}

	pc := &apisv1alpha1.ProviderConfig{}
	if err := c.kube.Get(ctx, types.NamespacedName{Name: cr.GetProviderConfigReference().Name}, pc); err != nil {
		return nil, errors.Wrap(err, errGetPC)
	}

	cd := pc.Spec.Credentials
	credsData, err := resource.CommonCredentialExtractor(ctx, cd.Source, c.kube, cd.CommonCredentialSelectors)
	if err != nil {
		return nil, errors.Wrap(err, errGetCreds)
	}

	// Convert the byte array to a string and parse the JSON
	credsJSON := string(credsData)
	var credsMap map[string]string
	if err := json.Unmarshal([]byte(credsJSON), &credsMap); err != nil {
		return nil, errors.Wrap(err, "failed to parse credentials JSON")
	}

	// Convert map[string]string to map[string][]byte
	creds := make(map[string][]byte)
	for k, v := range credsMap {
		creds[k] = []byte(v)
	}

	consistencyLevel := ""
	if pc.Spec.ConsistencyLevel != nil {
		consistencyLevel = *pc.Spec.ConsistencyLevel
	}

	db := c.newClient(creds, "", consistencyLevel)

	return &external{db: db}, nil
}

type external struct {
	db cassandra.DB
}

func (c *external) Observe(ctx context.Context, mg resource.Managed) (managed.ExternalObservation, error) {
	cr, ok := mg.(*v1alpha1.Grant)
	if !ok {
		return managed.ExternalObservation{}, errors.New(errNotGrant)
	}

	role := *cr.Spec.ForProvider.Role
	keyspace := *cr.Spec.ForProvider.Keyspace

	observedPermissions, resourceExists, err := c.getObservedPermissions(ctx, role, keyspace)
	if err != nil {
		return managed.ExternalObservation{}, err
	}

	desiredPermissions := c.getDesiredPermissions(cr.Spec.ForProvider.Privileges)
	upToDate := c.comparePermissions(observedPermissions, desiredPermissions, &cr.Status.AtProvider)

	if upToDate {
		cr.Status.AtProvider.Privileges = replaceUnderscoreWithSpace(cr.Spec.ForProvider.Privileges)
	}

	if resourceExists {
		cr.SetConditions(xpv1.Available())
	}

	return managed.ExternalObservation{
		ResourceExists:          resourceExists,
		ResourceLateInitialized: false,
		ResourceUpToDate:        upToDate,
	}, nil
}

func (c *external) getObservedPermissions(ctx context.Context, role, keyspace string) (map[string]bool, bool, error) {
	query := fmt.Sprintf("SELECT permissions FROM system_auth.role_permissions WHERE role = ? AND resource = 'data/%s'", keyspace)
	iter, err := c.db.Query(ctx, query, role)
	if err != nil {
		return nil, false, errors.Wrap(err, errGrantObserve)
	}
	defer func() {
		if closeErr := iter.Close(); closeErr != nil && err == nil {
			err = errors.Wrap(closeErr, "failed to close iterator")
		}
	}()

	observedPermissions := make(map[string]bool)
	resourceExists := false
	var permissions []string
	for c.db.Scan(iter, &permissions) {
		for _, p := range permissions {
			observedPermissions[p] = true
		}
		resourceExists = true
	}

	return observedPermissions, resourceExists, nil
}

func (c *external) getDesiredPermissions(privileges []v1alpha1.GrantPrivilege) map[string]bool {
	desiredPermissions := make(map[string]bool)
	for _, p := range replaceUnderscoreWithSpace(privileges) {
		desiredPermissions[p] = true
	}
	return desiredPermissions
}

func (c *external) comparePermissions(observed, desired map[string]bool, atProvider *v1alpha1.GrantObservation) bool {
	upToDate := true

	for p := range desired {
		if !observed[p] {
			upToDate = false
			break
		}
	}

	for _, p := range atProvider.Privileges {
		if !desired[p] {
			upToDate = false
		}
	}

	return upToDate
}

func (c *external) Create(ctx context.Context, mg resource.Managed) (managed.ExternalCreation, error) {
	cr, ok := mg.(*v1alpha1.Grant)
	if !ok {
		return managed.ExternalCreation{}, errors.New(errNotGrant)
	}

	role := *cr.Spec.ForProvider.Role
	keyspace := *cr.Spec.ForProvider.Keyspace
	privileges := replaceUnderscoreWithSpace(cr.Spec.ForProvider.Privileges)

	for _, privilege := range privileges {
		// we make multiple grants to support yugabyteDB dialect that doesn't allow multiple grants like GRANT SELECT, MODIFY ...
		query := fmt.Sprintf("GRANT %s ON KEYSPACE %s TO %s", privilege, cassandra.QuoteIdentifier(keyspace), cassandra.QuoteIdentifier(role))
		if err := c.db.Exec(ctx, query); err != nil {
			return managed.ExternalCreation{}, errors.Wrap(err, errGrantCreate)
		}
	}

	return managed.ExternalCreation{}, nil
}

func (c *external) Update(ctx context.Context, mg resource.Managed) (managed.ExternalUpdate, error) {
	cr, ok := mg.(*v1alpha1.Grant)
	if !ok {
		return managed.ExternalUpdate{}, errors.New(errNotGrant)
	}

	role := *cr.Spec.ForProvider.Role
	keyspace := *cr.Spec.ForProvider.Keyspace
	privileges := replaceUnderscoreWithSpace(cr.Spec.ForProvider.Privileges)
	desiredPermissions := make(map[string]bool)

	for _, privilege := range privileges {
		query := fmt.Sprintf("GRANT %s ON KEYSPACE %s TO %s", privilege, cassandra.QuoteIdentifier(keyspace), cassandra.QuoteIdentifier(role))
		if err := c.db.Exec(ctx, query); err != nil {
			return managed.ExternalUpdate{}, errors.Wrap(err, errGrantCreate)
		}
		desiredPermissions[privilege] = true
	}

	atProviderPrivileges := cr.Status.AtProvider.Privileges
	for _, p := range atProviderPrivileges {
		if !desiredPermissions[p] {
			query := fmt.Sprintf("REVOKE %s ON KEYSPACE %s FROM %s", p, cassandra.QuoteIdentifier(keyspace), cassandra.QuoteIdentifier(role))
			if err := c.db.Exec(ctx, query); err != nil {
				return managed.ExternalUpdate{}, errors.Wrap(err, errGrantDelete)
			}
		}
	}

	cr.Status.AtProvider.Privileges = privileges

	return managed.ExternalUpdate{}, nil
}

func (c *external) Delete(ctx context.Context, mg resource.Managed) error {
	cr, ok := mg.(*v1alpha1.Grant)
	if !ok {
		return errors.New(errNotGrant)
	}

	role := *cr.Spec.ForProvider.Role
	keyspace := *cr.Spec.ForProvider.Keyspace
	privileges := replaceUnderscoreWithSpace(cr.Spec.ForProvider.Privileges)

	for _, privilege := range privileges {
		query := fmt.Sprintf("REVOKE %s ON KEYSPACE %s FROM %s", privilege, cassandra.QuoteIdentifier(keyspace), cassandra.QuoteIdentifier(role))
		if err := c.db.Exec(ctx, query); err != nil {
			return errors.Wrap(err, errGrantDelete)
		}
	}

	return nil
}

func replaceUnderscoreWithSpace(privileges []v1alpha1.GrantPrivilege) []string {
	replaced := make([]string, len(privileges))
	for i, privilege := range privileges {
		replaced[i] = strings.ReplaceAll(string(privilege), "_", " ")
	}
	return replaced
}
