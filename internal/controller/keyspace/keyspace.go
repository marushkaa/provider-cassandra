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

package keyspace

import (
	"context"
	"encoding/json"
	"strconv"
	"strings"

	"github.com/pkg/errors"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"

	"github.com/crossplane/provider-cassandra/internal/clients/cassandra"

	xpv1 "github.com/crossplane/crossplane-runtime/apis/common/v1"
	"github.com/crossplane/crossplane-runtime/pkg/connection"
	"github.com/crossplane/crossplane-runtime/pkg/controller"
	"github.com/crossplane/crossplane-runtime/pkg/event"
	"github.com/crossplane/crossplane-runtime/pkg/meta"
	"github.com/crossplane/crossplane-runtime/pkg/ratelimiter"
	"github.com/crossplane/crossplane-runtime/pkg/reconciler/managed"
	"github.com/crossplane/crossplane-runtime/pkg/resource"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/crossplane/provider-cassandra/apis/cql/v1alpha1"
	apisv1alpha1 "github.com/crossplane/provider-cassandra/apis/v1alpha1"
	"github.com/crossplane/provider-cassandra/internal/features"
)

const (
	errNotKeyspace    = "managed resource is not a Keyspace custom resource"
	errTrackPCUsage   = "cannot track ProviderConfig usage"
	errGetPC          = "cannot get ProviderConfig"
	errGetCreds       = "cannot get credentials"
	errNewClient      = "cannot create new Service"
	errSelectKeyspace = "cannot select keyspace"
	errCreateKeyspace = "cannot create keyspace"
	errUpdateKeyspace = "cannot update keyspace"
	errDropKeyspace   = "cannot drop keyspace"
	maxConcurrency    = 5
	defaultStrategy   = "SimpleStrategy"
	defaultReplicas   = 1
)

// Setup adds a controller that reconciles Keyspace managed resources.
func Setup(mgr ctrl.Manager, o controller.Options) error {
	name := managed.ControllerName(v1alpha1.KeyspaceGroupKind)

	cps := []managed.ConnectionPublisher{managed.NewAPISecretPublisher(mgr.GetClient(), mgr.GetScheme())}
	if o.Features.Enabled(features.EnableAlphaExternalSecretStores) {
		cps = append(cps, connection.NewDetailsManager(mgr.GetClient(), apisv1alpha1.StoreConfigGroupVersionKind))
	}

	r := managed.NewReconciler(mgr,
		resource.ManagedKind(v1alpha1.KeyspaceGroupVersionKind),
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
		For(&v1alpha1.Keyspace{}).
		Complete(ratelimiter.NewReconciler(name, r, o.GlobalRateLimiter))
}

type connector struct {
	kube      client.Client
	usage     resource.Tracker
	newClient func(creds map[string][]byte, keyspace string, consistencyLevel string) cassandra.DB
}

func (c *connector) Connect(ctx context.Context, mg resource.Managed) (managed.ExternalClient, error) {
	cr, ok := mg.(*v1alpha1.Keyspace)
	if !ok {
		return nil, errors.New(errNotKeyspace)
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
	cr, ok := mg.(*v1alpha1.Keyspace)
	if !ok {
		return managed.ExternalObservation{}, errors.New(errNotKeyspace)
	}

	exists, err := c.keyspaceExists(ctx, cr)
	if err != nil {
		return managed.ExternalObservation{}, err
	}
	if !exists {
		return managed.ExternalObservation{
			ResourceExists:   false,
			ResourceUpToDate: false,
		}, nil
	}

	observed, err := c.getKeyspaceDetails(ctx, cr)
	if err != nil {
		return managed.ExternalObservation{}, err
	}

	cr.SetConditions(xpv1.Available())

	return managed.ExternalObservation{
		ResourceExists:          true,
		ResourceLateInitialized: lateInit(observed, &cr.Spec.ForProvider),
		ResourceUpToDate:        upToDate(observed, &cr.Spec.ForProvider),
	}, nil
}

func (c *external) keyspaceExists(ctx context.Context, cr *v1alpha1.Keyspace) (bool, error) {
	query := "SELECT keyspace_name FROM system_schema.keyspaces WHERE keyspace_name = ?"
	var keyspaceName string
	iter, err := c.db.Query(ctx, query, meta.GetExternalName(cr))
	if err != nil {
		return false, errors.Wrap(err, "failed to check keyspace existence")
	}
	defer func() {
		if closeErr := iter.Close(); closeErr != nil && err == nil {
			err = errors.Wrap(closeErr, "failed to close iterator")
		}
	}()

	if !c.db.Scan(iter, &keyspaceName) {
		return false, nil
	}
	return true, nil
}

func (c *external) getKeyspaceDetails(ctx context.Context, cr *v1alpha1.Keyspace) (*v1alpha1.KeyspaceParameters, error) {
	query := "SELECT replication, durable_writes FROM system_schema.keyspaces WHERE keyspace_name = ?"
	iter, err := c.db.Query(ctx, query, meta.GetExternalName(cr))
	if err != nil {
		return nil, errors.Wrap(err, errSelectKeyspace)
	}
	defer func() {
		if closeErr := iter.Close(); closeErr != nil && err == nil {
			err = errors.Wrap(closeErr, "failed to close iterator")
		}
	}()

	observed := &v1alpha1.KeyspaceParameters{
		ReplicationClass:  new(string),
		ReplicationFactor: new(int),
		DurableWrites:     new(bool),
	}

	replicationMap := map[string]string{}
	if !c.db.Scan(iter, &replicationMap, &observed.DurableWrites) {
		return nil, errors.New("failed to scan keyspace attributes")
	}

	if rc, ok := replicationMap["class"]; ok {
		*observed.ReplicationClass = strings.TrimPrefix(rc, "org.apache.cassandra.locator.")
	}
	if rf, ok := replicationMap["replication_factor"]; ok {
		rfInt, _ := strconv.Atoi(rf)
		*observed.ReplicationFactor = rfInt
	}

	return observed, nil
}

func (c *external) Create(ctx context.Context, mg resource.Managed) (managed.ExternalCreation, error) {
	cr, ok := mg.(*v1alpha1.Keyspace)
	if !ok {
		return managed.ExternalCreation{}, errors.New(errNotKeyspace)
	}

	params := cr.Spec.ForProvider
	strategy := defaultStrategy
	if params.ReplicationClass != nil {
		strategy = *params.ReplicationClass
	}

	replicationFactor := defaultReplicas
	if params.ReplicationFactor != nil {
		replicationFactor = *params.ReplicationFactor
	}

	durableWrites := true
	if params.DurableWrites != nil {
		durableWrites = *params.DurableWrites
	}

	query := "CREATE KEYSPACE IF NOT EXISTS " + cassandra.QuoteIdentifier(meta.GetExternalName(cr)) +
		" WITH replication = {'class': '" + strategy + "', 'replication_factor': " + strconv.Itoa(replicationFactor) + "} AND durable_writes = " + strconv.FormatBool(durableWrites)

	if err := c.db.Exec(ctx, query); err != nil {
		return managed.ExternalCreation{}, errors.New(errCreateKeyspace + ": " + err.Error())
	}

	return managed.ExternalCreation{}, nil
}

func (c *external) Update(ctx context.Context, mg resource.Managed) (managed.ExternalUpdate, error) {
	cr, ok := mg.(*v1alpha1.Keyspace)
	if !ok {
		return managed.ExternalUpdate{}, errors.New(errNotKeyspace)
	}

	params := cr.Spec.ForProvider
	strategy := defaultStrategy
	if params.ReplicationClass != nil {
		strategy = *params.ReplicationClass
	}

	replicationFactor := defaultReplicas
	if params.ReplicationFactor != nil {
		replicationFactor = *params.ReplicationFactor
	}

	durableWrites := true
	if params.DurableWrites != nil {
		durableWrites = *params.DurableWrites
	}

	query := "ALTER KEYSPACE " + cassandra.QuoteIdentifier(meta.GetExternalName(cr)) +
		" WITH replication = {'class': '" + strategy + "', 'replication_factor': " + strconv.Itoa(replicationFactor) + "} AND durable_writes = " + strconv.FormatBool(durableWrites)

	if err := c.db.Exec(ctx, query); err != nil {
		return managed.ExternalUpdate{}, errors.New(errUpdateKeyspace + ": " + err.Error())
	}

	return managed.ExternalUpdate{}, nil
}

func (c *external) Delete(ctx context.Context, mg resource.Managed) error {
	cr, ok := mg.(*v1alpha1.Keyspace)
	if !ok {
		return errors.New(errNotKeyspace)
	}

	query := "DROP KEYSPACE IF EXISTS " + cassandra.QuoteIdentifier(meta.GetExternalName(cr))
	if err := c.db.Exec(ctx, query); err != nil {
		return errors.New(errDropKeyspace + ": " + err.Error())
	}

	return nil
}

func upToDate(observed *v1alpha1.KeyspaceParameters, desired *v1alpha1.KeyspaceParameters) bool {
	if observed.ReplicationClass == nil || desired.ReplicationClass == nil || *observed.ReplicationClass != *desired.ReplicationClass {
		return false
	}
	if observed.ReplicationFactor == nil || desired.ReplicationFactor == nil || *observed.ReplicationFactor != *desired.ReplicationFactor {
		return false
	}
	if observed.DurableWrites == nil || desired.DurableWrites == nil || *observed.DurableWrites != *desired.DurableWrites {
		return false
	}
	return true
}

func lateInit(observed *v1alpha1.KeyspaceParameters, desired *v1alpha1.KeyspaceParameters) bool {
	li := false

	if desired.ReplicationClass == nil {
		desired.ReplicationClass = observed.ReplicationClass
		li = true
	}
	if desired.ReplicationFactor == nil {
		desired.ReplicationFactor = observed.ReplicationFactor
		li = true
	}
	if desired.DurableWrites == nil {
		desired.DurableWrites = observed.DurableWrites
		li = true
	}

	return li
}
