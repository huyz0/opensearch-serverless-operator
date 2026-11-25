# Generated development schema

The instance CRD in this directory is generated from the current Go types with controller-gen v0.17.3. The Go API represents only an initial subset of the planned fields. Installing this CRD would prune fields that are not in that subset. It has not passed API-server validation and must not be installed for production or used with the full design examples.

Generate it from the repository root with controller-gen v0.17.3 on `PATH`:

```sh
controller-gen crd paths=./api/v1alpha1 output:crd:artifacts:config=config/generated-dev
```

M01 must complete both APIs, add action-specific validation markers and exercise both generated schemas on a real API server. The generated operation CRD is also incomplete and must not be installed for production.
