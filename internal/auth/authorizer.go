package auth

import (
	"fmt"

	"github.com/casbin/casbin/v2"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Authorizer struct {
	enforcer *casbin.Enforcer
}

func NewAuthorizer(model string, policy string) (*Authorizer, error) {
	enforcer, err := casbin.NewEnforcer(model, policy)
	if err != nil {
		return nil, fmt.Errorf("error creating new enforcer: %w", err)
	}
	return &Authorizer{enforcer: enforcer}, nil
}

func (azr *Authorizer) Authorize(subject, object, action string) error {
	if ok, _ := azr.enforcer.Enforce(subject, object, action); !ok {
		msg := fmt.Sprintf("%s not permitted to %s to %s", subject, action, object)
		sts := status.New(codes.PermissionDenied, msg)
		return sts.Err()
	}
	return nil
}
