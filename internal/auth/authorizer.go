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

func NewAuthorizer(model string, policy string) *Authorizer {
	enforcer, _ := casbin.NewEnforcer(model, policy)
	return &Authorizer{
		enforcer: enforcer,
	}
}

func (a *Authorizer) Authorize(subject, object, action string) error {
	ok, _ := a.enforcer.Enforce(subject, object, action)
	if !ok {
		msg := fmt.Sprintf("%s not permitted to %s to %s", subject, object, action)
		sts := status.New(codes.PermissionDenied, msg)
		return sts.Err()
	}
	return nil
}
