package tg

import (
	"context"
	"fmt"
	"net"
	"strings"

	"github.com/aws/aws-sdk-go/service/elbv2"
	"github.com/kubernetes-sigs/aws-alb-ingress-controller/internal/albctx"
	"github.com/kubernetes-sigs/aws-alb-ingress-controller/internal/aws"
	"github.com/kubernetes-sigs/aws-alb-ingress-controller/internal/ingress/backend"
	api "k8s.io/api/core/v1"
	extensions "k8s.io/api/extensions/v1beta1"
)

// Targets contains the targets for a target group.
type Targets struct {
	// TgArn is the ARN of the target group
	TgArn string

	// Targets are the targets for the target group
	Targets []*elbv2.TargetDescription

	// TargetType is the type of targets, either ip or instance
	TargetType string

	// Ingress is the ingress for the targets
	Ingress *extensions.Ingress

	// Backend is the ingress backend for the targets
	Backend *extensions.IngressBackend
}

// NewTargets returns a new Targets pointer
func NewTargets(targetType string, ingress *extensions.Ingress, backend *extensions.IngressBackend) *Targets {
	return &Targets{
		TargetType: targetType,
		Ingress:    ingress,
		Backend:    backend,
	}
}

// TargetsController provides functionality to manage targets
type TargetsController interface {
	// Reconcile ensures the target group targets in AWS matches the targets configured in the ingress backend.
	Reconcile(context.Context, *Targets) error
}

// NewTargetsController constructs a new target group targets controller
func NewTargetsController(cloud aws.CloudAPI, endpointResolver backend.EndpointResolver) TargetsController {
	return &targetsController{
		cloud:            cloud,
		endpointResolver: endpointResolver,
	}
}

type targetsController struct {
	cloud            aws.CloudAPI
	endpointResolver backend.EndpointResolver
}

func (c *targetsController) processAbsentTargets(ctx context.Context, errStr string, additions []*elbv2.TargetDescription, t *Targets) error {
	// HACK: Sometimes, Kubernetes does not delete terminated node objects quickly enough. If a new node comes
	// up with the same nodeName but a different instanceID, AWS rejects the calls to register the nodes in the
	// target group. The "fix" here is to identify such a case and fixing it by:
	// 1. parsing the returned error to identify the instace id.
	// 2. creating a new TargetInput without instance id and trying to register them.
	var prefix, absentID string
	_, err := fmt.Sscanf(errStr, "%s The following instances do not exist: %s", &prefix, &absentID)
	if err != nil {
		albctx.GetLogger(ctx).Errorf("Error finding absent targets for %v: %v", t.TgArn, err)
		return err
	}

	absentID = strings.Trim(absentID, "'")
	albctx.GetLogger(ctx).Infof("Identified absentID %v", absentID)
	var newAdd []*elbv2.TargetDescription
	for _, a := range additions {
		if *a.Id != absentID {
			newAdd = append(newAdd, a)
		}
	}

	if len(newAdd) == 0 {
		albctx.GetLogger(ctx).Infof("No instance ids other than absent instanceIDs. Returning ...")
		return nil
	}

	in := &elbv2.RegisterTargetsInput{
		TargetGroupArn: aws.String(t.TgArn),
		Targets:        newAdd,
	}
	if _, err := c.cloud.RegisterTargetsWithContext(ctx, in); err != nil {
		albctx.GetLogger(ctx).Errorf("Error re-adding targets to %v: %v", t.TgArn, err.Error())
	} else {
		albctx.GetLogger(ctx).Infof("Successfully added targets to %v. Skipped %v", t.TgArn, absentID)
	}

	return err
}

func (c *targetsController) Reconcile(ctx context.Context, t *Targets) error {
	desired, err := c.endpointResolver.Resolve(t.Ingress, t.Backend, t.TargetType)
	if err != nil {
		return err
	}
	if t.TargetType == elbv2.TargetTypeEnumIp {
		err = c.populateTargetAZ(ctx, desired)
		if err != nil {
			return err
		}
	}
	current, err := c.getCurrentTargets(ctx, t.TgArn)
	if err != nil {
		return err
	}
	additions, removals := targetChangeSets(current, desired)
	if len(additions) > 0 {
		albctx.GetLogger(ctx).Infof("Adding targets to %v: %v", t.TgArn, tdsString(additions))
		in := &elbv2.RegisterTargetsInput{
			TargetGroupArn: aws.String(t.TgArn),
			Targets:        additions,
		}

		if _, err := c.cloud.RegisterTargetsWithContext(ctx, in); err != nil {
			errStr := err.Error()
			albctx.GetLogger(ctx).Errorf("Error adding targets to %v: %v", t.TgArn, errStr)
			albctx.GetEventf(ctx)(api.EventTypeWarning, "ERROR", "Error adding targets to target group %s: %s", t.TgArn, errStr)

			if err2 := c.processAbsentTargets(ctx, errStr, additions, t); err2 != nil {
				albctx.GetLogger(ctx).Errorf("Failed attempting to process absent target IDs %v: %v. Returning original error...", t.TgArn, err2.Error())
			} else {
                            // Reset err to nil since processAbsentTargets succeeded.
                            err = nil
                        }
			return err
		}
               // TODO add Add events ?
	}

	if len(removals) > 0 {
		albctx.GetLogger(ctx).Infof("Removing targets from %v: %v", t.TgArn, tdsString(removals))
		in := &elbv2.DeregisterTargetsInput{
			TargetGroupArn: aws.String(t.TgArn),
			Targets:        removals,
		}

		if _, err := c.cloud.DeregisterTargetsWithContext(ctx, in); err != nil {
			albctx.GetLogger(ctx).Errorf("Error removing targets from %v: %v", t.TgArn, err.Error())
			albctx.GetEventf(ctx)(api.EventTypeWarning, "ERROR", "Error removing targets from target group %s: %s", t.TgArn, err.Error())
			return err
		}
		// TODO add Delete events ?
	}
	t.Targets = desired
	return nil
}

func (c *targetsController) getCurrentTargets(ctx context.Context, TgArn string) ([]*elbv2.TargetDescription, error) {
	opts := &elbv2.DescribeTargetHealthInput{TargetGroupArn: aws.String(TgArn)}
	resp, err := c.cloud.DescribeTargetHealthWithContext(ctx, opts)
	if err != nil {
		return nil, err
	}

	var current []*elbv2.TargetDescription
	for _, thd := range resp.TargetHealthDescriptions {
		if aws.StringValue(thd.TargetHealth.State) == elbv2.TargetHealthStateEnumDraining {
			continue
		}
		current = append(current, thd.Target)
	}
	return current, nil
}

func (c *targetsController) populateTargetAZ(ctx context.Context, a []*elbv2.TargetDescription) error {
	vpc, err := c.cloud.GetVpcWithContext(ctx)
	if err != nil {
		return err
	}
	cidrBlocks := make([]*net.IPNet, 0)
	for _, cidrBlockAssociation := range vpc.CidrBlockAssociationSet {
		_, ipv4Net, err := net.ParseCIDR(*cidrBlockAssociation.CidrBlock)
		if err != nil {
			return err
		}
		cidrBlocks = append(cidrBlocks, ipv4Net)
	}
	for i := range a {
		inVPC := false
		for _, cidrBlock := range cidrBlocks {
			if cidrBlock.Contains(net.ParseIP(*a[i].Id)) {
				inVPC = true
				break
			}
		}
		if !inVPC {
			a[i].AvailabilityZone = aws.String("all")
		}
	}
	return nil
}

// targetChangeSets compares b to a, returning a list of targets to add and remove from a to match b
func targetChangeSets(current, desired []*elbv2.TargetDescription) (add []*elbv2.TargetDescription, remove []*elbv2.TargetDescription) {
	currentMap := map[string]bool{}
	desiredMap := map[string]bool{}

	for _, i := range current {
		currentMap[tdString(i)] = true
	}
	for _, i := range desired {
		desiredMap[tdString(i)] = true
	}

	for _, i := range desired {
		if _, ok := currentMap[tdString(i)]; !ok {
			add = append(add, i)
		}
	}

	for _, i := range current {
		if _, ok := desiredMap[tdString(i)]; !ok {
			remove = append(remove, i)
		}
	}

	return add, remove
}

func tdString(td *elbv2.TargetDescription) string {
	return fmt.Sprintf("%v:%v", aws.StringValue(td.Id), aws.Int64Value(td.Port))
}

func tdsString(tds []*elbv2.TargetDescription) string {
	var s []string
	for _, td := range tds {
		s = append(s, tdString(td))
	}
	return strings.Join(s, ", ")
}
