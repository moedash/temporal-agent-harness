// Command worker-lambda runs the Slack connector Temporal worker inside AWS
// Lambda via the lambdaworker contrib package. Connection settings are loaded
// from the environment / temporal.toml by envconfig (TEMPORAL_ADDRESS,
// TEMPORAL_NAMESPACE, TEMPORAL_TASK_QUEUE, TEMPORAL_API_KEY, TEMPORAL_TLS*).
//
// See README.md in this directory for build, deployment, and versioning notes.
package main

import (
	"fmt"
	"log"
	"os"
	"time"

	"github.com/temporal-community/temporal-agent-harness/nexus/ui_connector/agent"
	"github.com/temporal-community/temporal-agent-harness/nexus/ui_connector/router"
	"github.com/temporal-community/temporal-agent-harness/nexus/ui_connector/slack"
	slackoutbound "github.com/temporal-community/temporal-agent-harness/nexus/ui_connector/slack/outbound"

	"go.temporal.io/sdk/contrib/aws/lambdaworker"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
)

func getenvOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func main() {
	lambdaworker.RunWorker(worker.WorkerDeploymentVersion{
		DeploymentName: getenvOr("WORKER_DEPLOYMENT_NAME", "nexus-connector-slack"),
		BuildID:        getenvOr("WORKER_BUILD_ID", "dev"),
	}, func(o *lambdaworker.Options) error {
		// The connector workflow is short-lived; pin it to the build it starts
		// on so a new deployment never migrates in-flight runs.
		o.WorkerOptions.DeploymentOptions.DefaultVersioningBehavior = workflow.VersioningBehaviorPinned

		// Default the task queue and namespace to match the always-on worker
		// (cmd/worker) unless overridden via TEMPORAL_TASK_QUEUE / TEMPORAL_NAMESPACE.
		if o.TaskQueue == "" {
			o.TaskQueue = "nexus-connector-slack"
		}
		if o.ClientOptions.Namespace == "" {
			o.ClientOptions.Namespace = "connector"
		}

		// Source the Temporal Cloud API key and Slack bot token from Secrets
		// Manager when their *_SECRET_ARN vars are set (falling back to env).
		slackToken, err := resolveSecrets(o)
		if err != nil {
			return err
		}
		if slackToken == "" {
			return fmt.Errorf("slack bot token is required")
		}

		bot, err := slack.NewSlackBot(slackToken)
		if err != nil {
			return err
		}
		if bot.UserID != "" {
			log.Printf("Bot user ID: %s", bot.UserID)
		}

		// Compose this connector: a Slack outbound driver + the temporal-agent-harness
		// backend driver, plugged into the generic RouterWorkflow — same wiring as
		// cmd/worker, but registered on o (a worker.Registry) instead of a
		// worker.Worker, since the Lambda worker never Start()s or Run()s itself.
		platform := slackoutbound.NewSlackPlatform(bot.Client, bot.TeamID)
		outboundDriver := slackoutbound.NewDriver(workflow.ActivityOptions{
			StartToCloseTimeout: 30 * time.Second,
			RetryPolicy:         &temporal.RetryPolicy{MaximumAttempts: 1},
		})
		backendDriver := &agent.Driver{}

		routerWorkflow := router.NewRouterWorkflow(outboundDriver, backendDriver)
		o.RegisterWorkflowWithOptions(routerWorkflow.Run, workflow.RegisterOptions{Name: router.WorkflowName})
		slackoutbound.RegisterActivities(o, platform)

		return nil
	})
}
