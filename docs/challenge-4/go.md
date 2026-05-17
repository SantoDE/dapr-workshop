# Challenge 4 - Workflows

## Overview

On the fourth and final challenge, you will orchestrate the process of ordering, cooking, and delivering a pizza using Dapr Workflows. Dapr workflow makes it easy for developers to write business logic and integrations in a reliable way. Since Dapr workflows are stateful, they support long-running and fault-tolerant applications, ideal for orchestrating microservices.

You will:

- Replace the current chained service invocation calls in the `pizza-storefront` service with a Workflow process.
- Add an external event to validate the quality of the pizza after the cooking process is done.
- Query for the workflow status, pause, resume, and cancel the run.

<img src="../../imgs/workflow.png" width=75%>

To learn more about the Workflow building block, refer to the [Dapr docs](https://docs.dapr.io/developing-applications/building-blocks/workflow/).

## Install the required dependencies

Navigate to the `/pizza-workflow` directory and install dependencies:

```bash
cd pizza-workflow
go mod tidy
```

Or from the root of the repository:

```bash
make tidy
```

## Modify the state store component to accept workflows

The Workflow building block requires a state store to manage its state during the run. Update `/resources/statestore.yaml` to reflect that:

```yaml
apiVersion: dapr.io/v1alpha1
kind: Component
metadata:
  name: pizzastatestore
spec:
  type: state.redis
  version: v1
  metadata:
  - name: redisHost
    value: localhost:6379
  - name: redisPassword
    value: ""
  - name: actorStateStore
    value: "true"
scopes:
- pizza-workflow
- pizza-order
```

By setting `actorStateStore` to `true`, this state store component is able to manage Workflow data, since Workflows rely on [Dapr Actors](https://docs.dapr.io/developing-applications/building-blocks/workflow/workflow-features-concepts/#workflow-backend) in the background. You are also scoping the component to allow the `pizza-workflow` service to access it.

## Creating the Activities

Workflow activities are the basic unit of work in a workflow and are the tasks that get orchestrated in the business process. In this challenge, you will create a workflow to process a pizza order. The tasks will involve:

- Making a request to start the pizza order to the Storefront service.
- Invoking the Kitchen service to cook the pizza.
- Validating the pizza quality after it is cooked.
- Invoking the Delivery service to deliver the pizza.

In Go, activities are functions with the signature `func(workflow.ActivityContext) (any, error)`. Add them to `/pizza-workflow/main.go`.

First, add the required import:

```go
import (
    "github.com/dapr/go-sdk/workflow"
)
```

### Create the Storefront, Kitchen, and Delivery activities

1. Create the Storefront activity:

```go
func OrderPizzaActivity(ctx workflow.ActivityContext) (any, error) {
    var input OrderData
    if err := ctx.GetInput(&input); err != nil {
        return nil, err
    }
    log.Printf("Calling pizza-storefront service for order %s", input.OrderID)

    if err := invokeService(3505, "pizza-storefront", "order", &input); err != nil {
        return nil, fmt.Errorf("storefront invocation failed: %w", err)
    }
    return &input, nil
}
```

2. Add the Kitchen activity:

```go
func CookPizzaActivity(ctx workflow.ActivityContext) (any, error) {
    var input OrderData
    if err := ctx.GetInput(&input); err != nil {
        return nil, err
    }
    log.Printf("Calling pizza-kitchen service for order %s", input.OrderID)

    if err := invokeService(3505, "pizza-kitchen", "cook", &input); err != nil {
        return nil, fmt.Errorf("kitchen invocation failed: %w", err)
    }
    input.Status = "cooked"
    return &input, nil
}
```

3. Finally, add the Delivery activity:

```go
func DeliverPizzaActivity(ctx workflow.ActivityContext) (any, error) {
    var input OrderData
    if err := ctx.GetInput(&input); err != nil {
        return nil, err
    }
    log.Printf("Calling pizza-delivery service for order %s", input.OrderID)

    if err := invokeService(3505, "pizza-delivery", "deliver", &input); err != nil {
        return nil, fmt.Errorf("delivery invocation failed: %w", err)
    }
    input.Status = "delivered"
    return &input, nil
}
```

Add the `invokeService` helper:

```go
func invokeService(daprPort int, appID, method string, payload any) error {
    body, err := json.Marshal(payload)
    if err != nil {
        return err
    }
    targetURL := fmt.Sprintf("http://localhost:%d/%s", daprPort, method)
    req, err := http.NewRequest(http.MethodPost, targetURL, bytes.NewReader(body))
    if err != nil {
        return err
    }
    req.Header.Set("dapr-app-id", appID)
    req.Header.Set("Content-Type", "application/json")
    resp, err := http.DefaultClient.Do(req)
    if err != nil {
        return err
    }
    defer resp.Body.Close()
    return nil
}
```

### Create the Validation Activity

The Validation Activity saves a `pending_validation` record to the state store, then pauses execution until the workflow receives an external approval event.

```go
func ValidatePizzaActivity(ctx workflow.ActivityContext) (any, error) {
    var input OrderData
    if err := ctx.GetInput(&input); err != nil {
        return nil, err
    }
    log.Printf("Starting validation process for order %s", input.OrderID)

    validationState := map[string]string{
        "order_id": input.OrderID,
        "status":   "pending_validation",
    }
    stateJSON, _ := json.Marshal(validationState)
    stateKey := fmt.Sprintf("validation_%s", input.OrderID)

    if err := daprClient.SaveState(context.Background(), daprStoreName, stateKey, stateJSON, nil); err != nil {
        return nil, fmt.Errorf("failed to save validation state: %w", err)
    }
    return &input, nil
}
```

## Creating the Workflow

Now let's create the workflow itself. The Dapr Workflow engine will take care of scheduling and execution of the order tasks, including managing failures and retries.

In Go, a workflow is a function with the signature `func(*workflow.WorkflowContext) (any, error)`. Add it to `/pizza-workflow/main.go`:

```go
func PizzaOrderWorkflow(ctx *workflow.WorkflowContext) (any, error) {
    var input OrderData
    if err := ctx.GetInput(&input); err != nil {
        return nil, err
    }
    log.Printf("Starting workflow for order %s", input.OrderID)

    // Step 1: Place and process the order via storefront
    var ordered OrderData
    if err := ctx.CallActivity(OrderPizzaActivity,
        workflow.ActivityInput(input)).Await(&ordered); err != nil {
        return &OrderData{OrderID: input.OrderID, Status: "failed", Error: err.Error()}, nil
    }

    // Step 2: Cook the pizza
    var cooked OrderData
    if err := ctx.CallActivity(CookPizzaActivity,
        workflow.ActivityInput(ordered)).Await(&cooked); err != nil {
        return &OrderData{OrderID: input.OrderID, Status: "failed", Error: err.Error()}, nil
    }

    // Step 3: Wait for manager validation
    log.Printf("Waiting for manager validation of order %s", input.OrderID)
    var approved bool
    if err := ctx.WaitForExternalEvent("ValidationComplete", 0).Await(&approved); err != nil {
        return nil, err
    }

    if !approved {
        return &OrderData{OrderID: input.OrderID, Status: "rejected"}, nil
    }

    // Step 4: Deliver the pizza
    var delivered OrderData
    if err := ctx.CallActivity(DeliverPizzaActivity,
        workflow.ActivityInput(cooked)).Await(&delivered); err != nil {
        return &OrderData{OrderID: input.OrderID, Status: "failed", Error: err.Error()}, nil
    }

    return &OrderData{
        OrderID: input.OrderID,
        Status:  "completed",
    }, nil
}
```

### Breaking this down

1. `PizzaOrderWorkflow` receives a `*workflow.WorkflowContext` and extracts the input with `ctx.GetInput`:

```go
func PizzaOrderWorkflow(ctx *workflow.WorkflowContext) (any, error) {
    var input OrderData
    ctx.GetInput(&input)
```

2. Every activity is called using `ctx.CallActivity` passing the activity function and input. The result is retrieved with `.Await`:

```go
var ordered OrderData
ctx.CallActivity(OrderPizzaActivity, workflow.ActivityInput(input)).Await(&ordered)
```

This applies for the Storefront, Cooking, and Delivery activities.

3. The workflow pauses at the validation step, waiting for an external event named `"ValidationComplete"`:

```go
var approved bool
ctx.WaitForExternalEvent("ValidationComplete", 0).Await(&approved)

if !approved {
    return &OrderData{OrderID: input.OrderID, Status: "rejected"}, nil
}
```

The second argument `0` means wait indefinitely. The workflow will not proceed until this event is raised from outside.

4. If any step fails, a `failed` status with the error is returned:

```go
return &OrderData{OrderID: input.OrderID, Status: "failed", Error: err.Error()}, nil
```

## Creating the controller

Open `/pizza-workflow/main.go`. The HTTP endpoints for managing workflows are already stubbed out. Now you'll implement them using the Dapr Workflow client.

### Modify the endpoints to include workflow actions

1. Inside `startOrder`, replace the `// TODO:` comment with:

```go
wfClient, err := workflow.NewClient()
if err != nil {
    http.Error(w, err.Error(), http.StatusInternalServerError)
    return
}
defer wfClient.Close()

_, err = wfClient.ScheduleNewWorkflow(r.Context(), "PizzaOrderWorkflow",
    workflow.WithInstanceID(instanceID),
    workflow.WithInput(orderData))
if err != nil {
    http.Error(w, err.Error(), http.StatusInternalServerError)
    return
}
```

`ScheduleNewWorkflow` starts a new workflow instance, passing:
- The workflow name: `"PizzaOrderWorkflow"`
- An instance ID: used to manage the workflow lifecycle
- The input: the `orderData` object

2. Inside `validatePizza`, replace the `// TODO:` comment with:

```go
wfClient, err := workflow.NewClient()
if err != nil {
    http.Error(w, err.Error(), http.StatusInternalServerError)
    return
}
defer wfClient.Close()

if err := wfClient.RaiseEvent(r.Context(), instanceID, "ValidationComplete",
    workflow.WithEventPayload(validationData.Approved)); err != nil {
    http.Error(w, err.Error(), http.StatusInternalServerError)
    return
}
```

`RaiseEvent` unblocks the workflow's `WaitForExternalEvent` call, passing the approval boolean that determines whether to proceed with delivery.

3. Inside `getStatus`, replace the `// TODO:` comment with:

```go
wfClient, err := workflow.NewClient()
if err != nil {
    http.Error(w, err.Error(), http.StatusInternalServerError)
    return
}
defer wfClient.Close()

metadata, err := wfClient.FetchWorkflowMetadata(r.Context(), instanceID)
if err != nil {
    http.Error(w, err.Error(), http.StatusInternalServerError)
    return
}

w.Header().Set("Content-Type", "application/json")
json.NewEncoder(w).Encode(metadata.RuntimeStatus)
return
```

4. Inside `pauseOrder`, replace the `// TODO:` comment with:

```go
wfClient, err := workflow.NewClient()
if err != nil {
    http.Error(w, err.Error(), http.StatusInternalServerError)
    return
}
defer wfClient.Close()

if err := wfClient.SuspendWorkflow(r.Context(), instanceID, "paused by user"); err != nil {
    http.Error(w, err.Error(), http.StatusInternalServerError)
    return
}
```

`SuspendWorkflow` pauses the workflow after the current activity completes.

5. Inside `resumeOrder`, replace the `// TODO:` comment with:

```go
wfClient, err := workflow.NewClient()
if err != nil {
    http.Error(w, err.Error(), http.StatusInternalServerError)
    return
}
defer wfClient.Close()

if err := wfClient.ResumeWorkflow(r.Context(), instanceID, "resumed by user"); err != nil {
    http.Error(w, err.Error(), http.StatusInternalServerError)
    return
}
```

6. Inside `cancelOrder`, replace the `// TODO:` comment with:

```go
wfClient, err := workflow.NewClient()
if err != nil {
    http.Error(w, err.Error(), http.StatusInternalServerError)
    return
}
defer wfClient.Close()

if err := wfClient.TerminateWorkflow(r.Context(), instanceID, "cancelled by user"); err != nil {
    http.Error(w, err.Error(), http.StatusInternalServerError)
    return
}
```

### Register the Workflow and Activities

Replace the `// TODO: Create a workflow worker` and `// TODO: Register workflows and activities` comments with:

```go
w, err := workflow.NewWorker()
if err != nil {
    log.Fatalf("Failed to create workflow worker: %v", err)
}

w.RegisterWorkflow(PizzaOrderWorkflow)
w.RegisterActivity(OrderPizzaActivity)
w.RegisterActivity(CookPizzaActivity)
w.RegisterActivity(ValidatePizzaActivity)
w.RegisterActivity(DeliverPizzaActivity)
```

### Start the workflow runtime

Replace the `// TODO: Start the workflow runtime in a goroutine` comment with:

```go
go func() {
    if err := w.Start(); err != nil {
        log.Fatalf("Failed to start workflow worker: %v", err)
    }
}()
defer w.Shutdown()
```

## Remove the service invocation calls from the Storefront service

Inside `/pizza-storefront/main.go`, remove the kitchen and delivery invocation calls from `processOrder`. The workflow will now be responsible for orchestrating these calls:

```go
// Remove these lines:
if err := invokeService("pizza-kitchen", "cook", orderData); err != nil {
    return fmt.Errorf("failed to call pizza-kitchen: %w", err)
}

if err := invokeService("pizza-delivery", "deliver", orderData); err != nil {
    return fmt.Errorf("failed to call pizza-delivery: %w", err)
}
```

Now that the workflow orchestrates service invocation, you don't need this behaviour in the Storefront service.

## Run the application

It's time to run all five applications. Stop any currently running services first.

1. Open a new terminal, navigate to `/pizza-order` and run:

```bash
dapr run --app-id pizza-order --app-protocol http --app-port 8001 --dapr-http-port 3501 --resources-path ../resources -- go run .
```

2. Navigate to `/pizza-storefront` and run:

```bash
dapr run --app-id pizza-storefront --app-protocol http --app-port 8002 --dapr-http-port 3502 --resources-path ../resources -- go run .
```

3. Navigate to `/pizza-kitchen` and run:

```bash
dapr run --app-id pizza-kitchen --app-protocol http --app-port 8003 --dapr-http-port 3503 --resources-path ../resources -- go run .
```

4. Navigate to `/pizza-delivery` and run:

```bash
dapr run --app-id pizza-delivery --app-protocol http --app-port 8004 --dapr-http-port 3504 --resources-path ../resources -- go run .
```

5. Navigate to `/pizza-workflow` and run:

```bash
dapr run --app-id pizza-workflow --app-protocol http --app-port 8005 --dapr-http-port 3505 --resources-path ../resources -- go run .
```

> [!IMPORTANT]
> If you are using Consul as a naming resolution service, add `--config ../resources/config/config.yaml` before `-- go run .` on your Dapr run command.

## Test the service

### Use VS Code REST Client

Open `Endpoints.http` and start a new workflow by sending the request under `Start a new pizza order workflow`.

Navigate to the `pizza-workflow` terminal, where you should see:

```bash
== APP == 2024/12/05 23:48:49 Starting pizza-workflow service on port :8005
== APP == 2024/12/05 23:48:49 Starting workflow for order 123
== APP == 2024/12/05 23:48:49 Calling pizza-storefront service for order 123
== APP == 2024/12/05 23:48:53 Calling pizza-kitchen service for order 123
== APP == 2024/12/05 23:49:06 Waiting for manager validation of order 123
```

Once the validation stage arrives, send a request under `Validate pizza (approve)` to move the workflow forward:

```bash
== APP == 2024/12/05 23:51:25 Calling pizza-delivery service for order 123
== APP == 2024/12/05 23:51:38 Starting workflow for order 123
INFO[0042] pizza-order-123: 'PizzaOrderWorkflow' completed with a COMPLETED status.  app_id=pizza-workflow
```

## Dapr multi-app run

Open the multi-app run file `dapr.yaml` and include the workflow service:

```yaml
version: 1
common:
  resourcesPath: ./resources
  # Uncomment the following line if you are running Consul for service naming resolution
  # configFilePath: ./resources/config/config.yaml
apps:
  - appDirPath: ./pizza-order/
    appID: pizza-order
    appPort: 8001
    daprHTTPPort: 3501
    command: ["go", "run", "."]
  - appDirPath: ./pizza-storefront/
    appID: pizza-storefront
    daprHTTPPort: 3502
    appPort: 8002
    command: ["go", "run", "."]
  - appDirPath: ./pizza-kitchen/
    appID: pizza-kitchen
    appPort: 8003
    daprHTTPPort: 3503
    command: ["go", "run", "."]
  - appDirPath: ./pizza-delivery/
    appID: pizza-delivery
    appPort: 8004
    daprHTTPPort: 3504
    command: ["go", "run", "."]
  - appDirPath: ./pizza-workflow/
    appID: pizza-workflow
    appPort: 8005
    daprHTTPPort: 3505
    command: ["go", "run", "."]
```

Stop the services if they are running, and enter the following command in the terminal:

```bash
dapr run -f .
```

All five services will run at the same time and log events in the same terminal window.

## Visualize the Workflow with the Diagrid Dashboard

Now that your workflow is running, you can use the [Diagrid Dashboard](https://www.diagrid.io/blog/improving-the-local-dapr-workflow-experience-diagrid-dashboard) to visually inspect the workflow state. It is a local containerized UI that connects directly to your Redis state store — no extra configuration needed.

Start the dashboard in a new terminal:

```bash
docker run -p 8080:8080 ghcr.io/diagridio/diagrid-dashboard:latest
```

Open your browser at `http://localhost:8080`. From there you can:

- **View all workflow instances** — see running, completed, and failed workflows at a glance.
- **Inspect execution details** — drill into a specific workflow to examine its input, output, and history.
- **Expand individual steps** — click any event in the Execution History table to review detailed step information, useful for debugging the validation pause and each activity.

Start a new pizza order via the REST Client, then refresh the dashboard to watch the workflow progress through each step in real time.

## Next steps

Congratulations, you have completed all the challenges and you can claim your [reward](../completion.md)!
