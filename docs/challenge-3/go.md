# Challenge 3 - Pub/Sub

## Overview

On the third challenge, your goal is to update the state store with all the events from pizza order that we are generating from the storefront, kitchen, and delivery services. For that, you will:

- Send all the generated events to a new Dapr component, a pub/sub message broker.
- Update the storefront, kitchen, and delivery services to publish a message to the pub/sub.
- Subscribe to these events in the order service, which is already managing the order state in our state store.

<img src="../../imgs/challenge-3.png" width=50%>

To learn more about the Publish & Subscribe building block, refer to the [Dapr docs](https://docs.dapr.io/developing-applications/building-blocks/pubsub/).

## Create the Pub/Sub component

Open the `/resources` folder and create a file called `pubsub.yaml`. Add the following content:

```yaml
apiVersion: dapr.io/v1alpha1
kind: Component
metadata:
  name: pizzapubsub
spec:
  type: pubsub.redis
  version: v1
  metadata:
  - name: redisHost
    value: localhost:6379
  - name: redisPassword
    value: ""
scopes:
- pizza-storefront
- pizza-kitchen
- pizza-delivery
- pizza-order
```

Similar to the `statestore.yaml` file, this new definition creates a Dapr component called _pizzapubsub_ of type _pubsub.redis_ pointing to the local Redis instance, using Redis Streams. Each app will initialize this component to interact with it.

## Create a subscription

Still inside the `/resources` folder, create a new file called `subscription.yaml`. Add the following content to it:

```yaml
apiVersion: dapr.io/v1alpha1
kind: Subscription
metadata:
  name: pizza-subscription
spec:
  topic: orders
  route: /orders-sub
  pubsubname: pizzapubsub
scopes:
- pizza-order
```

This file of kind `Subscription` specifies that every time the Pub/Sub `pizzapubsub` component receives a message in the `orders` topic, this message will be sent to a route called `/orders-sub` on the scoped `pizza-order` service. The `/orders-sub` endpoint is already declared in `pizza-order/main.go` and ready to receive events.

## Update the Kitchen service to publish messages to the message broker

Open `/pizza-kitchen/main.go` and:

1. Add the pub/sub constants at the top of the file:

```go
const (
    daprPubSubName  = "pizzapubsub"
    daprPubSubTopic = "orders"
)
```

2. Declare a package-level Dapr client and initialize it in `main()`:

```go
var daprClient dapr.Client

func main() {
    var err error
    daprClient, err = dapr.NewClient()
    if err != nil {
        log.Fatalf("Failed to create Dapr client: %v", err)
    }
    defer daprClient.Close()
    // ...
}
```

3. Update the `cookPizza` function. Replace the `// TODO: Publish the status update to the message broker` comment with:

```go
if err := daprClient.PublishEvent(
    context.Background(),
    daprPubSubName,
    daprPubSubTopic,
    orderData,
    dapr.PublishEventWithContentType("application/json"),
); err != nil {
    return fmt.Errorf("failed to publish event: %w", err)
}
```

`PublishEvent` publishes the `orderData` struct to the `pizzapubsub` component on the `orders` topic, serialized as JSON. The `dapr.PublishEventWithContentType("application/json")` option ensures the CloudEvent is properly typed.

## Update the Delivery service to publish messages to the message broker

Apply the same changes to `/pizza-delivery/main.go`:

1. Add the pub/sub constants and Dapr client (same as kitchen).

2. Update the `deliverPizza` function. Replace the `// TODO:` with:

```go
if err := daprClient.PublishEvent(
    context.Background(),
    daprPubSubName,
    daprPubSubTopic,
    orderData,
    dapr.PublishEventWithContentType("application/json"),
); err != nil {
    return fmt.Errorf("failed to publish event: %w", err)
}
```

## Update the Storefront service to publish messages to the message broker

Open `/pizza-storefront/main.go` and add the pub/sub constants:

```go
const (
    daprPubSubName  = "pizzapubsub"
    daprPubSubTopic = "orders"
)
```

Inside the `processOrder` function, replace the `// TODO: Publish the status update to the message broker` comment with:

```go
if err := daprClient.PublishEvent(
    context.Background(),
    daprPubSubName,
    daprPubSubTopic,
    orderData,
    dapr.PublishEventWithContentType("application/json"),
); err != nil {
    return fmt.Errorf("failed to publish event: %w", err)
}
```

Keep the service invocation code as well. After publishing the `pizza-storefront` events, we still need to invoke `/cook` and `/deliver`.

## Subscribe to events

Now that you've published the events to the `orders` topic, you will subscribe to the same topic in the `pizza-order` service.

Navigate to `/pizza-order/main.go` and find the `ordersSubscription` handler.

Replace the `// TODO: Update the order state` comment with:

```go
ctx := context.Background()
stateKey := fmt.Sprintf("order_%s", orderData.OrderID)

// Get existing order state and merge with the update
existingOrder := OrderData{}
state, err := daprClient.GetState(ctx, daprStoreName, stateKey, nil)
if err == nil && len(state.Value) > 0 {
    json.Unmarshal(state.Value, &existingOrder)
}

// Update the status while preserving other fields
if existingOrder.OrderID == "" {
    existingOrder = orderData
} else {
    existingOrder.Status = orderData.Status
}

updatedJSON, err := json.Marshal(existingOrder)
if err != nil {
    http.Error(w, err.Error(), http.StatusInternalServerError)
    return
}

if err := daprClient.SaveState(ctx, daprStoreName, stateKey, updatedJSON, nil); err != nil {
    http.Error(w, err.Error(), http.StatusInternalServerError)
    return
}

log.Printf("Updated state for order %s", orderData.OrderID)
```

The CloudEvent sent by Dapr to `/orders-sub` wraps the payload in a `data` field. The handler extracts it like this:

```go
var event map[string]any
json.NewDecoder(r.Body).Decode(&event)
dataBytes, _ := json.Marshal(event["data"])
var orderData OrderData
json.Unmarshal(dataBytes, &orderData)
```

Following the `subscription.yaml` file spec, every time a new message lands in the `orders` topic within the `pizzapubsub` pub/sub, it will be routed to `/orders-sub` and the order state will be updated.

## Run the application

It's time to run all four applications. If services are still running, press **CTRL+C** in each terminal window to stop them.

1. Open a new terminal, navigate to the `/pizza-order` folder and run:

```bash
dapr run --app-id pizza-order --app-protocol http --app-port 8001 --dapr-http-port 3501 --resources-path ../resources -- go run .
```

2. In your terminal, navigate to `/pizza-storefront` and run:

```bash
dapr run --app-id pizza-storefront --app-protocol http --app-port 8002 --dapr-http-port 3502 --resources-path ../resources -- go run .
```

3. Open a new terminal and navigate to `/pizza-kitchen`. Run:

```bash
dapr run --app-id pizza-kitchen --app-protocol http --app-port 8003 --dapr-http-port 3503 --resources-path ../resources -- go run .
```

4. Open a third terminal and navigate to `/pizza-delivery`. Run:

```bash
dapr run --app-id pizza-delivery --app-protocol http --app-port 8004 --dapr-http-port 3504 --resources-path ../resources -- go run .
```

> [!IMPORTANT]
> If you are using Consul as a naming resolution service, add `--config ../resources/config/config.yaml` before `-- go run .` on your Dapr run command.

Check the Dapr and application logs for all four services. You should now see the pubsub component loaded:

```bash
INFO[0000] Component loaded: pizzapubsub (pubsub.redis/v1)  app_id=pizza-storefront instance=diagrid.local scope=dapr.runtime.processor type=log ver=1.14.4
```

## Test the service

### Use VS Code REST Client

Open `Endpoints.http` and create a new order sending the request on `Direct Pizza Store Endpoint (for testing)`.

Navigate to the `pizza-order` terminal, where you should see all order status events being received and stored:

```bash
== APP == 2024/12/05 22:40:46 Received order update for order 123: validating
== APP == 2024/12/05 22:40:46 Updated state for order 123
== APP == 2024/12/05 22:40:47 Received order update for order 123: processing
== APP == 2024/12/05 22:40:47 Updated state for order 123
== APP == 2024/12/05 22:40:49 Received order update for order 123: confirmed
== APP == 2024/12/05 22:40:49 Updated state for order 123
== APP == 2024/12/05 22:40:50 Received order update for order 123: cooking_preparing_ingredients
== APP == 2024/12/05 22:40:50 Updated state for order 123
== APP == 2024/12/05 22:40:52 Received order update for order 123: cooking_making_dough
== APP == 2024/12/05 22:40:52 Updated state for order 123
== APP == 2024/12/05 22:40:55 Received order update for order 123: cooking_adding_toppings
== APP == 2024/12/05 22:40:55 Updated state for order 123
== APP == 2024/12/05 22:40:57 Received order update for order 123: cooking_baking
== APP == 2024/12/05 22:40:57 Updated state for order 123
== APP == 2024/12/05 22:41:02 Received order update for order 123: cooking_quality_check
== APP == 2024/12/05 22:41:02 Updated state for order 123
== APP == 2024/12/05 22:41:03 Received order update for order 123: delivery_finding_driver
== APP == 2024/12/05 22:41:03 Updated state for order 123
== APP == 2024/12/05 22:41:05 Received order update for order 123: delivery_driver_assigned
== APP == 2024/12/05 22:41:05 Updated state for order 123
== APP == 2024/12/05 22:41:06 Received order update for order 123: delivery_picked_up
== APP == 2024/12/05 22:41:06 Updated state for order 123
== APP == 2024/12/05 22:41:08 Received order update for order 123: delivery_on_the_way
== APP == 2024/12/05 22:41:08 Updated state for order 123
== APP == 2024/12/05 22:41:13 Received order update for order 123: delivery_arriving
== APP == 2024/12/05 22:41:13 Updated state for order 123
== APP == 2024/12/05 22:41:15 Received order update for order 123: delivery_at_location
== APP == 2024/12/05 22:41:15 Updated state for order 123
```

### Use _cURL_

Open a fourth terminal window and create a new order using cURL:

```bash
curl -H 'Content-Type: application/json' \
    -d '{ "order_id": "123", "pizza_type": "pepperoni", "size": "large", "customer": { "name": "John Doe", "address": "123 Main St", "phone": "555-0123" } }' \
    -X POST \
    http://localhost:8002/order
```

## Dapr multi-app run

Instead of opening multiple terminals to run the services, you can take advantage of a great Dapr CLI feature: [multi-app run](https://docs.dapr.io/developing-applications/local-development/multi-app-dapr-run/multi-app-overview/). This enables you to run all services with just one command!

In the parent folder, create a new file called `dapr.yaml`. Add the following content to it:

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
```

Stop the services if they are running, and enter the following command in the terminal:

```bash
dapr run -f .
```

All four services will run at the same time and log events in the same terminal window.

## Next steps

In the next challenge we will orchestrate the pizza ordering, cooking, and delivering process leveraging Dapr's Workflow API. Once you are ready, navigate to Challenge 4: [Workflows](/docs/challenge-4/go.md)!
